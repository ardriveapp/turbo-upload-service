/**
 * Copyright (C) 2022-2024 Permanent Data Solutions, Inc. All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
import { Message } from "@aws-sdk/client-sqs";
import { JWKInterface, Tag } from "arbundles";
import { Base64UrlString } from "arweave/node/lib/utils";
import crypto from "node:crypto";
import winston from "winston";

import { ArweaveGateway } from "../arch/arweaveGateway";
import { Database } from "../arch/db/database";
import { ObjectStore } from "../arch/objectStore";
import { PaymentService, ReserveBalanceResponse } from "../arch/payment";
import { enqueue } from "../arch/queues";
import { StreamingDataItem } from "../bundles/streamingDataItem";
import {
  blocklistedAddresses,
  deadlineHeightIncrement,
  receiptVersion,
  skipOpticalPostAddresses,
} from "../constants";
import { MetricRegistry } from "../metricRegistry";
import { KoaContext } from "../server";
import { InFlightMultiPartUpload, PostedNewDataItem } from "../types/dbTypes";
import { UploadId } from "../types/types";
import { W } from "../types/winston";
import { fromB64Url, toB64Url } from "../utils/base64";
import {
  filterKeysFromObject,
  getPremiumFeatureType,
  payloadContentTypeFromDecodedTags,
  sleep,
} from "../utils/common";
import {
  BlocklistedAddressError,
  DataItemExistsWarning,
  EnqueuedForValidationError,
  InsufficientBalance,
  InvalidChunk,
  InvalidChunkSize,
  InvalidDataItem,
  MultiPartUploadNotFound,
} from "../utils/errors";
import {
  completeMultipartUpload,
  createMultipartUpload,
  getMultiPartUploadByteCount,
  getMultipartUploadObject,
  getMultipartUploadParts,
  getRawDataItem,
  getRawDataItemByteCount,
  moveFinalizedMultipartObject,
  rawDataItemExists,
  removeDataItem,
  uploadPart,
} from "../utils/objectStoreUtils";
import {
  encodeTagsForOptical,
  signDataItemHeader,
} from "../utils/opticalUtils";
import { ownerToNativeAddress } from "../utils/ownerToNativeAddress";
import {
  IrysSignedReceipt,
  IrysUnsignedReceipt,
  signIrysReceipt,
} from "../utils/signReceipt";

const shouldSkipBalanceCheck = process.env.SKIP_BALANCE_CHECKS === "true";
const opticalBridgingEnabled = process.env.OPTICAL_BRIDGING_ENABLED !== "false";

export const chunkMinSize = 1024 * 1024 * 5; // 5MiB - AWS minimum
export const chunkMaxSize = 1024 * 1024 * 500; // 500MiB // NOTE: AWS supports upto 5GiB
export const defaultChunkSize = 25_000_000; // 25MB

export async function createMultiPartUpload(ctx: KoaContext) {
  const { database, objectStore, logger } = ctx.state;
  logger.debug("Creating new multipart upload");
  const uploadKey = crypto.randomUUID();
  const newUploadId = await createMultipartUpload(objectStore, uploadKey);

  logger.debug("Created new multipart upload", { newUploadId });
  // create new upload
  await database.insertInFlightMultiPartUpload({
    uploadId: newUploadId,
    uploadKey,
  });

  // In order to combat RDS replication-lag-related issues with posting chunks (parts)
  // for this uploadId immediately after receiving the fresh uploadId, impose an
  // arbitrary 50ms delay here.
  await sleep(50);

  logger.info("Inserted new multipart upload into database", { newUploadId });

  ctx.body = {
    id: newUploadId,
    max: chunkMaxSize,
    min: chunkMinSize,
  };

  return; // do not return next()
}

export async function getMultipartUpload(ctx: KoaContext) {
  const { uploadId } = ctx.params;
  const { database, logger, objectStore } = ctx.state;

  try {
    logger.info("Getting multipart upload", { uploadId });

    // check the upload exists, then get chunks
    const upload = await database.getInflightMultiPartUpload(uploadId);
    const chunks = await getMultipartUploadParts(
      objectStore,
      upload.uploadKey,
      uploadId
    );

    const chunkSize = upload.chunkSize || defaultChunkSize;
    // TODO: Could add finalization status here without having to add a new endpoint
    ctx.body = {
      id: upload.uploadId,
      max: chunkMaxSize,
      min: chunkMinSize,
      size: chunkSize,
      chunks: chunks
        // sort chunks in ascending order
        .sort((a, b) => a.partNumber - b.partNumber)
        // 0-index based offsets
        .map((chunk) => [chunkSize * (chunk.partNumber - 1), chunk.size]),
      failedReason: upload.failedReason,
    };
  } catch (error) {
    logger.error("Error getting multipart upload", {
      uploadId,
      error: error instanceof Error ? error.message : error,
    });
    if (error instanceof MultiPartUploadNotFound) {
      ctx.status = 404;
      ctx.message = error.message;
    } else if (error instanceof InvalidChunkSize) {
      ctx.status = 400;
      ctx.message = error.message;
    } else {
      ctx.status = 503;
      ctx.message = "Internal Server Error";
    }
  }

  return; // do not return next();
}

export async function getMultipartUploadStatus(ctx: KoaContext) {
  const { uploadId } = ctx.params;
  const { database, logger, objectStore } = ctx.state;
  logger.info("Getting multipart upload status...", { uploadId });
  try {
    // If this succeeds, then we're either still assembling the data item
    // or validating it.
    const inFlightUpload = await database.getInflightMultiPartUpload(uploadId);

    // A non-zero response here means that assembly was completed
    // NOTE: A race condition exists when, between the time the inFlightUpload is
    // returned and getting the byte count below, the multipart upload is validated
    // and moved to the new data item bucket. In this case, we'd incorrectly return
    // 'assembling' for a very short period of time.
    const multipartUploadByteCount = await getMultiPartUploadByteCount(
      objectStore,
      inFlightUpload.uploadKey
    );

    // TODO: Sign responses in the future
    ctx.body = {
      status: inFlightUpload.failedReason
        ? inFlightUpload.failedReason
        : multipartUploadByteCount > 0
        ? "VALIDATING"
        : "ASSEMBLING",
      timestamp: Date.now(),
    };
    return;
  } catch (error) {
    if (error instanceof MultiPartUploadNotFound) {
      // We may have already finished validating the upload. Let's check.
      logger.debug(
        "No inflight upload found. Checking for validated upload...",
        {
          uploadId,
          error: error instanceof Error ? error.message : error,
        }
      );
    } else {
      logger.error("Error getting multipart upload", {
        uploadId,
        error: error instanceof Error ? error.message : error,
      });
      ctx.status = 500;
      ctx.message = "Internal Server Error";
      return;
    }
  }

  try {
    // If this succeeds, then we've already validated the data item and
    // are getting the data item into the fulfillment pipeline.
    const validatedUpload = await database.getFinalizedMultiPartUpload(
      uploadId
    );

    // The data item may already have made it to fulfillment...
    const fulfillmentInfo = await database.getDataItemInfo(
      validatedUpload.dataItemId
    );

    // TODO: Sign this in the future
    ctx.body = {
      status: validatedUpload.failedReason
        ? validatedUpload.failedReason
        : fulfillmentInfo
        ? "FINALIZED"
        : "FINALIZING",
      timestamp: Date.now(),
    };
    return;
  } catch (error) {
    if (error instanceof MultiPartUploadNotFound) {
      logger.error("No inflight or validated upload found.", {
        uploadId,
        error: error instanceof Error ? error.message : error,
      });
      ctx.status = 404;
      ctx.message = error.message;
    } else {
      logger.error("Error getting multipart upload", {
        uploadId,
        error: error instanceof Error ? error.message : error,
      });
      ctx.status = 500;
      ctx.message = "Internal Server Error";
    }
    return;
  }
}

export async function postDataItemChunk(ctx: KoaContext) {
  const { uploadId, chunkOffset } = ctx.params;
  const { objectStore, database, logger } = ctx.state;

  const contentLength = ctx.req.headers["content-length"];

  if (!contentLength) {
    ctx.status = 400;
    ctx.message =
      "Content-Length header is required a must be a positive integer.";
    return;
  }

  logger.debug("Posting data item chunk", { uploadId, chunkOffset });
  // check that upload exists
  try {
    const upload = await database.getInflightMultiPartUpload(uploadId);
    logger.debug("Got multipart upload", { ...upload });

    // No need to proceed if this upload has already failed
    if (upload.failedReason) {
      throw new InvalidDataItem();
    }

    const sizeOfChunk = contentLength ? +contentLength : defaultChunkSize;
    const chunkSize = upload.chunkSize || sizeOfChunk;

    try {
      if (!upload.chunkSize || sizeOfChunk > upload.chunkSize) {
        logger.debug("Updating chunk size in database", {
          uploadId,
          sizeOfChunk,
        });
        // NOTE: this may be better suited in a redis or read through cache
        await database.updateMultipartChunkSize(sizeOfChunk, uploadId);
        logger.debug("Successfully updated chunk size 👍", {
          uploadId,
          sizeOfChunk,
        });
      }
      logger.debug("Retrieved chunk size for upload", {
        uploadId,
        sizeOfChunk,
      });
    } catch (error) {
      logger.warn("Collision while updating chunk size... continuing.", {
        uploadId,
        error: error instanceof Error ? error.message : error,
      });
    }

    const partNumber = Math.floor(chunkOffset / chunkSize) + 1;

    // Need to give content length here for last chunk or s3 will wait for more data
    const etag = await uploadPart({
      objectStore,
      uploadKey: upload.uploadKey,
      stream: ctx.req,
      uploadId,
      partNumber,
      sizeOfChunk,
    });
    logger.info("Uploaded part", { uploadId, partNumber, etag, sizeOfChunk });

    ctx.status = 200;
  } catch (error) {
    logger.error("Error posting data item chunk", {
      uploadId,
      error,
    });

    if (error instanceof MultiPartUploadNotFound) {
      ctx.status = 404;
      ctx.message = error.message;
    } else if (
      error instanceof InvalidChunkSize ||
      error instanceof InvalidChunk ||
      error instanceof InvalidDataItem
    ) {
      ctx.status = 400;
      ctx.message = error.message;
    } else {
      ctx.status = 503;
      ctx.message = "Internal Server Error";
    }
  }
  return; // do not return next();
}

export async function finalizeMultipartUploadWithQueueMessage({
  message,
  paymentService,
  objectStore,
  database,
  arweaveGateway,
  getArweaveWallet,
  logger,
}: {
  message: Message;
  paymentService: PaymentService;
  objectStore: ObjectStore;
  database: Database;
  arweaveGateway: ArweaveGateway;
  getArweaveWallet: () => Promise<JWKInterface>;
  logger: winston.Logger;
}) {
  const uploadId = JSON.parse(message.Body ?? "").uploadId as UploadId;
  if (!uploadId) {
    throw new Error(
      "Malformed message! Expected string key 'uploadId' in message body."
    );
  }

  await finalizeMultipartUpload({
    uploadId,
    paymentService,
    objectStore,
    database,
    arweaveGateway,
    getArweaveWallet,
    logger,
    asyncValidation: false,
  });
}

export async function finalizeMultipartUploadWithHttpRequest(ctx: KoaContext) {
  const { uploadId } = ctx.params;
  const asyncValidation = ctx.state.asyncValidation ? true : false;
  const {
    paymentService,
    objectStore,
    database,
    logger,
    getArweaveWallet,
    arweaveGateway,
  } = ctx.state;
  try {
    const result = await finalizeMultipartUpload({
      uploadId,
      paymentService,
      objectStore,
      database,
      arweaveGateway,
      getArweaveWallet,
      logger,
      asyncValidation,
    });
    ctx.status = result.newDataItemAdded && asyncValidation ? 201 : 200;
    ctx.body = result.receipt;
  } catch (error) {
    if (error instanceof EnqueuedForValidationError) {
      ctx.status = 202;
      // TODO: Message/body data? Location header for status endpoint?
      return;
    } else if (error instanceof DataItemExistsWarning) {
      ctx.status = 201; // matches Irys (we use 202 on dataItemPost.ts)
      logger.debug(error.message);
      ctx.message = error.message;
      return;
    }
    logger.error("Error finalizing multipart upload", {
      uploadId,
      error: error instanceof Error ? error.message : error,
    });
    if (error instanceof MultiPartUploadNotFound) {
      ctx.status = 404;
      ctx.message = error.message;
    } else if (
      error instanceof InvalidDataItem ||
      error instanceof InvalidChunkSize
    ) {
      ctx.status = 400;
      ctx.message = error.message;
    } else if (error instanceof InsufficientBalance) {
      ctx.status = 402;
      ctx.message = error.message;
    } else if (error instanceof BlocklistedAddressError) {
      ctx.status = 403;
      ctx.message = error.message;
    } else {
      ctx.status = 503;
      ctx.message = "Internal Server Error";
    }
  }
}

export async function finalizeMultipartUpload({
  uploadId,
  paymentService,
  objectStore,
  database,
  arweaveGateway,
  getArweaveWallet,
  logger,
  asyncValidation,
}: {
  uploadId: UploadId;
  paymentService: PaymentService;
  objectStore: ObjectStore;
  database: Database;
  arweaveGateway: ArweaveGateway;
  getArweaveWallet: () => Promise<JWKInterface>;
  logger: winston.Logger;
  asyncValidation: boolean;
}): Promise<{
  receipt: IrysSignedReceipt;
  newDataItemAdded: boolean;
}> {
  const fnLogger = logger.child({ uploadId });

  const finishedMPUEntity = await database
    .getFinalizedMultiPartUpload(uploadId)
    .catch(() => {
      // Could be not found or actual error. If actual error, could lead to a false 404 below
      fnLogger.warn("No finalized entity found.");
      return undefined;
    });

  if (finishedMPUEntity) {
    // If we recently failed, then we're done with this upload
    if (finishedMPUEntity.failedReason) {
      fnLogger.error("Failed multipart upload found.", {
        failedReason: finishedMPUEntity.failedReason,
      });
      // It doesn't matter if they topped up since we last checked. We already cleaned up the file.
      if (finishedMPUEntity.failedReason === "UNDERFUNDED") {
        throw new InsufficientBalance();
      }
      throw new MultiPartUploadNotFound(uploadId);
    }

    // If the data item is already in/through fulfillment, we're done here
    const info = finishedMPUEntity
      ? await database.getDataItemInfo(finishedMPUEntity.dataItemId)
      : undefined;
    if (info) {
      const deadlineHeight =
        info.deadlineHeight ?? // TODO: Remove this fallback after all data items have a deadline height
        (await estimatedBlockHeightAtTimestamp(
          info.uploadedTimestamp,
          arweaveGateway
        )) + deadlineHeightIncrement;

      // Regenerate and transmit receipt
      const receipt: IrysUnsignedReceipt = {
        id: finishedMPUEntity.dataItemId,
        timestamp: info.uploadedTimestamp,
        version: receiptVersion,
        deadlineHeight,
      };

      const jwk = await getArweaveWallet();
      const signedReceipt = await signIrysReceipt(receipt, jwk);
      logger.debug("Receipt signed!", signedReceipt);

      return {
        receipt: signedReceipt,
        newDataItemAdded: false,
      };
    }
  }

  fnLogger.info("Finalizing multipart upload");

  if (finishedMPUEntity) {
    fnLogger.info(`Resuming upload finalization...`, {
      ...finishedMPUEntity,
    });

    const signedReceipt = await finalizeMPUWithValidatedInfo({
      uploadId,
      objectStore,
      paymentService,
      database,
      arweaveGateway,
      getArweaveWallet,
      validatedUploadInfo: {
        uploadKey: finishedMPUEntity.uploadKey,
        dataItemId: finishedMPUEntity.dataItemId,
        etag: finishedMPUEntity.etag,
      },
      logger: fnLogger,
    });
    return {
      receipt: signedReceipt,
      newDataItemAdded: true,
    };
  }

  const inFlightMPUEntity = await database.getInflightMultiPartUpload(uploadId);
  const signedReceipt = await finalizeMPUWithInFlightEntity({
    uploadId,
    paymentService,
    objectStore,
    database,
    arweaveGateway,
    getArweaveWallet,
    inFlightMPUEntity,
    logger: fnLogger,
    asyncValidation,
  });

  return {
    receipt: signedReceipt,
    newDataItemAdded: true,
  };
}

export async function finalizeMPUWithInFlightEntity({
  uploadId,
  paymentService,
  objectStore,
  database,
  arweaveGateway,
  getArweaveWallet,
  inFlightMPUEntity,
  logger,
  asyncValidation,
}: {
  uploadId: UploadId;
  paymentService: PaymentService;
  objectStore: ObjectStore;
  database: Database;
  arweaveGateway: ArweaveGateway;
  getArweaveWallet: () => Promise<JWKInterface>;
  inFlightMPUEntity: InFlightMultiPartUpload;
  logger: winston.Logger;
  asyncValidation: boolean;
}): Promise<IrysSignedReceipt> {
  if (inFlightMPUEntity.failedReason) {
    throw new InvalidDataItem();
  }

  // At this point we know that the data item has not yet reached fulfillment
  // and has not yet been validated. We'll stream out the headers of the data
  // item for accounting as well as the payload for validation and then move on
  // to finalize the multipart upload.
  let fnLogger = logger;

  const getMultipartUpload = async () =>
    await getMultipartUploadObject(
      objectStore,
      inFlightMPUEntity.uploadKey
    ).then((multipartUploadObject) => {
      multipartUploadObject.readable.on("error", () =>
        fnLogger.debug("Ending stream for data item")
      );
      return multipartUploadObject;
    });

  const verifyDataStartTime = Date.now();
  let { readable: dataItemReadable, etag: finalizedEtag } =
    await getMultipartUpload().catch((error) => {
      if ((error as { Code: string }).Code === "AccessDenied") {
        fnLogger.debug(
          "Access denied to multipart upload object, key may not yet exist."
        );
      } else {
        fnLogger.error("Error getting multipart upload object", {
          error,
        });
      }
      return { readable: undefined, etag: undefined };
    });

  if (dataItemReadable === undefined || finalizedEtag === undefined) {
    // Check for parts. If it throws, we're done... (hopefully 404)
    await getMultipartUploadParts(
      objectStore,
      inFlightMPUEntity.uploadKey,
      uploadId
    );
    const completeMultipartStartTime = Date.now();

    finalizedEtag = await completeMultipartUpload(
      objectStore,
      inFlightMPUEntity.uploadKey,
      uploadId
    );
    fnLogger = fnLogger.child({ finalizedEtag });
    fnLogger.debug(`Finalized upload in object store `, {
      durationMs: Date.now() - completeMultipartStartTime,
    });

    if (asyncValidation) {
      await enqueue("finalize-upload", {
        uploadId,
      });

      // Use a thrown custom error to shift control flow
      throw new EnqueuedForValidationError(uploadId);
    }
  }

  // Stream out the data items headers (for accounting and receipts) and verify the data item
  const multipartUploadObject = await getMultipartUpload();
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  dataItemReadable = multipartUploadObject.readable!;
  // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
  finalizedEtag = multipartUploadObject.etag!;
  const dataItemStream = new StreamingDataItem(dataItemReadable, fnLogger);
  const cleanUpDataItemStreamAndThrow = (error: Error) => {
    dataItemReadable?.destroy();
    throw error;
  };
  const dataItemHeaders = await dataItemStream
    .getHeaders()
    .catch(cleanUpDataItemStreamAndThrow);
  const {
    id: dataItemId,
    dataOffset: payloadDataStart,
    tags,
    signature,
  } = dataItemHeaders;
  const signatureType = await dataItemStream
    .getSignatureType()
    .catch(cleanUpDataItemStreamAndThrow);
  const ownerPublicAddress = await dataItemStream
    .getOwnerAddress()
    .catch(cleanUpDataItemStreamAndThrow);

  // Perform blocklist checking before consuming the (potentially large) remainder of the stream
  if (blocklistedAddresses.includes(ownerPublicAddress)) {
    logger.info(
      "The owner's address is on the arweave public address block list. Rejecting data item..."
    );

    // end the stream
    dataItemReadable.destroy();
    throw new BlocklistedAddressError();
  }

  const payloadContentType = payloadContentTypeFromDecodedTags(tags);
  const isValid = await dataItemStream
    .isValid()
    .catch(cleanUpDataItemStreamAndThrow);
  if (!isValid) {
    await database.failInflightMultiPartUpload({
      uploadId,
      failedReason: "INVALID",
    });
    dataItemReadable.destroy();
    throw new InvalidDataItem();
  }
  const payloadDataByteCount = await dataItemStream
    .getPayloadSize()
    .catch(cleanUpDataItemStreamAndThrow);
  const rawDataItemByteCount = payloadDataStart + payloadDataByteCount;

  // end the stream
  dataItemReadable.destroy();

  fnLogger.debug(`Data item stream consumed and verified!`, {
    durationMs: Date.now() - verifyDataStartTime,
    msPerByte: (Date.now() - verifyDataStartTime) / payloadDataByteCount,
  });

  const premiumFeatureType = getPremiumFeatureType(ownerPublicAddress, tags);

  // Prepare the data needed for optical posting and new_data_item insert
  const dataItemInfo: Omit<
    PostedNewDataItem,
    "uploadedDate" | "deadlineHeight"
  > = {
    dataItemId,
    payloadDataStart,
    byteCount: rawDataItemByteCount,
    ownerPublicAddress,
    payloadContentType,
    premiumFeatureType,
    signatureType,
    assessedWinstonPrice: W("0"), // Stubbed until new_data_item insert
    failedBundles: [],
    signature: fromB64Url(signature),
  };
  fnLogger = fnLogger.child(filterKeysFromObject(dataItemInfo, ["signature"]));

  // TODO: handle bdis?

  fnLogger.info("Parsed multi-part upload data item id and tags", {
    tags,
  });

  // NOTE: THIS PREVENTS THE FURTHER UPLOADING OF CHUNKS!
  // FUTURE: DO THIS BEFORE VERIFYING THE DATA ITEM AND JUST RETURN 202 AFTER SQS ENQUEUE. DO VERIFY ASYNC. RETURN VERIFYING IN STATUS ENDPOINT.
  await database.finalizeMultiPartUpload({
    uploadId,
    etag: finalizedEtag,
    dataItemId,
  });

  return await finalizeMPUWithValidatedInfo({
    uploadId,
    objectStore,
    paymentService,
    database,
    arweaveGateway,
    getArweaveWallet,
    validatedUploadInfo: {
      uploadKey: inFlightMPUEntity.uploadKey,
      etag: finalizedEtag,
      dataItemId: dataItemHeaders.id,
    },
    logger,
  });
}

export async function finalizeMPUWithValidatedInfo({
  uploadId,
  objectStore,
  paymentService,
  database,
  arweaveGateway,
  getArweaveWallet,
  validatedUploadInfo,
  logger,
}: {
  uploadId: UploadId;
  objectStore: ObjectStore;
  paymentService: PaymentService;
  database: Database;
  arweaveGateway: ArweaveGateway;
  getArweaveWallet: () => Promise<JWKInterface>;
  validatedUploadInfo: {
    uploadKey: string;
    etag: string;
    dataItemId: string;
  };
  logger: winston.Logger;
}): Promise<IrysSignedReceipt> {
  // If we're here, then we know we've previously completed the multipart upload and
  // validated the data item, but the data item isn't yet in the fulfillment pipeline.
  // We'll have to determine whether we still have to move the data item into the raw data
  // items prefix, insert the data item into the database for fulfillment, enqueue it
  // for optical posting, and/or returned the signed receipt to the client.
  let fnLogger = logger;

  const { uploadKey, dataItemId } = validatedUploadInfo;

  if (await rawDataItemExists(objectStore, validatedUploadInfo.dataItemId)) {
    return await finalizeMPUWithRawDataItem({
      uploadId,
      paymentService,
      objectStore,
      database,
      arweaveGateway,
      getArweaveWallet,
      dataItemId,
      logger: fnLogger,
    });
  }

  const multipartUploadStream = (
    await getMultipartUploadObject(objectStore, uploadKey)
  ).readable;

  const dataItemStream = new StreamingDataItem(multipartUploadStream, fnLogger);
  const cleanUpDataItemStreamAndThrow = (error: Error) => {
    multipartUploadStream.destroy();
    throw error;
  };
  const dataItemHeaders = await dataItemStream
    .getHeaders()
    .catch(cleanUpDataItemStreamAndThrow);
  const signatureType = await dataItemStream
    .getSignatureType()
    .catch(cleanUpDataItemStreamAndThrow);
  const ownerPublicAddress = await dataItemStream
    .getOwnerAddress()
    .catch(cleanUpDataItemStreamAndThrow);
  const payloadDataStart = dataItemHeaders.dataOffset;
  const payloadContentType = payloadContentTypeFromDecodedTags(
    dataItemHeaders.tags
  );
  multipartUploadStream.destroy();
  const rawDataItemByteCount = await getMultiPartUploadByteCount(
    objectStore,
    uploadKey
  );

  // move the data item to data items prefix
  const moveMultiPartObjectStart = Date.now();
  await moveFinalizedMultipartObject(
    objectStore,
    uploadKey,
    dataItemId,
    payloadContentType,
    payloadDataStart
  );
  fnLogger.debug(`Finished moving multi part object`, {
    durationMs: Date.now() - moveMultiPartObjectStart,
    msPerByte: (Date.now() - moveMultiPartObjectStart) / rawDataItemByteCount,
  });

  fnLogger = fnLogger.child({ ...validatedUploadInfo });
  fnLogger.debug("Moved multi part object");

  return await finalizeMPUWithDataItemInfo({
    uploadId,
    objectStore,
    paymentService,
    database,
    arweaveGateway,
    getArweaveWallet,
    dataItemInfo: {
      dataItemId,
      payloadDataStart,
      byteCount: rawDataItemByteCount,
      ownerPublicAddress,
      payloadContentType,
      premiumFeatureType: getPremiumFeatureType(
        ownerPublicAddress,
        dataItemHeaders.tags
      ),
      signatureType,
      assessedWinstonPrice: W("0"), // Stubbed until new_data_item insert
      failedBundles: [],
      signature: fromB64Url(dataItemHeaders.signature),
      target: dataItemHeaders.target,
      tags: dataItemHeaders.tags,
      owner: dataItemHeaders.owner,
    },
    logger: fnLogger,
  });
}

export async function finalizeMPUWithRawDataItem({
  uploadId,
  paymentService,
  objectStore,
  database,
  arweaveGateway,
  getArweaveWallet,
  dataItemId,
  logger,
}: {
  uploadId: UploadId;
  paymentService: PaymentService;
  objectStore: ObjectStore;
  database: Database;
  arweaveGateway: ArweaveGateway;
  getArweaveWallet: () => Promise<JWKInterface>;
  dataItemId: string;
  logger: winston.Logger;
}) {
  logger.info(
    "Resuming finalization of multipart upload with existing raw data item"
  );

  // A previous attempt to finalize made it through to moving the data item
  // in place for bundling and serving, but a database entry was not made.
  // Complete the process by using the data items headers to prepare the
  // data necessary for an optical post and a new data item db entry.
  const rawDataItemStream = await getRawDataItem(objectStore, dataItemId);
  const dataItemStream = new StreamingDataItem(rawDataItemStream, logger);
  const cleanUpDataItemStreamAndThrow = (error: Error) => {
    rawDataItemStream.destroy();
    throw error;
  };
  const dataItemHeaders = await dataItemStream
    .getHeaders()
    .catch(cleanUpDataItemStreamAndThrow);
  const ownerPublicAddress = await dataItemStream
    .getOwnerAddress()
    .catch(cleanUpDataItemStreamAndThrow);
  const signatureType = await dataItemStream
    .getSignatureType()
    .catch(cleanUpDataItemStreamAndThrow);
  const rawDataItemByteCount = await getRawDataItemByteCount(
    objectStore,
    dataItemId
  );
  rawDataItemStream.destroy();

  // Prepare the data needed for optical posting and new_data_item insert
  const dataItemInfo: Omit<
    PostedNewDataItem,
    "uploadedDate" | "deadlineHeight"
  > = {
    dataItemId,
    payloadDataStart: dataItemHeaders.dataOffset,
    byteCount: rawDataItemByteCount,
    ownerPublicAddress,
    payloadContentType: payloadContentTypeFromDecodedTags(dataItemHeaders.tags),
    premiumFeatureType: getPremiumFeatureType(
      ownerPublicAddress,
      dataItemHeaders.tags
    ),
    signatureType,
    assessedWinstonPrice: W("0"), // Stubbed until new_data_item insert
    failedBundles: [],
    signature: fromB64Url(dataItemHeaders.signature),
  };

  return await finalizeMPUWithDataItemInfo({
    uploadId,
    objectStore,
    paymentService,
    database,
    arweaveGateway,
    getArweaveWallet,
    dataItemInfo: {
      ...dataItemInfo,
      target: dataItemHeaders.target,
      tags: dataItemHeaders.tags,
      owner: dataItemHeaders.owner,
    },
    logger,
  });
}

export async function finalizeMPUWithDataItemInfo({
  uploadId,
  objectStore,
  paymentService,
  database,
  arweaveGateway,
  getArweaveWallet,
  dataItemInfo,
  logger,
}: {
  uploadId: UploadId;
  objectStore: ObjectStore;
  dataItemInfo: Omit<PostedNewDataItem, "uploadedDate" | "deadlineHeight"> & {
    owner: Base64UrlString;
    target: string | undefined;
    tags: Tag[];
  };
  paymentService: PaymentService;
  database: Database;
  arweaveGateway: ArweaveGateway;
  getArweaveWallet: () => Promise<JWKInterface>;
  logger: winston.Logger;
}): Promise<IrysSignedReceipt> {
  // At the point, the DB has finalized the in flight upload, the validated data item is in
  // the raw data item bucket, and we now need to reserve balance at payment svc, optical post,
  // construct the receipt, insert the data item into the database, and return the receipt.
  let fnLogger = logger;
  const uploadTimestamp = Date.now();
  const currentBlockHeight = await arweaveGateway.getCurrentBlockHeight();
  const deadlineHeight = currentBlockHeight + deadlineHeightIncrement;

  const receipt: IrysUnsignedReceipt = {
    id: dataItemInfo.dataItemId,
    timestamp: uploadTimestamp,
    version: receiptVersion,
    deadlineHeight,
  };

  const jwk = await getArweaveWallet();
  const signedReceipt = await signIrysReceipt(receipt, jwk);
  fnLogger.info("Receipt signed!", signedReceipt);

  fnLogger.debug("Reserving balance for upload...");
  const paymentResponse: ReserveBalanceResponse = shouldSkipBalanceCheck
    ? { isReserved: true, costOfDataItem: W("0"), walletExists: true }
    : await paymentService.reserveBalanceForData({
        nativeAddress: ownerToNativeAddress(
          dataItemInfo.owner,
          dataItemInfo.signatureType
        ),
        size: dataItemInfo.byteCount,
        dataItemId: dataItemInfo.dataItemId,
        signatureType: dataItemInfo.signatureType,
      });
  fnLogger = fnLogger.child({
    paymentResponse,
    byteCount: dataItemInfo.byteCount,
    ownerAddress: dataItemInfo.ownerPublicAddress,
  });
  fnLogger.debug("Finished reserving balance for upload.");

  if (paymentResponse.isReserved) {
    dataItemInfo.assessedWinstonPrice = paymentResponse.costOfDataItem;
    fnLogger.debug("Balance successfully reserved", {
      assessedWinstonPrice: paymentResponse.costOfDataItem,
    });
  } else {
    fnLogger.error(`Failing multipart upload due to insufficient balance.`);
    void removeDataItem(objectStore, dataItemInfo.dataItemId, database); // don't need to await this - just invoke and move on
    await database.failFinishedMultiPartUpload({
      uploadId,
      failedReason: "UNDERFUNDED",
    });
    throw new InsufficientBalance();
  }

  if (
    opticalBridgingEnabled &&
    !skipOpticalPostAddresses.includes(dataItemInfo.ownerPublicAddress)
  ) {
    fnLogger.debug("Asynchronously optical posting...");
    try {
      void enqueue(
        "optical-post",
        await signDataItemHeader(
          encodeTagsForOptical({
            id: dataItemInfo.dataItemId,
            signature: toB64Url(dataItemInfo.signature),
            owner: dataItemInfo.owner,
            owner_address: dataItemInfo.ownerPublicAddress,
            target: dataItemInfo.target ?? "",
            content_type: dataItemInfo.payloadContentType,
            data_size: dataItemInfo.byteCount - dataItemInfo.payloadDataStart,
            tags: dataItemInfo.tags,
          })
        )
      ).catch((error) => {
        fnLogger.error("Error enqueuing data item to optical", { error });
        MetricRegistry.opticalBridgeEnqueueFail.inc();
      });
    } catch (err) {
      // Ran into this case in local development with an absent SQS queue URL env var
      fnLogger.error("Error enqueuing data item to optical", { err });
      MetricRegistry.opticalBridgeEnqueueFail.inc();
    }
  } else {
    // Attach skip feature to logger for log parsing in final receipt log statement
    fnLogger = fnLogger.child({ skipOpticalPost: true });
    fnLogger.debug("Skipped optical posting.");
  }

  fnLogger.debug("Inserting new_data_item into db...");
  const dbInsertStart = Date.now();
  // TODO: Add deadline height to the new data item entity
  try {
    await database.insertNewDataItem({
      ...dataItemInfo,
      uploadedDate: new Date(uploadTimestamp).toISOString(),
      deadlineHeight,
    });
    fnLogger.debug(`DB insert duration:ms`, {
      durationMs: Date.now() - dbInsertStart,
    });
  } catch (error) {
    const dataItemExists = error instanceof DataItemExistsWarning;
    fnLogger.debug(
      `DB ${dataItemExists ? "insert exists" : "insert failed"} duration: ${
        Date.now() - dbInsertStart
      }ms`
    );
    if (paymentResponse.costOfDataItem.isGreaterThan(W(0))) {
      await paymentService.refundBalanceForData({
        signatureType: dataItemInfo.signatureType,
        nativeAddress: ownerToNativeAddress(
          dataItemInfo.owner,
          dataItemInfo.signatureType
        ),
        winston: paymentResponse.costOfDataItem,
        dataItemId: dataItemInfo.dataItemId,
      });
      fnLogger.info(`Balance refunded due to database error.`, {
        assessedWinstonPrice: paymentResponse.costOfDataItem,
      });
    }
    if (!dataItemExists) {
      void removeDataItem(objectStore, dataItemInfo.dataItemId, database); // don't need to await this - just invoke and move on
    }
    throw error;
  }

  return signedReceipt;
}

// TODO: GET RID OF THIS ONCE WE START SAVING DEADLINE HEIGHTS TO THE DB
async function estimatedBlockHeightAtTimestamp(
  timestamp: number,
  arweaveGateway: ArweaveGateway
) {
  const currentBlockHeight = await arweaveGateway.getCurrentBlockHeight();
  const currentBlockTimestamp = await arweaveGateway.getCurrentBlockTimestamp();
  const timestampsDifference = currentBlockTimestamp - timestamp;
  const blockHeightDifference = Math.floor(
    timestampsDifference / 1000 /*ms/sec*/ / 60 /*sec/min*/ / 2 /*min/block*/
  );
  return currentBlockHeight - blockHeightDifference;
}
