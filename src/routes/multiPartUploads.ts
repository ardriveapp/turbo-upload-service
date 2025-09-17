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
import { ReadThroughPromiseCache } from "@ardrive/ardrive-promise-cache";
import { Message } from "@aws-sdk/client-sqs";
import { JWKInterface, SignatureConfig, Tag } from "@dha-team/arbundles";
import { Base64UrlString } from "arweave/node/lib/utils";
import crypto from "node:crypto";
import winston from "winston";

import { ArweaveGateway } from "../arch/arweaveGateway";
import { Database } from "../arch/db/database";
import { getElasticacheService } from "../arch/elasticacheService";
import { ObjectStore } from "../arch/objectStore";
import { PaymentService, ReserveBalanceResponse } from "../arch/payment";
import { EnqueueFinalizeUpload, enqueue } from "../arch/queues";
import { StreamingDataItem } from "../bundles/streamingDataItem";
import {
  approvalAmountTagName,
  approvalExpiresBySecondsTagName,
  blocklistedAddresses,
  createDelegatedPaymentApprovalTagName,
  dataCaches,
  deadlineHeightIncrement,
  fastFinalityIndexes,
  jobLabels,
  multipartChunkMaxSize,
  multipartChunkMinSize,
  multipartDefaultChunkSize,
  receiptVersion,
  revokeDelegatePaymentApprovalTagName,
  skipOpticalPostAddresses,
} from "../constants";
import { MetricRegistry } from "../metricRegistry";
import { KoaContext } from "../server";
import { InFlightMultiPartUpload, PostedNewDataItem } from "../types/dbTypes";
import { NativeAddress, UploadId } from "../types/types";
import { W } from "../types/winston";
import { fromB64Url, toB64Url } from "../utils/base64";
import {
  filterKeysFromObject,
  getPremiumFeatureType,
  payloadContentTypeFromDecodedTags,
  sleep,
} from "../utils/common";
import { quarantineDataItem } from "../utils/dataItemUtils";
import {
  BlocklistedAddressError,
  DataItemExistsWarning,
  EnqueuedForValidationError,
  InsufficientBalance,
  InvalidChunk,
  InvalidChunkSize,
  InvalidDataItem,
  MultiPartUploadNotFound,
  PaymentServiceReturnedError,
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
  rawDataItemObjectExists,
  uploadPart,
} from "../utils/objectStoreUtils";
import {
  containsAns104Tags,
  encodeTagsForOptical,
  signDataItemHeader,
} from "../utils/opticalUtils";
import { ownerToNativeAddress } from "../utils/ownerToNativeAddress";
import {
  IrysSignedReceipt,
  IrysUnsignedReceipt,
  SignedReceipt,
  signIrysReceipt,
  signReceipt,
} from "../utils/signReceipt";

const shouldSkipBalanceCheck = process.env.SKIP_BALANCE_CHECKS === "true";
const opticalBridgingEnabled = process.env.OPTICAL_BRIDGING_ENABLED !== "false";
const maxAllowablePartNumber = 10_000; // AWS S3 MultiPartUpload part number limitation

const inFlightUploadCache = new ReadThroughPromiseCache<
  UploadId,
  InFlightMultiPartUpload,
  Database
>({
  cacheParams: {
    cacheCapacity: 1000,
    cacheTTLMillis: 60_000,
  },
  readThroughFunction: async (uploadId, database) => {
    return database.getInflightMultiPartUpload(uploadId);
  },
  metricsConfig: {
    cacheName: "mpu_in_flight_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV ?? "local",
    },
  },
});

export async function createMultiPartUpload(ctx: KoaContext) {
  const { database, objectStore, logger } = ctx.state;

  const chunkSizeRaw = ctx.query.chunkSize;
  const chunkSize =
    typeof chunkSizeRaw === "string" ? parseInt(chunkSizeRaw, 10) : undefined;
  if (
    chunkSize !== undefined &&
    (chunkSize < multipartChunkMinSize || chunkSize > multipartChunkMaxSize)
  ) {
    ctx.status = 400;
    ctx.body = {
      error: "Invalid chunk size",
      min: multipartChunkMinSize,
      max: multipartChunkMaxSize,
    };
    return;
  }

  logger.debug("Creating new multipart upload");
  const uploadKey = crypto.randomUUID();
  const newUploadId = await createMultipartUpload(objectStore, uploadKey);

  logger.debug("Created new multipart upload", { newUploadId });
  // create new upload
  await inFlightUploadCache.put(
    newUploadId,
    database.insertInFlightMultiPartUpload({
      uploadId: newUploadId,
      uploadKey,
      chunkSize,
    })
  );

  // In order to combat RDS replication-lag-related issues with posting chunks (parts)
  // for this uploadId immediately after receiving the fresh uploadId, impose an
  // arbitrary 250ms delay here.
  await sleep(250); // TODO: Lower this after Service Level Cache is implemented for multi-part uploads

  logger.info("Inserted new multipart upload into database", { newUploadId });

  ctx.body = {
    id: newUploadId,
    max: multipartChunkMaxSize,
    min: multipartChunkMinSize,
    chunkSize: chunkSize,
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

    const chunkSize = upload.chunkSize || multipartDefaultChunkSize;
    // TODO: Could add finalization status here without having to add a new endpoint
    ctx.body = {
      id: upload.uploadId,
      max: multipartChunkMaxSize,
      min: multipartChunkMinSize,
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
  const { database, logger, objectStore, getArweaveWallet } = ctx.state;
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
      ctx.status = 503;
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

    let signedReceipt: SignedReceipt | undefined;
    if (fulfillmentInfo !== undefined) {
      if (!fulfillmentInfo.deadlineHeight) {
        logger.warn(
          "Data item info is missing deadlineHeight! Cannot re-calculate receipt signature",
          {
            info: fulfillmentInfo,
          }
        );
      } else {
        try {
          signedReceipt = await signReceipt(
            {
              dataCaches,
              fastFinalityIndexes,
              id: validatedUpload.dataItemId,
              winc: fulfillmentInfo.assessedWinstonPrice.toString(),
              timestamp: fulfillmentInfo.uploadedTimestamp,
              deadlineHeight: fulfillmentInfo.deadlineHeight,
              version: receiptVersion,
            },
            await getArweaveWallet()
          );
          logger.debug("Finalized receipt successfully re-signed", {
            uploadId,
          });
        } catch (error) {
          logger.error("Error signing receipt", {
            uploadId,
            error: error instanceof Error ? error.message : error,
          });
          // If signing fails, we proceed without the signed receipt
        }
      }
    }

    // TODO: Sign this in the future
    ctx.body = {
      status: validatedUpload.failedReason
        ? validatedUpload.failedReason
        : fulfillmentInfo
        ? "FINALIZED"
        : "FINALIZING",
      timestamp: Date.now(),
      receipt:
        signedReceipt === undefined
          ? undefined
          : // Append owner to match data item post (/tx) return type
            { ...signedReceipt, owner: fulfillmentInfo?.owner },
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
      ctx.status = 503;
      ctx.message = "Internal Server Error";
    }
    return;
  }
}

export async function postDataItemChunk(ctx: KoaContext) {
  const { uploadId, chunkOffset } = ctx.params;
  const { objectStore, database, logger } = ctx.state;

  const contentLength = ctx.req.headers["content-length"];

  if (!contentLength || isNaN(+contentLength) || +contentLength <= 0) {
    ctx.status = 400;
    ctx.message =
      "Content-Length header is required a must be a positive integer.";
    return;
  }

  logger.debug("Posting data item chunk", { uploadId, chunkOffset });
  // check that upload exists
  try {
    const upload = await inFlightUploadCache.get(uploadId, database);
    logger.debug("Got multipart upload", { ...upload });

    // No need to proceed if this upload has already failed
    if (upload.failedReason) {
      throw new InvalidDataItem();
    }

    const sizeOfIncomingChunk = +contentLength;
    const expectedChunkSize = await computeExpectedChunkSize({
      upload,
      sizeOfIncomingChunk,
      logger,
      database,
    });

    if (chunkOffset % expectedChunkSize !== 0) {
      /* This can happen when the last chunk is processed first and
         has a size that is not a multiple of the expected chunk size.
         Retrying that chunk upload should usually clear that up.

         A problematic case is when the chunk is smaller than the intended
         chunk size but is a multiple of it. In this case, we can't tell
         if the chunk size is wrong or if the chunk is the last one. But
         two outcomes are possible there:
         1) The computed part number is large, but sufficiently higher than
            the preceding chunk's will be. If so, the upload can still complete.
         2) The part number chosen is too large, and the chunk upload will fail,
            but might succeed on a successive try. Forcing the part number to
            the max allowed in this case is not worth the risk of getting it wrong.
      */

      // TODO: Could also check db again for updated chunk size from other chunks
      inFlightUploadCache.remove(uploadId); // Precautionary measure
      throw new InvalidChunk();
    }

    const partNumber = Math.floor(chunkOffset / expectedChunkSize) + 1; // + 1 due to 1-indexing of part numbers
    if (partNumber > maxAllowablePartNumber) {
      // This can happen if the user chose a chunk size too small for the number of chunks their upload needs
      logger.error("Part number exceeds maximum allowable part number", {
        uploadId,
        partNumber,
        chunkOffset,
        expectedChunkSize,
        sizeOfIncomingChunk,
      });
      throw new InvalidChunk();
    }

    // Need to give content length here for last chunk or s3 will wait for more data
    const etag = await uploadPart({
      objectStore,
      uploadKey: upload.uploadKey,
      stream: ctx.req,
      uploadId,
      partNumber,
      sizeOfChunk: sizeOfIncomingChunk,
    });
    logger.info("Uploaded part", {
      uploadId,
      partNumber,
      etag,
      sizeOfChunk: sizeOfIncomingChunk,
    });

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

async function computeExpectedChunkSize({
  upload,
  logger,
  sizeOfIncomingChunk,
  database,
}: {
  upload: InFlightMultiPartUpload;
  sizeOfIncomingChunk: number;
  logger: winston.Logger;
  database: Database;
}): Promise<number> {
  const uploadId = upload.uploadId;
  let expectedChunkSize = upload.chunkSize;

  if (!expectedChunkSize || sizeOfIncomingChunk > expectedChunkSize) {
    logger.debug("Updating chunk size in database", {
      uploadId,
      prevChunkSize: expectedChunkSize,
      newChunkSize: sizeOfIncomingChunk,
    });
    // NOTE: this may be better suited in a redis + read through cache
    expectedChunkSize = await database.updateMultipartChunkSize(
      sizeOfIncomingChunk,
      upload
    );
    // Memoize this update
    upload.chunkSize = expectedChunkSize;
    void inFlightUploadCache.put(uploadId, Promise.resolve(upload));
    logger.debug("Successfully updated chunk size 👍", {
      uploadId,
      estimatedSizeOfIncomingChunk: sizeOfIncomingChunk,
      expectedChunkSize,
    });
  }
  logger.debug("Retrieved chunk size for upload", {
    uploadId,
    sizeOfChunk: sizeOfIncomingChunk,
    expectedChunkSize,
  });

  return expectedChunkSize;
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
  const body: EnqueueFinalizeUpload = JSON.parse(message.Body ?? "");
  const uploadId = body.uploadId;
  if (!uploadId) {
    throw new Error(
      "Malformed message! Expected string key 'uploadId' in message body."
    );
  }

  const token = body.token ?? "arweave";

  await finalizeMultipartUpload({
    uploadId,
    paymentService,
    objectStore,
    database,
    arweaveGateway,
    getArweaveWallet,
    logger,
    asyncValidation: false,
    token,
    paidBy: body.paidBy,
  });
}

export async function finalizeMultipartUploadWithHttpRequest(ctx: KoaContext) {
  const { uploadId, token } = ctx.params;

  const paidBys: string[] = [];
  ctx.request.req.rawHeaders.forEach((header, index) => {
    if (header === "x-paid-by") {
      // get x-paid-by values from raw headers
      const rawPaidBy = ctx.request.req.rawHeaders[index + 1];
      if (rawPaidBy) {
        // split by comma and trim whitespace
        const paidByAddresses = rawPaidBy
          .split(",")
          .map((address) => address.trim());
        paidBys.push(...paidByAddresses);
      }
    }
  });
  const paidBy = paidBys.length > 0 ? paidBys : undefined;

  const asyncValidation = ctx.state.asyncValidation ? true : false;
  const {
    paymentService,
    objectStore,
    database,
    logger,
    getArweaveWallet,
    arweaveGateway,
  } = ctx.state;

  logger.debug("Finalizing via HTTP request", {
    paidBy,
    uploadId,
    token,
    asyncValidation,
    rawHeaders: ctx.request.req.rawHeaders,
  });
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
      token: token ?? "arweave",
      paidBy,
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

type RemainingUploadResponse = {
  dataCaches: string[];
  fastFinalityIndexes: string[];
  owner: string;
  winc: string;
};
type MultiPartUploadResponse = IrysSignedReceipt & RemainingUploadResponse;

export async function finalizeMultipartUpload({
  uploadId,
  paymentService,
  objectStore,
  database,
  arweaveGateway,
  getArweaveWallet,
  logger,
  asyncValidation,
  token,
  paidBy,
}: {
  uploadId: UploadId;
  paymentService: PaymentService;
  objectStore: ObjectStore;
  database: Database;
  arweaveGateway: ArweaveGateway;
  getArweaveWallet: () => Promise<JWKInterface>;
  logger: winston.Logger;
  asyncValidation: boolean;
  token: string;
  paidBy?: NativeAddress[];
}): Promise<{
  receipt: MultiPartUploadResponse;
  newDataItemAdded: boolean;
}> {
  const fnLogger = logger.child({ uploadId, paidBy, token });

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
    const info = await database.getDataItemInfo(finishedMPUEntity.dataItemId);
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
        receipt: {
          ...signedReceipt,
          dataCaches,
          fastFinalityIndexes,
          owner: info.owner,
          winc: info.assessedWinstonPrice.toString(),
        },
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
      token,
      paidBy,
    });
    return {
      receipt: signedReceipt,
      newDataItemAdded: true,
    };
  }

  const inFlightMPUEntity = await inFlightUploadCache.get(uploadId, database);
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
    token,
    paidBy,
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
  token,
  paidBy,
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
  token: string;
  paidBy?: NativeAddress[];
}): Promise<MultiPartUploadResponse> {
  if (inFlightMPUEntity.failedReason) {
    throw new InvalidDataItem();
  }

  let signatureTypeOverride: number | undefined;
  if (token === "kyve") {
    signatureTypeOverride = SignatureConfig.KYVE;
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
  const multipartUploadObject = await getMultipartUpload().catch((error) => {
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
  let dataItemReadable = multipartUploadObject.readable;
  let finalizedEtag = multipartUploadObject.etag;

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
      paidBy,
      token,
      finalizedEtag,
      asyncValidation,
    });

    if (asyncValidation) {
      await enqueue(jobLabels.finalizeUpload, {
        uploadId,
        token,
        paidBy,
      });

      // Use a thrown custom error to shift control flow
      throw new EnqueuedForValidationError(uploadId);
    }

    dataItemReadable ??= (await getMultipartUpload()).readable;
  }

  // Stream out the data items headers (for accounting and receipts) and verify the data item
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
  const signatureType =
    signatureTypeOverride ??
    (await dataItemStream
      .getSignatureType()
      .catch(cleanUpDataItemStreamAndThrow));
  const ownerPublicAddress = await dataItemStream
    .getOwnerAddress()
    .catch(cleanUpDataItemStreamAndThrow);
  const targetPublicAddress = await dataItemStream
    .getTarget()
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
    inFlightUploadCache.remove(uploadId);
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

  const premiumFeatureType = getPremiumFeatureType(
    ownerPublicAddress,
    tags,
    signatureType,
    [], // TODO: get nested data item headers on multi-part uploads
    targetPublicAddress
  );

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
  inFlightUploadCache.remove(uploadId);

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
    token,
    paidBy,
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
  token,
  paidBy,
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
  token: string;
  paidBy?: NativeAddress[];
}): Promise<MultiPartUploadResponse> {
  // If we're here, then we know we've previously completed the multipart upload and
  // validated the data item, but the data item isn't yet in the fulfillment pipeline.
  // We'll have to determine whether we still have to move the data item into the raw data
  // items prefix, insert the data item into the database for fulfillment, enqueue it
  // for optical posting, and/or returned the signed receipt to the client.
  let fnLogger = logger;

  let signatureTypeOverride: number | undefined;
  if (token === "kyve") {
    signatureTypeOverride = SignatureConfig.KYVE;
  }

  const { uploadKey, dataItemId } = validatedUploadInfo;

  if (
    await rawDataItemObjectExists(objectStore, validatedUploadInfo.dataItemId)
  ) {
    return await finalizeMPUWithRawDataItem({
      uploadId,
      paymentService,
      objectStore,
      database,
      arweaveGateway,
      getArweaveWallet,
      dataItemId,
      logger: fnLogger,
      token,
      paidBy,
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
  const signatureType =
    signatureTypeOverride ??
    (await dataItemStream
      .getSignatureType()
      .catch(cleanUpDataItemStreamAndThrow));
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
        dataItemHeaders.tags,
        signatureType,
        [] // TODO: get nested data item headers on multi-part uploads
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
    paidBy,
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
  token,
  paidBy,
}: {
  uploadId: UploadId;
  paymentService: PaymentService;
  objectStore: ObjectStore;
  database: Database;
  arweaveGateway: ArweaveGateway;
  getArweaveWallet: () => Promise<JWKInterface>;
  dataItemId: string;
  logger: winston.Logger;
  token: string;
  paidBy?: NativeAddress[];
}) {
  logger.info(
    "Resuming finalization of multipart upload with existing raw data item"
  );

  let signatureTypeOverride: number | undefined;
  if (token === "kyve") {
    signatureTypeOverride = SignatureConfig.KYVE;
  }

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
  const targetPublicAddress = await dataItemStream
    .getTarget()
    .catch(cleanUpDataItemStreamAndThrow);
  const signatureType =
    signatureTypeOverride ??
    (await dataItemStream
      .getSignatureType()
      .catch(cleanUpDataItemStreamAndThrow));
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
      dataItemHeaders.tags,
      signatureType,
      [], // TODO: get nested data item headers on multi-part uploads
      targetPublicAddress
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
    paidBy,
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
  paidBy,
}: {
  uploadId: UploadId;
  objectStore: ObjectStore;
  dataItemInfo: Omit<PostedNewDataItem, "uploadedDate" | "deadlineHeight"> & {
    owner: Base64UrlString;
    target: string | undefined;
    tags: Tag[];
  };
  paidBy?: NativeAddress[];
  paymentService: PaymentService;
  database: Database;
  arweaveGateway: ArweaveGateway;
  getArweaveWallet: () => Promise<JWKInterface>;
  logger: winston.Logger;
}): Promise<MultiPartUploadResponse> {
  // At the point, the DB has finalized the in flight upload, the validated data item is in
  // the raw data item bucket, and we now need to reserve balance at payment svc, optical post,
  // construct the receipt, insert the data item into the database, and return the receipt.
  let fnLogger = logger.child({ paidBy });

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

  const nativeAddress = ownerToNativeAddress(
    dataItemInfo.owner,
    dataItemInfo.signatureType
  );

  fnLogger.debug("Reserving balance for upload...");
  const paymentResponse: ReserveBalanceResponse = shouldSkipBalanceCheck
    ? { isReserved: true, costOfDataItem: W("0"), walletExists: true }
    : await paymentService.reserveBalanceForData({
        nativeAddress,
        size: dataItemInfo.byteCount,
        dataItemId: dataItemInfo.dataItemId,
        signatureType: dataItemInfo.signatureType,
        paidBy,
      });
  fnLogger = fnLogger.child({
    paymentResponse,
    byteCount: dataItemInfo.byteCount,
    ownerAddress: dataItemInfo.ownerPublicAddress,
  });
  fnLogger.debug("Finished reserving balance for upload.");

  const performQuarantine = () => {
    // don't need to await this - just invoke and move on
    void quarantineDataItem({
      objectStore,
      cacheService: getElasticacheService(), // TODO: Actually integrate with Elasticache effectively in this file
      dataItemId: dataItemInfo.dataItemId,
      database,
      logger: fnLogger,
      contentLength: dataItemInfo.byteCount,
      payloadInfo: {
        payloadContentType: dataItemInfo.payloadContentType,
        payloadDataStart: dataItemInfo.payloadDataStart,
      },
    });
  };

  if (paymentResponse.isReserved) {
    dataItemInfo.assessedWinstonPrice = paymentResponse.costOfDataItem;
    fnLogger.debug("Balance successfully reserved", {
      assessedWinstonPrice: paymentResponse.costOfDataItem,
    });
  } else {
    fnLogger.error(`Failing multipart upload due to insufficient balance.`);
    performQuarantine();
    await database.failFinishedMultiPartUpload({
      uploadId,
      failedReason: "UNDERFUNDED",
    });
    inFlightUploadCache.remove(uploadId);
    throw new InsufficientBalance();
  }

  // admin action tags
  const approvedAddress = dataItemInfo.tags.find(
    (tag) => tag.name === createDelegatedPaymentApprovalTagName
  )?.value;
  const winc = dataItemInfo.tags.find(
    (tag) => tag.name === approvalAmountTagName
  )?.value;
  if (approvedAddress && winc) {
    const expiresInSeconds = dataItemInfo.tags.find(
      (tag) => tag.name === approvalExpiresBySecondsTagName
    )?.value;

    try {
      await paymentService.createDelegatedPaymentApproval({
        approvedAddress,
        payingAddress: nativeAddress,
        dataItemId: dataItemInfo.dataItemId,
        winc,
        expiresInSeconds,
      });
    } catch (error) {
      const message = `Unable to create delegated payment approval ${
        error instanceof PaymentServiceReturnedError
          ? `: ${error.message}`
          : "!"
      }`;
      if (paymentResponse.costOfDataItem.isGreaterThan(W(0))) {
        await paymentService.refundBalanceForData({
          dataItemId: dataItemInfo.dataItemId,
          nativeAddress,
          signatureType: dataItemInfo.signatureType,
          winston: paymentResponse.costOfDataItem,
        });
      }
      await database.failFinishedMultiPartUpload({
        uploadId,
        failedReason: "APPROVAL_FAILED",
      });

      throw new Error(message);
    }
  }
  const revokedAddress = dataItemInfo.tags.find(
    (tag) => tag.name === revokeDelegatePaymentApprovalTagName
  )?.value;
  if (revokedAddress) {
    try {
      await paymentService.revokeDelegatedPaymentApprovals({
        revokedAddress,
        payingAddress: nativeAddress,
        dataItemId: dataItemInfo.dataItemId,
      });
    } catch (error) {
      const message = `Unable to revoke delegated payment approvals ${
        error instanceof PaymentServiceReturnedError
          ? `: ${error.message}`
          : "!"
      }`;
      if (paymentResponse.costOfDataItem.isGreaterThan(W(0))) {
        await paymentService.refundBalanceForData({
          dataItemId: dataItemInfo.dataItemId,
          nativeAddress,
          signatureType: dataItemInfo.signatureType,
          winston: paymentResponse.costOfDataItem,
        });
      }

      await database.failFinishedMultiPartUpload({
        uploadId,
        failedReason: "REVOKE_FAILED",
      });

      throw new Error(message);
    }
  }

  const shouldSkipOpticalPost = skipOpticalPostAddresses.includes(
    dataItemInfo.ownerPublicAddress
  );
  if (opticalBridgingEnabled && !shouldSkipOpticalPost) {
    fnLogger.debug("Asynchronously optical posting...");
    try {
      const signedDataItemHeader = await signDataItemHeader(
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
      );
      void enqueue(jobLabels.opticalPost, {
        ...signedDataItemHeader,
        uploaded_at: uploadTimestamp,
      }).catch((error) => {
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

  // Enqueue data item for unbundling if it appears to be a BDI
  if (containsAns104Tags(dataItemInfo.tags)) {
    try {
      logger.debug("Enqueuing BDI for unbundling...");
      await enqueue(jobLabels.unbundleBdi, {
        id: dataItemInfo.dataItemId,
        uploaded_at: uploadTimestamp,
      });
    } catch (bdiEnqueueError) {
      // Soft error, just log
      logger.error(
        `Error while attempting to enqueue for bdi unbundling!`,
        bdiEnqueueError
      );
      MetricRegistry.unbundleBdiEnqueueFail.inc();
    }
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
      performQuarantine();
    }
    throw error;
  }

  return {
    ...signedReceipt,
    dataCaches,
    fastFinalityIndexes: shouldSkipBalanceCheck ? [] : fastFinalityIndexes,
    owner: dataItemInfo.ownerPublicAddress,
    winc: paymentResponse.costOfDataItem.toString(),
  };
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
