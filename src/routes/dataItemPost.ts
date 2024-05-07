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
import { Tag } from "arbundles";
import { Next } from "koa";

import { CheckBalanceResponse, ReserveBalanceResponse } from "../arch/payment";
import { enqueue } from "../arch/queues";
import { StreamingDataItem } from "../bundles/streamingDataItem";
import { signatureTypeInfo } from "../bundles/verifyDataItem";
import {
  anchorLength,
  blocklistedAddresses,
  dataCaches,
  deadlineHeightIncrement,
  emptyAnchorLength,
  emptyTargetLength,
  fastFinalityIndexes,
  maxSingleDataItemByteCount,
  octetStreamContentType,
  receiptVersion,
  signatureTypeLength,
  skipOpticalPostAddresses,
  targetLength,
} from "../constants";
import { MetricRegistry } from "../metricRegistry";
import { KoaContext } from "../server";
import { TransactionId } from "../types/types";
import { W } from "../types/winston";
import {
  errorResponse,
  filterKeysFromObject,
  getPremiumFeatureType,
  payloadContentTypeFromDecodedTags,
  sleep,
  tapStream,
} from "../utils/common";
import { DataItemExistsWarning } from "../utils/errors";
import {
  putDataItemRaw,
  rawDataItemExists,
  removeDataItem,
} from "../utils/objectStoreUtils";
import {
  containsAns104Tags,
  encodeTagsForOptical,
  signDataItemHeader,
} from "../utils/opticalUtils";
import { ownerToNativeAddress } from "../utils/ownerToNativeAddress";
import {
  SignedReceipt,
  UnsignedReceipt,
  signReceipt,
} from "../utils/signReceipt";

const shouldSkipBalanceCheck = process.env.SKIP_BALANCE_CHECKS === "true";
const opticalBridgingEnabled = process.env.OPTICAL_BRIDGING_ENABLED !== "false";

const dataItemIdCache: Set<TransactionId> = new Set();
const addToDataItemCache = (dataItemId: TransactionId) =>
  dataItemIdCache.add(dataItemId);
const removeFromDataItemCache = (dataItemId: TransactionId) =>
  dataItemIdCache.delete(dataItemId);

export async function dataItemRoute(ctx: KoaContext, next: Next) {
  let { logger } = ctx.state;
  const durations = {
    totalDuration: 0,
    cacheDuration: 0,
    extractDuration: 0,
    dbInsertDuration: 0,
  };

  const requestStartTime = Date.now();
  const { objectStore, paymentService, arweaveGateway, getArweaveWallet } =
    ctx.state;

  // Validate the content-length header
  const contentLength = ctx.req.headers?.["content-length"];
  if (contentLength === undefined) {
    logger.debug("Request has no content length header!");
  } else if (+contentLength > maxSingleDataItemByteCount) {
    return errorResponse(ctx, {
      errorMessage: `Data item is too large, this service only accepts data items up to ${maxSingleDataItemByteCount} bytes!`,
    });
  }

  // Inspect, but do not validate, the content-type header
  const requestContentType = ctx.req.headers?.["content-type"];
  if (!requestContentType) {
    logger.debug("Missing request content type!");
  } else if (requestContentType !== octetStreamContentType) {
    errorResponse(ctx, {
      errorMessage: "Invalid Content Type",
    });

    return next();
  }

  // Duplicate the request body stream. The original will go to the data item
  // event emitter. This one will go to the object store.
  ctx.request.req.pause();
  const rawDataItemStream = tapStream({
    readable: ctx.request.req,
    logger: logger.child({ context: "rawDataItemStream" }),
  });

  // Create a streaming data item with the request body
  const streamingDataItem = new StreamingDataItem(ctx.request.req, logger);
  ctx.request.req.resume();

  // Assess a Winston price and/or whitelist-status for this upload once
  // enough data item info has streamed to the data item event emitter
  let signatureType: number;
  let signature: string;
  let owner: string;
  let ownerPublicAddress: string;
  let dataItemId: string;

  try {
    signatureType = await streamingDataItem.getSignatureType();
    signature = await streamingDataItem.getSignature();
    owner = await streamingDataItem.getOwner();
    ownerPublicAddress = await streamingDataItem.getOwnerAddress();

    dataItemId = await streamingDataItem.getDataItemId();
    // signature and owner will be too noisy in the logs and the latter hashes down to ownerPublicAddress
    logger = logger.child({
      signatureType,
      ownerPublicAddress,
      dataItemId,
    });
  } catch (error) {
    errorResponse(ctx, {
      errorMessage: "Data item parsing error!",
      error,
    });

    return next();
  }

  const nativeAddress = ownerToNativeAddress(owner, signatureType);
  logger = logger.child({ nativeAddress });

  // Catch duplicate data item attacks via in memory cache (for single instance of service)
  if (dataItemIdCache.has(dataItemId)) {
    // create the error for consistent responses
    const error = new DataItemExistsWarning(dataItemId);
    logger.debug("Data item already uploaded to this service instance.");
    ctx.status = 202;
    ctx.res.statusMessage = error.message;
    return next();
  }
  addToDataItemCache(dataItemId);

  // Reserve balance for this upload if the content-length header was present
  if (shouldSkipBalanceCheck) {
    logger.debug("Skipping balance check...");
  } else if (contentLength !== undefined) {
    let checkBalanceResponse: CheckBalanceResponse;
    try {
      logger.debug("Checking balance for upload...");
      checkBalanceResponse = await paymentService.checkBalanceForData({
        nativeAddress,
        size: +contentLength,
        signatureType,
      });
    } catch (error) {
      errorResponse(ctx, {
        status: 503,
        errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Payment Service is unreachable`,
      });
      removeFromDataItemCache(dataItemId);
      return next();
    }

    if (checkBalanceResponse.userHasSufficientBalance) {
      logger.debug("User can afford bytes", checkBalanceResponse);
    } else {
      errorResponse(ctx, {
        status: 402,
        errorMessage: "Insufficient balance",
      });

      removeFromDataItemCache(dataItemId);
      return next();
    }
  }

  // Parse out the content type and the payload stream
  let payloadContentType: string;
  let payloadDataStart: number;
  let anchor: string | undefined;
  let target: string | undefined;
  let tags: Tag[];
  try {
    // Log some useful debugging info
    anchor = await streamingDataItem.getAnchor();
    target = await streamingDataItem.getTarget();
    const numTags = await streamingDataItem.getNumTags();
    const numTagsBytes = await streamingDataItem.getNumTagsBytes();
    tags = await streamingDataItem.getTags();
    payloadContentType = payloadContentTypeFromDecodedTags(tags);

    // Log tags and other useful info for log parsing
    logger = logger.child({
      payloadContentType,
      numTags,
      tags,
    });
    logger.debug(`Data Item parsed, awaiting payload stream...`, {
      numTagsBytes,
      anchor,
      target,
    });

    const tagsStart =
      signatureTypeLength +
      signatureTypeInfo[signatureType].signatureLength +
      signatureTypeInfo[signatureType].pubkeyLength +
      (target === undefined ? emptyTargetLength : targetLength) +
      (anchor === undefined ? emptyAnchorLength : anchorLength);
    payloadDataStart = tagsStart + 16 + numTagsBytes;
  } catch (error) {
    errorResponse(ctx, {
      errorMessage: "Data item parsing error!",
      error,
    });

    removeFromDataItemCache(dataItemId);
    return next();
  }

  // Cache the raw and extracted data item streams
  const objectStoreCacheStart = Date.now();
  try {
    await Promise.allSettled([
      putDataItemRaw(
        objectStore,
        dataItemId,
        rawDataItemStream,
        payloadContentType,
        payloadDataStart
      ).then(() => {
        durations.cacheDuration = Date.now() - objectStoreCacheStart;
        logger.debug(`Cache full item duration: ${durations.cacheDuration}ms`);
      }),
      (async () => {
        logger.debug(`Consuming payload stream...`);
        await streamingDataItem.isValid();
        logger.debug(`Payload stream consumed.`);
      })(),
    ]).then((results) => {
      for (const result of results) {
        if (result.status === "rejected") {
          throw result.reason;
        }
      }
      logger.debug(
        `Finished uploading raw and extracted data item to object stores!`
      );
    });
  } catch (error) {
    errorResponse(ctx, {
      status: 503,
      errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Object Store is unreachable`,
      error,
    });

    removeFromDataItemCache(dataItemId);
    return next();
  }

  logger.debug(`Assessing data item validity...`);
  let isValid: boolean;
  try {
    isValid = await streamingDataItem.isValid();
  } catch (error) {
    errorResponse(ctx, {
      errorMessage: "Data item parsing error!",
      error,
    });

    removeFromDataItemCache(dataItemId);
    void removeDataItem(objectStore, dataItemId); // no need to await - just invoke and forget
    return next();
  }
  logger.debug(`Got data item validity.`, { isValid });
  if (!isValid) {
    errorResponse(ctx, {
      errorMessage: "Invalid Data Item!",
    });

    removeFromDataItemCache(dataItemId);
    return removeDataItem(objectStore, dataItemId);
  }

  // NOTE: Safe to get payload size now that payload has been fully consumed
  const payloadDataByteCount = await streamingDataItem.getPayloadSize();
  const totalSize = payloadDataByteCount + payloadDataStart;

  if (totalSize > maxSingleDataItemByteCount) {
    errorResponse(ctx, {
      errorMessage: `Data item is too large, this service only accepts data items up to ${maxSingleDataItemByteCount} bytes!`,
    });
    removeFromDataItemCache(dataItemId);
    void removeDataItem(objectStore, dataItemId);
    return next();
  }

  if (blocklistedAddresses.includes(ownerPublicAddress)) {
    logger.info(
      "The owner's address is on the arweave public address block list. Rejecting data item..."
    );
    errorResponse(ctx, {
      status: 403,
      errorMessage: "Forbidden",
    });

    removeFromDataItemCache(dataItemId);
    void removeDataItem(objectStore, dataItemId); // don't need to await this - just invoke and move on
    return next();
  }

  // Reserve balance for this upload if the content-length header was not present
  let paymentResponse: ReserveBalanceResponse;
  if (shouldSkipBalanceCheck) {
    logger.debug("Skipping balance check...");
    paymentResponse = {
      isReserved: true,
      costOfDataItem: W(0),
      walletExists: true,
    };
  } else {
    try {
      logger.debug("Reserving balance for upload...");
      paymentResponse = await paymentService.reserveBalanceForData({
        nativeAddress,
        size: totalSize,
        dataItemId,
        signatureType,
      });
      logger = logger.child({ paymentResponse });
    } catch (error) {
      errorResponse(ctx, {
        status: 503,
        errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Payment Service is unreachable`,
      });
      removeFromDataItemCache(dataItemId);
      return next();
    }

    if (paymentResponse.isReserved) {
      logger.debug("Balance successfully reserved", {
        assessedWinstonPrice: paymentResponse.costOfDataItem,
      });
    } else {
      if (!paymentResponse.walletExists) {
        logger.debug("Wallet does not exist.");
      }

      errorResponse(ctx, {
        status: 402,
        errorMessage: "Insufficient balance",
      });

      removeFromDataItemCache(dataItemId);
      return next();
    }
  }

  // Enqueue data item for optical bridging
  const confirmedFeatures: {
    dataCaches: string[];
    fastFinalityIndexes: string[];
  } = {
    dataCaches,
    fastFinalityIndexes: [],
  };
  try {
    if (
      opticalBridgingEnabled &&
      !skipOpticalPostAddresses.includes(ownerPublicAddress)
    ) {
      logger.debug("Enqueuing data item to optical...");
      await enqueue(
        "optical-post",
        await signDataItemHeader(
          encodeTagsForOptical({
            id: dataItemId,
            signature,
            owner,
            owner_address: ownerPublicAddress,
            target: target ?? "",
            content_type: payloadContentType,
            data_size: payloadDataByteCount,
            tags,
          })
        )
      );
      confirmedFeatures.fastFinalityIndexes = fastFinalityIndexes;
    } else {
      // Attach skip feature to logger for log parsing in final receipt log statement
      logger = logger.child({ skipOpticalPost: true });
    }
  } catch (opticalError) {
    // Soft error, just log
    logger.error(
      `Error while attempting to enqueue for optical bridging!`,
      opticalError
    );
    MetricRegistry.opticalBridgeEnqueueFail.inc();
  }

  // Enqueue data item for unbundling if it appears to be a BDI
  if (containsAns104Tags(tags)) {
    try {
      logger.debug("Enqueuing BDI for unbundling...");
      await enqueue("unbundle-bdi", dataItemId);
    } catch (bdiEnqueueError) {
      // Soft error, just log
      logger.error(
        `Error while attempting to enqueue for bdi unbundling!`,
        bdiEnqueueError
      );
      MetricRegistry.unbundleBdiEnqueueFail.inc();
    }
  }

  let uploadTimestamp: number;
  let signedReceipt: SignedReceipt;
  let deadlineHeight: number;
  try {
    // do a head check in s3 before we sign the receipt
    if (!(await rawDataItemExists(objectStore, dataItemId))) {
      throw new Error(`Data item failed head check to object store.`);
    }
    const currentBlockHeight = await arweaveGateway.getCurrentBlockHeight();
    const jwk = await getArweaveWallet();

    deadlineHeight = currentBlockHeight + deadlineHeightIncrement;
    uploadTimestamp = Date.now();
    const receipt: UnsignedReceipt = {
      id: dataItemId,
      timestamp: uploadTimestamp,
      winc: paymentResponse.costOfDataItem.toString(),
      version: receiptVersion,
      deadlineHeight,
      ...confirmedFeatures,
    };
    signedReceipt = await signReceipt(receipt, jwk);
    // Log the signed receipt for log parsing
    logger.info(
      "Receipt signed!",
      filterKeysFromObject(signedReceipt, ["public"])
    );
  } catch (error) {
    errorResponse(ctx, {
      status: 503,
      errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Unable to sign receipt...`,
      error,
    });
    if (paymentResponse.costOfDataItem.isGreaterThan(W(0))) {
      await paymentService.refundBalanceForData({
        signatureType,
        nativeAddress,
        winston: paymentResponse.costOfDataItem,
        dataItemId,
      });
      logger.warn(`Balance refunded due to signed receipt error.`, {
        assessedWinstonPrice: paymentResponse.costOfDataItem,
      });
    }
    removeFromDataItemCache(dataItemId);
    void removeDataItem(objectStore, dataItemId); // don't need to await this - just invoke and move on
    return next();
  }

  const premiumFeatureType = getPremiumFeatureType(ownerPublicAddress, tags);

  const dbInsertStart = Date.now();
  try {
    await enqueue("new-data-item", {
      dataItemId,
      ownerPublicAddress,
      assessedWinstonPrice: paymentResponse.costOfDataItem,
      byteCount: totalSize,
      payloadDataStart,
      signatureType,
      failedBundles: [],
      uploadedDate: new Date(uploadTimestamp).toISOString(),
      payloadContentType,
      premiumFeatureType,
      signature,
      deadlineHeight,
    });

    // Anticipate 20ms of replication delay. Modicum of protection against caller checking status immediately after returning
    await sleep(20);

    durations.dbInsertDuration = Date.now() - dbInsertStart;
    logger.debug(`DB insert duration: ${durations.dbInsertDuration}ms`);
  } catch (error) {
    logger.debug(`DB insert failed duration: ${Date.now() - dbInsertStart}ms`);
    errorResponse(ctx, {
      status: 503,
      errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable.`,
      error,
    });
    if (paymentResponse.costOfDataItem.isGreaterThan(W(0))) {
      await paymentService.refundBalanceForData({
        nativeAddress,
        winston: paymentResponse.costOfDataItem,
        dataItemId,
        signatureType: signatureType,
      });
      logger.warn(`Balance refunded due to database error.`, {
        assessedWinstonPrice: paymentResponse.costOfDataItem,
      });
    }
    // always remove from instance cache
    removeFromDataItemCache(dataItemId);
    await removeDataItem(objectStore, dataItemId);
    return next();
  }

  ctx.status = 200;

  ctx.body = { ...signedReceipt, owner: ownerPublicAddress };

  removeFromDataItemCache(dataItemId);

  durations.totalDuration = Date.now() - requestStartTime;
  // TODO: our logger middleware now captures total request time, so these can logs can be removed if they are not being used for any reporting/alerting
  logger.debug(`Total request duration: ${durations.totalDuration}ms`);
  logger.debug(`Durations (ms):`, durations);

  // Avoid DIV0
  if (durations.totalDuration > 0) {
    // Compute what proportion of total request time each step took
    const proportionalDurations = Object.entries(durations).reduce(
      (acc, [key, duration]) => {
        acc[key + "Pct"] = duration / durations.totalDuration;
        return acc;
      },
      {} as Record<string, number>
    );
    logger.debug(`Duration proportions:`, proportionalDurations);

    const toMiBPerSec = 1000 / 1048576;
    const throughputs = {
      totalThroughput: (totalSize / durations.totalDuration) * toMiBPerSec,
      cacheThroughput: (totalSize / durations.cacheDuration) * toMiBPerSec,
      extractThroughput: (totalSize / durations.extractDuration) * toMiBPerSec,
    };
    logger.debug(`Throughputs (MiB/sec):`, throughputs);
  }

  return next();
}
