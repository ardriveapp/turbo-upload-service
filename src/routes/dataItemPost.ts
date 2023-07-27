/**
 * Copyright (C) 2022-2023 Permanent Data Solutions, Inc. All Rights Reserved.
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
import { Readable } from "stream";
import winston from "winston";

import { ReserveBalanceResponse } from "../arch/payment";
import { enqueue } from "../arch/queues";
import { StreamingDataItem } from "../bundles/streamingDataItem";
import { signatureTypeInfo } from "../bundles/verifyDataItem";
import {
  anchorLength,
  emptyAnchorLength,
  emptyTargetLength,
  maxDataItemSize,
  octetStreamContentType,
  signatureTypeLength,
  targetLength,
} from "../constants";
import { MetricRegistry } from "../metricRegistry";
import { KoaContext } from "../server";
import { TransactionId } from "../types/types";
import { Winston } from "../types/winston";
import { errorResponse, tapStream } from "../utils/common";
import { DataItemExistsWarning } from "../utils/errors";
import {
  putDataItemData,
  putDataItemRaw,
  removeDataItem,
} from "../utils/objectStoreUtils";
import {
  containsAns104Tags,
  encodeTagsForOptical,
  signDataItemHeader,
} from "../utils/opticalUtils";

const dataItemIdCache: Set<TransactionId> = new Set();
const addToDataItemCache = (dataItemId: TransactionId) =>
  dataItemIdCache.add(dataItemId);
const removeFromDataItemCache = (dataItemId: TransactionId) =>
  dataItemIdCache.delete(dataItemId);

export async function dataItemRoute(ctx: KoaContext, next: Next) {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let { logger }: { logger: winston.Logger } = ctx.state as any;
  const durations = {
    totalDuration: 0,
    cacheDuration: 0,
    extractDuration: 0,
    dbInsertDuration: 0,
  };

  const requestStartTime = Date.now();
  logger.info("New Data Item posting...");

  const { objectStore, database, paymentService } = ctx.state;

  // Validate the content-length header
  const contentLength = ctx.req.headers?.["content-length"];
  logger.info("request content length: ", { contentLength });
  if (contentLength === undefined) {
    return errorResponse(ctx, {
      errorMessage: "Request content has no length",
    });
  } else if (+contentLength > maxDataItemSize) {
    return errorResponse(ctx, {
      errorMessage: `Data item is too large, this service only accepts data items up to ${maxDataItemSize} bytes!`,
    });
  }

  // Inspect, but do not validate, the content-type header
  const requestContentType = ctx.req.headers?.["content-type"];
  if (!requestContentType) {
    logger.warn("Missing request content type!");
  } else if (requestContentType !== octetStreamContentType) {
    logger.warn(
      `Request content type is unexpected... Rejecting this request with a 400 status!`,
      { requestContentType, contentLength }
    );
    errorResponse(ctx, {
      errorMessage: "Invalid Content Type",
    });

    return next();
  }

  // Duplicate the request body stream. The original will go to the data item
  // event emitter. This one will go to the object store.
  const rawDataItemStream = tapStream({
    readable: ctx.request.req,
    logger: logger.child({ context: "rawDataItemStream" }),
  });

  // Create a streaming data item with the request body
  const streamingDataItem = new StreamingDataItem(ctx.request.req, logger);

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
    logger = logger.child({ signatureType, ownerPublicAddress, dataItemId });
  } catch (error) {
    errorResponse(ctx, {
      errorMessage: "Data item parsing error!",
      error,
    });

    return next();
  }

  logger.info("Retrieved data item signature type, owner, and ID.");

  // Catch duplicate data item attacks via in memory cache (for single instance of service)
  if (dataItemIdCache.has(dataItemId)) {
    logger.info("Data item already uploaded to this service instance.");
    ctx.status = 202;
    ctx.res.statusMessage = "Data Item Exists";

    return next();
  }
  addToDataItemCache(dataItemId);

  let paymentResponse: ReserveBalanceResponse;
  try {
    logger.info("Reserving balance for upload...");
    paymentResponse = await paymentService.reserveBalanceForData({
      ownerPublicAddress,
      size: +contentLength,
      dataItemId,
    });
  } catch (error) {
    errorResponse(ctx, {
      status: 503,
      errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Payment Service is unreachable`,
    });
    removeFromDataItemCache(dataItemId);
    return next();
  }

  let assessedWinstonPrice: Winston;

  if (paymentResponse.isReserved) {
    assessedWinstonPrice = paymentResponse.costOfDataItem;
    logger.info("Balance successfully reserved", { assessedWinstonPrice });
  } else {
    if (!paymentResponse.walletExists) {
      logger.info("Wallet does not exist.");
    }

    errorResponse(ctx, {
      status: 401,
      errorMessage: "Insufficient balance",
    });

    removeFromDataItemCache(dataItemId);
    return next();
  }

  // Parse out the content type and the payload stream
  let contentType: string;
  let dataStart: number;
  let anchor: string | undefined;
  let target: string | undefined;
  let tags: Tag[];
  let payloadStream: Readable;
  try {
    // Log some useful debugging info
    anchor = await streamingDataItem.getAnchor();
    target = await streamingDataItem.getTarget();
    logger.info(`Target and anchor parsed:`, {
      anchor,
      target,
    });

    const numTags = await streamingDataItem.getNumTags();
    logger.info("Parsed tag count", {
      numTags,
    });

    const numTagsBytes = await streamingDataItem.getNumTagsBytes();
    tags = await streamingDataItem.getTags();

    logger.info(`Tags parsed:`, {
      tags,
      numTagsBytes,
      dataItemId,
    });

    contentType =
      tags.filter((tag) => tag.name.toLowerCase() === "content-type").shift()
        ?.value || octetStreamContentType;

    logger.info(`Awaiting a payload stream for caching...`, {
      contentType,
    });

    const tagsStart =
      signatureTypeLength +
      signatureTypeInfo[signatureType].signatureLength +
      signatureTypeInfo[signatureType].pubkeyLength +
      (target === undefined ? emptyTargetLength : targetLength) +
      (anchor === undefined ? emptyAnchorLength : anchorLength);
    dataStart = tagsStart + 16 + numTagsBytes;

    payloadStream = await streamingDataItem.getPayloadStream();
  } catch (error) {
    errorResponse(ctx, {
      errorMessage: "Data item parsing error!",
      error,
    });
    await paymentService.refundBalanceForData({
      ownerPublicAddress,
      winston: paymentResponse.costOfDataItem,
      dataItemId,
    });

    removeFromDataItemCache(dataItemId);
    return next();
  }

  logger.info(`Tapping payload stream for caching...`);

  const extractedDataItemStream = tapStream({
    readable: payloadStream,
    logger: logger.child({ context: "extractedDataItemStream" }),
  });

  // Cache the raw and extracted data item streams
  const objectStoreCacheStart = Date.now();
  try {
    await Promise.allSettled([
      putDataItemRaw(objectStore, dataItemId, rawDataItemStream).then(() => {
        durations.cacheDuration = Date.now() - objectStoreCacheStart;
        logger.info(`Cache full item duration: ${durations.cacheDuration}ms`);
      }),
      putDataItemData(
        objectStore,
        dataItemId,
        contentType,
        extractedDataItemStream
      ).then(() => {
        durations.extractDuration = Date.now() - objectStoreCacheStart;
        logger.info(
          `Cache extracted item duration: ${durations.extractDuration}ms`
        );
      }),
    ]).then((results) => {
      for (const result of results) {
        if (result.status === "rejected") {
          throw result.reason;
        }
      }
      logger.info(
        `Finished uploading raw and extracted data item to object stores!`
      );
    });
  } catch (error) {
    errorResponse(ctx, {
      status: 503,
      errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Object Store is unreachable`,
      error,
    });

    await paymentService.refundBalanceForData({
      ownerPublicAddress,
      winston: assessedWinstonPrice,
      dataItemId,
    });

    logger.info(`Balance refunded due to caching error.`, {
      assessedWinstonPrice,
    });

    removeFromDataItemCache(dataItemId);
    return next();
  }

  logger.info(`Assessing data item validity...`);
  let isValid: boolean;
  try {
    isValid = await streamingDataItem.isValid();
  } catch (error) {
    errorResponse(ctx, {
      errorMessage: "Data item parsing error!",
      error,
    });
    await paymentService.refundBalanceForData({
      ownerPublicAddress,
      winston: paymentResponse.costOfDataItem,
      dataItemId,
    });
    logger.info(`Balance refunded due to parsing error.`, {
      assessedWinstonPrice,
    });
    removeFromDataItemCache(dataItemId);
    removeDataItem(objectStore, dataItemId); // no need to await - just invoke and forget
    return next();
  }
  logger.info(`Got data item validity.`, { isValid });
  if (!isValid) {
    errorResponse(ctx, {
      errorMessage: "Invalid Data Item!",
    });
    await paymentService.refundBalanceForData({
      ownerPublicAddress,
      winston: paymentResponse.costOfDataItem,
      dataItemId,
    });
    logger.info(`Balance refunded due to invalid data item.`, {
      assessedWinstonPrice,
    });
    removeFromDataItemCache(dataItemId);
    return removeDataItem(objectStore, dataItemId);
  }

  const dbInsertStart = Date.now();
  try {
    await database.insertNewDataItem({
      dataItemId,
      ownerPublicAddress: ownerPublicAddress,
      assessedWinstonPrice,
      byteCount: +contentLength,
      dataStart,
      signatureType,
      failedBundles: [],
    });
    durations.dbInsertDuration = Date.now() - dbInsertStart;
    logger.info(`DB insert duration: ${durations.dbInsertDuration}ms`);
  } catch (error) {
    if (error instanceof DataItemExistsWarning) {
      logger.info(`DB insert exists duration: ${Date.now() - dbInsertStart}ms`);
      ctx.status = 202;
      const message = (error as DataItemExistsWarning).message;
      logger.warn(message);
      ctx.res.statusMessage = message;
      return next();
    } else {
      logger.info(`DB insert failed duration: ${Date.now() - dbInsertStart}ms`);
      errorResponse(ctx, {
        status: 503,
        errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Cloud Database is unreachable`,
        error,
      });
      await paymentService.refundBalanceForData({
        ownerPublicAddress,
        winston: paymentResponse.costOfDataItem,
        dataItemId,
      });
      logger.info(`Balance refunded due to database error.`, {
        assessedWinstonPrice,
      });
      removeFromDataItemCache(dataItemId);
      removeDataItem(objectStore, dataItemId); // don't need to await this - just invoke and move on
      return next();
    }
  }

  // Enqueue data item for optical bridging
  // NOTE: Safe to get payload size now that payload has been fully consumed
  const data_size = await streamingDataItem.getPayloadSize();
  const confirmedFeatures: Record<string, string[]> = {
    dataCaches: ["arweave.net"],
  };
  try {
    logger.info("Enqueuing data item to optical...");
    await enqueue(
      "optical-post",
      await signDataItemHeader(
        encodeTagsForOptical({
          id: dataItemId,
          signature,
          owner,
          owner_address: ownerPublicAddress,
          target: target ?? "",
          content_type: contentType,
          data_size,
          tags,
        })
      )
    );
  } catch (opticalError) {
    // Soft error, just log
    logger.error(
      `Error while attempting to enqueue for optical bridging!`,
      opticalError
    );
    MetricRegistry.opticalBridgeEnqueueFail.inc();
  }

  // Enqueue data item for unbundling if it appears to be a BDI
  if (containsAns104Tags(tags))
    try {
      logger.info("Enqueuing BDI for unbundling...");
      await enqueue("unbundle-bdi", dataItemId);
    } catch (bdiEnqueueError) {
      // Soft error, just log
      logger.error(
        `Error while attempting to enqueue for bdi unbundling!`,
        bdiEnqueueError
      );
      MetricRegistry.unbundleBdiEnqueueFail.inc();
    }

  ctx.status = 200;
  ctx.body = {
    id: dataItemId,
    owner: ownerPublicAddress,
    ...confirmedFeatures,
  };

  removeFromDataItemCache(dataItemId);

  durations.totalDuration = Date.now() - requestStartTime;

  logger.info(`Total request duration: ${durations.totalDuration}ms`);
  logger.info(`Durations (ms):`, durations);

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
    logger.info(`Duration proportions:`, proportionalDurations);

    const toMiBPerSec = 1000 / 1048576;
    const throughputs = {
      totalThroughput: (+contentLength / durations.totalDuration) * toMiBPerSec,
      cacheThroughput: (+contentLength / durations.cacheDuration) * toMiBPerSec,
      extractThroughput:
        (+contentLength / durations.extractDuration) * toMiBPerSec,
    };
    logger.info(`Throughputs (MiB/sec):`, throughputs);
  }

  return next();
}
