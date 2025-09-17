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
import { Tag, processStream } from "@dha-team/arbundles";
import { Next } from "koa";

import {
  CheckBalanceResponse,
  DelegatedPaymentApproval,
  ReserveBalanceResponse,
} from "../arch/payment";
import { enqueue } from "../arch/queues";
import {
  DataItemInterface,
  InMemoryDataItem,
  StreamingDataItem,
} from "../bundles/streamingDataItem";
import { signatureTypeInfo } from "../constants";
import {
  anchorLength,
  approvalAmountTagName,
  approvalExpiresBySecondsTagName,
  blocklistedAddresses,
  createDelegatedPaymentApprovalTagName,
  dataCaches,
  deadlineHeightIncrement,
  emptyAnchorLength,
  emptyTargetLength,
  fastFinalityIndexes,
  jobLabels,
  maxSingleDataItemByteCount,
  octetStreamContentType,
  receiptVersion,
  revokeDelegatePaymentApprovalTagName,
  signatureTypeLength,
  skipOpticalPostAddresses,
  targetLength,
} from "../constants";
import globalLogger from "../logger";
import { MetricRegistry } from "../metricRegistry";
import { KoaContext } from "../server";
import { ParsedDataItemHeader, SignatureConfig } from "../types/types";
import { W } from "../types/winston";
import {
  errorResponse,
  filterKeysFromObject,
  getPremiumFeatureType,
  payloadContentTypeFromDecodedTags,
  sleep,
} from "../utils/common";
import {
  ValidDataItemStore,
  allValidDataItemStores,
  cacheDataItem,
  dataItemExists,
  quarantineDataItem,
  streamsForDataItemStorage,
} from "../utils/dataItemUtils";
import {
  DataItemExistsWarning,
  InsufficientBalance,
  PaymentServiceReturnedError,
} from "../utils/errors";
import {
  UPLOAD_DATA_PATH,
  ensureDataItemsBackupDirExists,
} from "../utils/fileSystemUtils";
import {
  dataItemIsInFlight,
  markInFlight,
  removeFromInFlight,
} from "../utils/inFlightDataItemCache";
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
import { streamToBuffer } from "../utils/streamToBuffer";

const shouldSkipBalanceCheck = process.env.SKIP_BALANCE_CHECKS === "true";
const opticalBridgingEnabled = process.env.OPTICAL_BRIDGING_ENABLED !== "false";
ensureDataItemsBackupDirExists().catch((error) => {
  globalLogger.error(
    `Failed to create upload data directory at ${UPLOAD_DATA_PATH}!`,
    { error }
  );
  throw error;
});

export const inMemoryDataItemThreshold = 10 * 1024; // 10 KiB

export async function dataItemRoute(ctx: KoaContext, next: Next) {
  let { logger } = ctx.state;
  const durations = {
    totalDuration: 0,
    cacheDuration: 0,
    extractDuration: 0,
    dbInsertDuration: 0,
  };

  const token = ctx.params.token;
  let signatureTypeOverride: number | undefined;
  if (token === "kyve") {
    signatureTypeOverride = SignatureConfig.KYVE;
  }

  const requestStartTime = Date.now();
  const {
    objectStore,
    cacheService,
    paymentService,
    arweaveGateway,
    getArweaveWallet,
    database,
  } = ctx.state;

  // Validate the content-length header
  const contentLengthStr = ctx.req.headers?.["content-length"];
  const rawContentLength = contentLengthStr ? +contentLengthStr : undefined;
  if (rawContentLength === undefined) {
    logger.debug("Request has no content length header!");
  } else if (rawContentLength > maxSingleDataItemByteCount) {
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

  // Duplicate the request body stream. The original will go to the data item
  // event emitter. This one will go to the object store.
  ctx.request.req.pause();

  const { cacheServiceStream, fsBackupStream, objStoreStream, dynamoStream } =
    await streamsForDataItemStorage({
      inputStream: ctx.request.req,
      contentLength: rawContentLength,
      logger,
      cacheService,
    });

  // Require that at least 1 durable store stream be present
  const haveDurableStream =
    (fsBackupStream || objStoreStream || dynamoStream) !== undefined;
  if (!haveDurableStream) {
    errorResponse(ctx, {
      status: 503,
      errorMessage:
        "No durable storage stream available. Cannot proceed with upload.",
    });
    return next();
  }

  // Create a streaming data item with the request body
  const streamingDataItem: DataItemInterface =
    rawContentLength !== undefined &&
    rawContentLength <= inMemoryDataItemThreshold
      ? new InMemoryDataItem(
          await streamToBuffer(ctx.request.req, rawContentLength)
        )
      : new StreamingDataItem(ctx.request.req, logger);
  ctx.request.req.resume();

  // Assess a Winston price and/or whitelist-status for this upload once
  // enough data item info has streamed to the data item event emitter
  let signatureType: number;
  let signature: string;
  let owner: string;
  let ownerPublicAddress: string;
  let dataItemId: string;
  let targetPublicAddress: string | undefined;

  try {
    signatureType = await streamingDataItem.getSignatureType();
    signature = await streamingDataItem.getSignature();
    owner = await streamingDataItem.getOwner();
    ownerPublicAddress = await streamingDataItem.getOwnerAddress();
    targetPublicAddress = await streamingDataItem.getTarget();

    dataItemId = await streamingDataItem.getDataItemId();

    if (signatureTypeOverride !== undefined) {
      logger.debug("Overriding signature type from token route...");
      signatureType = signatureTypeOverride;
    }

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
  if (await dataItemIsInFlight({ dataItemId, cacheService, logger })) {
    // create the error for consistent responses
    const error = new DataItemExistsWarning(dataItemId);
    logger.warn("Data item already uploaded to this service instance.");
    MetricRegistry.localCacheDataItemHit.inc();
    ctx.status = 202;
    ctx.res.statusMessage = error.message;
    return next();
  }
  logger.debug(
    `Data item ${dataItemId} is not in-flight. Proceeding with upload...`
  );
  await markInFlight({ dataItemId, cacheService, logger });

  // Reserve balance for this upload if the content-length header was present
  if (shouldSkipBalanceCheck) {
    logger.debug("Skipping balance check...");
  } else if (rawContentLength !== undefined) {
    let checkBalanceResponse: CheckBalanceResponse;
    try {
      logger.debug("Checking balance for upload...");
      checkBalanceResponse = await paymentService.checkBalanceForData({
        nativeAddress,
        paidBy,
        size: rawContentLength,
        signatureType,
      });
    } catch (error) {
      await removeFromInFlight({ dataItemId, cacheService, logger });
      errorResponse(ctx, {
        status: 503,
        errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Payment Service is unreachable`,
      });
      return next();
    }

    if (checkBalanceResponse.userHasSufficientBalance) {
      logger.debug("User can afford bytes", checkBalanceResponse);
    } else {
      await removeFromInFlight({ dataItemId, cacheService, logger });

      errorResponse(ctx, {
        status: 402,
        error: new InsufficientBalance(),
      });
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
    await removeFromInFlight({ dataItemId, cacheService, logger });
    errorResponse(ctx, {
      errorMessage: "Data item parsing error!",
      error,
    });

    return next();
  }

  const plannedStores = allValidDataItemStores.filter(
    (_, i) =>
      [cacheServiceStream, fsBackupStream, objStoreStream, dynamoStream][i]
  );
  let actualStores: ValidDataItemStore[] = [];
  try {
    actualStores = await cacheDataItem({
      streamingDataItem,
      rawContentLength,
      payloadContentType,
      payloadDataStart,
      cacheService,
      objectStore,
      cacheServiceStream,
      fsBackupStream,
      objStoreStream,
      dynamoStream,
      logger,
      durations,
    });
  } catch (error) {
    await removeFromInFlight({ dataItemId, cacheService, logger });
    errorResponse(ctx, {
      status: 503,
      errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Object Store is unreachable`,
      error,
    });

    return next();
  }

  logger.debug(`Assessing data item validity...`);
  const performQuarantine = async (errRspData: {
    errorMessage?: string;
    status?: number;
    error?: unknown;
  }) => {
    await quarantineDataItem({
      dataItemId,
      objectStore,
      cacheService,
      database,
      logger,
      contentLength: rawContentLength,
      contentType: requestContentType,
      payloadInfo:
        payloadContentType && payloadDataStart
          ? {
              payloadContentType,
              payloadDataStart,
            }
          : undefined,
    }).catch((error) => {
      logger.error("Remove data item failed!", { error });
    });

    errorResponse(ctx, errRspData);
  };
  let isValid: boolean;
  try {
    isValid = await streamingDataItem.isValid();
  } catch (error) {
    await removeFromInFlight({ dataItemId, cacheService, logger });
    await performQuarantine({
      errorMessage: "Data item parsing error!",
      error,
    });
    return next();
  }
  logger.debug(`Got data item validity.`, { isValid });
  if (!isValid) {
    await removeFromInFlight({ dataItemId, cacheService, logger });
    await performQuarantine({
      errorMessage: "Invalid Data Item!",
    });
    return next();
  }

  // NOTE: Safe to get payload size now that payload has been fully consumed
  const payloadDataByteCount = await streamingDataItem.getPayloadSize();
  const totalSize = payloadDataByteCount + payloadDataStart;

  if (totalSize > maxSingleDataItemByteCount) {
    await removeFromInFlight({ dataItemId, cacheService, logger });
    await performQuarantine({
      errorMessage: `Data item is too large, this service only accepts data items up to ${maxSingleDataItemByteCount} bytes!`,
    });
    return next();
  }

  if (blocklistedAddresses.includes(ownerPublicAddress)) {
    logger.info(
      "The owner's address is on the arweave public address block list. Rejecting data item..."
    );
    await removeFromInFlight({ dataItemId, cacheService, logger });
    await performQuarantine({
      status: 403,
      errorMessage: "Forbidden",
    });
    return next();
  }

  // TODO: Check arweave gateway cached blocklist for address

  // TODO: Configure via SSM Parameter Store
  const spammerContentLength = +(process.env.SPAMMER_CONTENT_LENGTH ?? 100372);
  if (
    rawContentLength &&
    rawContentLength === spammerContentLength &&
    tags.length === 0
  ) {
    logger.info(
      "Incoming data item matches known spammer pattern. No tags and content length of 100372 bytes. Rejecting data item..."
    );
    await removeFromInFlight({ dataItemId, cacheService, logger });
    await performQuarantine({
      status: 403,
      errorMessage: "Forbidden",
    });
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
        paidBy,
      });
      logger = logger.child({ paymentResponse });
    } catch (error) {
      errorResponse(ctx, {
        status: 503,
        errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Payment Service is unreachable`,
      });
      await removeFromInFlight({ dataItemId, cacheService, logger });
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
        error: new InsufficientBalance(),
      });

      await removeFromInFlight({ dataItemId, cacheService, logger });
      return next();
    }
  }

  // admin action tags
  const approvedAddress = tags.find(
    (tag) => tag.name === createDelegatedPaymentApprovalTagName
  )?.value;
  let createdApproval: DelegatedPaymentApproval | undefined = undefined;
  if (approvedAddress) {
    const winc = tags.find((tag) => tag.name === approvalAmountTagName)?.value;
    if (winc === undefined) {
      await removeFromInFlight({ dataItemId, cacheService, logger });
      errorResponse(ctx, {
        errorMessage: "Approval x-amount tag missing a winc value!",
      });

      return next();
    }

    const expiresInSeconds = tags.find(
      (tag) => tag.name === approvalExpiresBySecondsTagName
    )?.value;

    try {
      createdApproval = await paymentService.createDelegatedPaymentApproval({
        approvedAddress,
        payingAddress: nativeAddress,
        dataItemId,
        winc,
        expiresInSeconds,
      });
    } catch (error) {
      const message = `Unable to create delegated payment approval ${
        error instanceof PaymentServiceReturnedError
          ? `: ${error.message}`
          : "!"
      }`;
      await removeFromInFlight({ dataItemId, cacheService, logger });
      errorResponse(ctx, {
        errorMessage: message,
      });
      if (paymentResponse.costOfDataItem.isGreaterThan(W(0))) {
        await paymentService.refundBalanceForData({
          dataItemId,
          nativeAddress,
          signatureType,
          winston: paymentResponse.costOfDataItem,
        });
      }

      return next();
    }
  }
  const revokedAddress = tags.find(
    (tag) => tag.name === revokeDelegatePaymentApprovalTagName
  )?.value;
  let revokedApprovals: DelegatedPaymentApproval[] = [];
  if (revokedAddress) {
    try {
      revokedApprovals = await paymentService.revokeDelegatedPaymentApprovals({
        revokedAddress,
        payingAddress: nativeAddress,
        dataItemId,
      });
    } catch (error) {
      const message = `Unable to revoke delegated payment approval ${
        error instanceof PaymentServiceReturnedError
          ? `: ${error.message}`
          : "!"
      }`;
      await removeFromInFlight({ dataItemId, cacheService, logger });
      errorResponse(ctx, {
        errorMessage: message,
        error,
      });
      if (paymentResponse.costOfDataItem.isGreaterThan(W(0))) {
        await paymentService.refundBalanceForData({
          dataItemId,
          nativeAddress,
          signatureType,
          winston: paymentResponse.costOfDataItem,
        });
      }

      return next();
    }
  }

  const uploadTimestamp = Date.now();

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
      const signedDataItemHeader = await signDataItemHeader(
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
      );

      await enqueue(jobLabels.opticalPost, {
        ...signedDataItemHeader,
        uploaded_at: uploadTimestamp,
      });
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
      await enqueue(jobLabels.unbundleBdi, {
        id: dataItemId,
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

  let signedReceipt: SignedReceipt;
  let deadlineHeight: number;
  try {
    // Ensure at least 1 store still has the data item before signing the receipt
    if (!(await dataItemExists(dataItemId, cacheService, objectStore))) {
      throw new Error(`Data item not found in any store.`);
    }

    // TODO: Make failure here less dire when nodes are struggling, e.g. via static or remote cache
    const currentBlockHeight = await arweaveGateway.getCurrentBlockHeight();
    const jwk = await getArweaveWallet();

    deadlineHeight = currentBlockHeight + deadlineHeightIncrement;
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
    logger.info("Receipt signed!", {
      ...filterKeysFromObject(signedReceipt, ["public", "signature"]),
      plannedStores,
      actualStores,
    });
  } catch (error) {
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
    await removeFromInFlight({ dataItemId, cacheService, logger });
    await performQuarantine({
      status: 503,
      errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable. Unable to sign receipt...`,
      error,
    });

    return next();
  }

  let nestedDataItemHeaders: ParsedDataItemHeader[] = [];
  if (
    streamingDataItem instanceof InMemoryDataItem &&
    containsAns104Tags(tags)
  ) {
    // For in memory BDIs, get a payload stream and unbundle it into nested data item headers only one level deep
    nestedDataItemHeaders = (await processStream(
      await streamingDataItem.getPayloadStream()
    )) as ParsedDataItemHeader[];
  }

  const premiumFeatureType = getPremiumFeatureType(
    ownerPublicAddress,
    tags,
    signatureType,
    nestedDataItemHeaders,
    targetPublicAddress
  );

  const dbInsertStart = Date.now();
  try {
    await enqueue(jobLabels.newDataItem, {
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
    // TODO: Add status fetching from valkey to eliminate need for this
    await sleep(20);

    durations.dbInsertDuration = Date.now() - dbInsertStart;
    logger.debug(`DB insert duration: ${durations.dbInsertDuration}ms`);
  } catch (error) {
    logger.debug(`DB insert failed duration: ${Date.now() - dbInsertStart}ms`);

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
    await removeFromInFlight({ dataItemId, cacheService, logger });
    await performQuarantine({
      status: 503,
      errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable.`,
      error,
    });
    return next();
  }

  ctx.status = 200;

  let body: Record<
    string,
    | string
    | number
    | string[]
    | DelegatedPaymentApproval
    | DelegatedPaymentApproval[]
  > = {
    ...signedReceipt,
    owner: ownerPublicAddress,
  };
  if (createdApproval) {
    body = {
      ...body,
      createdApproval,
    };
  }
  if (revokedApprovals.length > 0) {
    body = {
      ...body,
      revokedApprovals,
    };
  }
  ctx.body = body;

  await removeFromInFlight({ dataItemId, cacheService, logger });

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
