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
import {
  Tag,
  processStream,
  serializeTags,
  streamSigner,
} from "@dha-team/arbundles";
import { EthereumSigner } from "@dha-team/arbundles";
import { randomUUID } from "crypto";
import { Next } from "koa";
import { PaymentRequirements } from "x402/types";

import { enqueue } from "../arch/queues";
import {
  PaymentSettlementResult,
  PaymentVerificationResult,
  paymentPayloadHasAuthorization,
} from "../arch/x402";
import {
  DataItemInterface,
  InMemoryDataItem,
  StreamingDataItem,
} from "../bundles/streamingDataItem";
import {
  allowArFSData,
  freeUploadLimitBytes,
  inMemoryDataItemThreshold,
  maxRawDataEndpointByteCount,
  signatureTypeInfo,
  stubEvmAddress,
} from "../constants";
import {
  blocklistedAddresses,
  dataCaches,
  deadlineHeightIncrement,
  emptyAnchorLength,
  emptyTargetLength,
  fastFinalityIndexes,
  jobLabels,
  maxSingleDataItemByteCount,
  octetStreamContentType,
  receiptVersion,
  skipOpticalPostAddresses,
} from "../constants";
import globalLogger from "../logger";
import { MetricRegistry } from "../metricRegistry";
import { KoaContext } from "../server";
import { ParsedDataItemHeader, SignatureConfig, W } from "../types/types";
import {
  errorResponse,
  filterKeysFromObject,
  getPremiumFeatureType,
  payloadContentTypeFromDecodedTags,
  sleep,
  x402PaymentRequiredResponse,
} from "../utils/common";
import { extractTagsFromHeaders } from "../utils/common";
import {
  ValidDataItemStore,
  allValidDataItemStores,
  cacheDataItem,
  containsAns104Tags,
  quarantineDataItem,
  streamsForDataItemStorage,
} from "../utils/dataItemUtils";
import { DataItemExistsWarning } from "../utils/errors";
import {
  UPLOAD_DATA_PATH,
  ensureDataItemsBackupDirExists,
  isBackupFSNeeded,
} from "../utils/fileSystemUtils";
import {
  dataItemIsInFlight,
  markInFlight,
  removeFromInFlight,
} from "../utils/inFlightDataItemCache";
import {
  getRawUnsignedData,
  putRawUnsignedData,
} from "../utils/objectStoreUtils";
import {
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

const opticalBridgingEnabled = process.env.OPTICAL_BRIDGING_ENABLED !== "false";
isBackupFSNeeded()
  .then((needed) =>
    needed
      ? ensureDataItemsBackupDirExists().catch((error) => {
          globalLogger.error(
            `Failed to create upload data directory at ${UPLOAD_DATA_PATH}!`,
            { error }
          );
          throw error;
        })
      : Promise.resolve()
  )
  .catch((error) => {
    globalLogger.error(`Failed to determine if backup filesystem is needed!`, {
      error,
    });
    throw error;
  });

export async function x402RawDataPostRoute(ctx: KoaContext, next: Next) {
  let { logger } = ctx.state;
  const {
    objectStore,
    cacheService,
    arweaveGateway,
    getArweaveWallet,
    getEVMDataItemSigningPrivateKey,
    database,
    pricingService,
    x402Service,
  } = ctx.state;

  const token = ctx.params.token;
  let signatureTypeOverride: number | undefined;
  if (token === "kyve") {
    signatureTypeOverride = SignatureConfig.KYVE;
  }

  // Inspect, but do not assert, the content-type header to ensure it is octet-stream of potential ANS-104 Data Item
  const requestContentType = ctx.req.headers?.["content-type"];

  if (!requestContentType) {
    logger.debug("Missing request content type!");
  }

  // Validate and assert the content-length header
  const contentLengthStr = ctx.req.headers?.["content-length"];
  const rawContentLength = contentLengthStr ? +contentLengthStr : undefined;
  if (rawContentLength === undefined) {
    errorResponse(ctx, {
      errorMessage:
        "Missing Content Length. Content Length is required for raw data post.",
      status: 411,
    });
    return next();
  } else if (rawContentLength > maxRawDataEndpointByteCount) {
    errorResponse(ctx, {
      errorMessage: `Raw data is too large, this service only accepts data up to ${maxRawDataEndpointByteCount} bytes!`,
    });
    return next();
  }

  // parse the payment payload from base64
  const paymentHeaderValue = ctx.headers["x-payment"] as string;
  const paymentPayload = x402Service.extractPaymentPayload(paymentHeaderValue);

  const customTagsFromHeaders = extractTagsFromHeaders(ctx.req.headers);
  const gqlTags = [
    {
      name: "Content-Type",
      value: requestContentType || octetStreamContentType,
    },
    { name: "Data-Item-Authority", value: "Turbo Upload Service" },
    ...customTagsFromHeaders, // Inject any custom tags from headers
  ];

  logger.debug("Incoming Tags:", { gqlTags });

  // We're not using the optional target and anchor fields, they will always be 1 byte
  const targetLength = 1;
  const anchorLength = 1;

  const isFreeUpload =
    allowArFSData && rawContentLength <= freeUploadLimitBytes;

  // Get byte length of tags after being serialized for avro schema
  const serializedTags = serializeTags([
    ...gqlTags,
    ...(isFreeUpload
      ? []
      : [
          {
            name: "X402-Payer-Address",
            value: paymentPayloadHasAuthorization(paymentPayload)
              ? paymentPayload.payload.authorization.from
              : stubEvmAddress,
          },
        ]),
  ]);
  const tagsLength = 16 + serializedTags.byteLength;

  const signerLength =
    signatureTypeInfo[SignatureConfig.ETHEREUM].signatureLength;
  const ownerLength = signatureTypeInfo[SignatureConfig.ETHEREUM].pubkeyLength;

  const signatureTypeLength = 2;

  const estimatedByteLength =
    signerLength +
    ownerLength +
    signatureTypeLength +
    targetLength +
    anchorLength +
    tagsLength +
    rawContentLength;

  logger.debug("Estimated byte length for x402 upload:", {
    estimatedByteLength,
  });

  const price = await pricingService.getUsdcForByteCount(estimatedByteLength);
  const winc = isFreeUpload ? W(0) : price.winc;

  const paymentRequirements: PaymentRequirements =
    x402Service.calculateRequirements({
      usdcAmount: `$${price.mUsdc / 1e6}`,
      contentLength: estimatedByteLength,
      contentType: requestContentType || octetStreamContentType,
      resourceUrl: `${ctx.protocol}://${ctx.host}${ctx.originalUrl}`,
    });

  let x402PaymentVerification: PaymentVerificationResult | undefined =
    undefined;
  if (!isFreeUpload) {
    if (paymentPayload === undefined) {
      x402PaymentRequiredResponse(ctx, {
        paymentRequirements,
        error: "X-Payment header is required for x402 uploads.",
      });
      return next();
    }

    // Verify payment first
    x402PaymentVerification = await x402Service.verifyPayment(
      paymentPayload,
      paymentRequirements
    );

    if (
      !x402PaymentVerification.isValid ||
      !x402PaymentVerification.payerAddress
    ) {
      x402PaymentRequiredResponse(ctx, {
        paymentRequirements,
        error:
          x402PaymentVerification.invalidReason ||
          "Payment verification failed",
      });
      return next();
    }
  }

  const randomDataHash = randomUUID();

  logger.debug(`Storing raw unsigned data with hash ${randomDataHash}...`);

  await putRawUnsignedData(
    objectStore,
    randomDataHash,
    ctx.req,
    requestContentType || octetStreamContentType
  );

  const signer = new EthereumSigner(await getEVMDataItemSigningPrivateKey());

  if (
    x402PaymentVerification !== undefined &&
    x402PaymentVerification.payerAddress !== undefined
  ) {
    gqlTags.push({
      name: "X402-Payer-Address",
      value: x402PaymentVerification.payerAddress,
    });
  }

  logger.debug("Signing data item from S3 Streams...");

  const dataItemStream = await streamSigner(
    await getRawUnsignedData(objectStore, randomDataHash),
    await getRawUnsignedData(objectStore, randomDataHash),
    signer,
    { tags: gqlTags }
  );

  dataItemStream.pause();

  const { cacheServiceStream, fsBackupStream, objStoreStream, dynamoStream } =
    await streamsForDataItemStorage({
      inputStream: dataItemStream,
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
      ? new InMemoryDataItem(await streamToBuffer(dataItemStream))
      : new StreamingDataItem(dataItemStream, logger);
  dataItemStream.resume();

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

  // Parse out the content type and the payload stream
  let payloadContentType: string;
  let payloadDataStart: number;
  let anchor: string | undefined;
  let target: string | undefined;
  let tagsFromDataItem: Tag[];
  try {
    // Log some useful debugging info
    anchor = await streamingDataItem.getAnchor();
    target = await streamingDataItem.getTarget();
    const numTags = await streamingDataItem.getNumTags();
    const numTagsBytes = await streamingDataItem.getNumTagsBytes();
    tagsFromDataItem = await streamingDataItem.getTags();
    payloadContentType = payloadContentTypeFromDecodedTags(tagsFromDataItem);

    // Log tags and other useful info for log parsing
    logger = logger.child({
      payloadContentType,
      numTags,
      tags: customTagsFromHeaders,
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

  if (totalSize > estimatedByteLength) {
    logger.error(
      "Data size larger than estimated from content length. x402 uploads require accurate Content-Length header!",
      {
        estimatedByteLength,
        totalSize,
        gqlTags,
        tagsLength,
        verification: x402PaymentVerification,
      }
    );
    await removeFromInFlight({ dataItemId, cacheService, logger });
    await performQuarantine({
      status: 400,
      errorMessage: `Data size larger than estimated from content length. Estimated ${estimatedByteLength} bytes, got ${totalSize} bytes. x402 uploads require accurate Content-Length header!`,
    });
    return next();
  }

  if (
    blocklistedAddresses.includes(
      x402PaymentVerification?.payerAddress ?? ownerPublicAddress
    )
  ) {
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
          tags: tagsFromDataItem,
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
  if (containsAns104Tags(tagsFromDataItem)) {
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

  let receipt: UnsignedReceipt | SignedReceipt = {
    id: dataItemId,
    timestamp: uploadTimestamp,
    winc: winc.toString(),
    version: receiptVersion,
    deadlineHeight: 3_000_000, // Default far future deadline height
    ...confirmedFeatures,
  };
  try {
    const currentBlockHeight = await arweaveGateway.getCurrentBlockHeight();
    const jwk = await getArweaveWallet();

    receipt.deadlineHeight = currentBlockHeight + deadlineHeightIncrement;
    receipt = await signReceipt(receipt, jwk);
    // Log the signed receipt for log parsing
    logger.info("Receipt signed!", {
      ...filterKeysFromObject(receipt, ["public", "signature"]),
      plannedStores,
      actualStores,
    });
  } catch (error) {
    // Soft error on x402 uploads, we've already settled the payment
    logger.warn(
      `Receipt signature has unexpectedly failed... Continuing with unsigned receipt.`,
      { error: error instanceof Error ? error.message : String(error), receipt }
    );
  }

  try {
    let nestedDataItemHeaders: ParsedDataItemHeader[] = [];
    if (
      streamingDataItem instanceof InMemoryDataItem &&
      containsAns104Tags(tagsFromDataItem)
    ) {
      // For in memory BDIs, get a payload stream and unbundle it into nested data item headers only one level deep
      nestedDataItemHeaders = (await processStream(
        await streamingDataItem.getPayloadStream()
      )) as ParsedDataItemHeader[];
    }

    const premiumFeatureType = getPremiumFeatureType(
      ownerPublicAddress,
      tagsFromDataItem,
      signatureType,
      nestedDataItemHeaders,
      targetPublicAddress
    );

    logger.debug("Enqueuing data item for processing job...", {
      premiumFeatureType,
    });

    await enqueue(jobLabels.newDataItem, {
      dataItemId,
      ownerPublicAddress:
        x402PaymentVerification?.payerAddress || ownerPublicAddress,
      assessedWinstonPrice: winc,
      byteCount: totalSize,
      payloadDataStart,
      signatureType,
      failedBundles: [],
      uploadedDate: new Date(uploadTimestamp).toISOString(),
      payloadContentType,
      premiumFeatureType,
      signature,
      deadlineHeight: receipt.deadlineHeight,
    });

    // Anticipate 20ms of replication delay. Modicum of protection against caller checking status immediately after returning
    await sleep(20);
  } catch (error) {
    logger.error(`Data item failed to be enqueued for processing!`, {
      assessedWinstonPrice: winc,
      receipt,
      dataItemId,
      error: error instanceof Error ? error.message : String(error),
    });
    // always remove from instance cache
    await removeFromInFlight({ dataItemId, cacheService, logger });
    errorResponse(ctx, {
      status: 503,
      errorMessage: `Data Item: ${dataItemId}. Upload Service is Unavailable.`,
      error,
    });
    return next();
  }

  // We settle AFTER all work is done to avoid settlement failure cases affecting user experience
  // NOTE: This could lead to rare cases of data being accepted but payment not being settled
  let settlement: PaymentSettlementResult | undefined = undefined;
  if (!isFreeUpload) {
    if (x402PaymentVerification === undefined || paymentPayload === undefined) {
      logger.error(
        "X402 payment verification state missing before settlement! This is a server error, it should exist. Something went wrong.",
        {
          verification: x402PaymentVerification,
          paymentPayload,
          dataItemId,
          ownerPublicAddress,
          winc: winc.toString(),
          totalSize,
        }
      );
      await removeFromInFlight({ dataItemId, cacheService, logger });
      await performQuarantine({
        status: 500,
        errorMessage: "Internal Server Error",
      });
      return next();
    }

    // Settle payment for this upload now this the data item is fully received and valid
    settlement = await x402Service.settlePayment(
      paymentPayload,
      paymentRequirements
    );

    // If settlement failed, quarantine the data item and return error
    if (settlement.success === false || !settlement.transaction) {
      await removeFromInFlight({ dataItemId, cacheService, logger });
      await performQuarantine({
        status: 402,
        errorMessage: "Payment failed",
        error: settlement.errorReason,
      });
      return next();
    }

    // attempt to record settled x402 payment in database - wrapped in try/catch to avoid
    // affecting upload flow should db be unreachable
    try {
      if (x402PaymentVerification.payerAddress === undefined) {
        throw new Error("Missing payer address from payment verification");
      }

      if (x402PaymentVerification.usdcAmount === undefined) {
        throw new Error("Missing usdc amount from payment verification");
      }

      await database.insertX402Payment({
        txHash: settlement.transaction,
        network: settlement.network,
        payerAddress: x402PaymentVerification.payerAddress,
        usdcAmount: x402PaymentVerification.usdcAmount,
        wincAmount: winc,
        dataItemId,
        byteCount: totalSize,
      });
      logger.info("x402 Payment settled successfully!", {
        transactionHash: settlement.transaction,
        payerAddress: x402PaymentVerification.payerAddress,
        usdcAmount: x402PaymentVerification.usdcAmount,
        wincAmount: winc.toString(),
      });
    } catch (dbError) {
      logger.error("Failed to record x402 payment in database!", {
        transactionHash: settlement.transaction,
        dataItemId,
        error: dbError,
      });
      // Payment was settled but not recorded - requires manual reconciliation
      // Don't quarantine data item since payment succeeded
      // Continue processing to avoid user paying without getting service
    }
  }

  ctx.status = 200;

  const body: Record<string, string | number | string[] | undefined> = {
    ...receipt,
    owner: ownerPublicAddress,
    x402PaymentTxId: settlement?.transaction,
    mUSDCPaid: x402PaymentVerification?.usdcAmount,
  };
  ctx.body = body;

  await removeFromInFlight({ dataItemId, cacheService, logger });

  return next();
}
