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
import pLimit from "p-limit";
import { Readable } from "stream";

import { defaultArchitecture } from "../arch/architecture";
import { ArweaveGateway, Gateway } from "../arch/arweaveGateway";
import { CacheService } from "../arch/cacheServiceTypes";
import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { getElasticacheService } from "../arch/elasticacheService";
import { ObjectStore } from "../arch/objectStore";
import { PricingService } from "../arch/pricing";
import { createQueueHandler, enqueue } from "../arch/queues";
import { ArweaveInterface } from "../arweaveJs";
import {
  assembleBundleHeader,
  bundleHeaderInfoFromBuffer,
  totalBundleSizeFromHeaderInfo,
} from "../bundles/assembleBundleHeader";
import {
  bufferIdFromBufferSignature,
  bufferIdFromReadableSignature,
} from "../bundles/idFromSignature";
import { signatureTypeInfo } from "../constants";
import {
  PremiumPaidFeatureType,
  dedicatedBundleTypes,
  gatewayUrl,
  jobLabels,
} from "../constants";
import defaultLogger from "../logger";
import { PlanId, PlannedDataItem } from "../types/dbTypes";
import { JWKInterface } from "../types/jwkTypes";
import { DataItemOffsetsInfo } from "../types/types";
import { W } from "../types/winston";
import {
  filterKeysFromObject,
  generateArrayChunks,
  sleep,
} from "../utils/common";
import {
  DataItemAttributes,
  assembleBundlePayload,
  getRawSignatureOfDataItem,
  getSignatureTypeOfDataItem,
} from "../utils/dataItemUtils";
import { BundlePlanExistsInAnotherStateWarning } from "../utils/errors";
import { getArweaveWallet } from "../utils/getArweaveWallet";
import {
  getBundlePayload,
  getS3ObjectStore,
  putBundlePayload,
  putBundleTx,
} from "../utils/objectStoreUtils";
import { streamToBuffer } from "../utils/streamToBuffer";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { version } = require("../../package.json");
const PARALLEL_LIMIT = 100;
interface PrepareBundleJobInjectableArch {
  database?: Database;
  objectStore?: ObjectStore;
  cacheService?: CacheService;
  jwk?: JWKInterface;
  pricing?: PricingService;
  arweaveGateway?: Gateway;
  arweave?: ArweaveInterface;
}

/**
 * Uses next bundle plan from to prepare a Bundle Transaction for posting
 *
 * - Gets planned Data Items from DB
 * - Assembles Bundle Header onto Object Store
 * - Assembles and signs Bundle tx onto ObjectStore
 * - Inserts NewBundle into DB for Post job
 */
export async function prepareBundleHandler(
  planId: PlanId,
  {
    database = new PostgresDatabase(),
    objectStore = getS3ObjectStore(),
    cacheService = getElasticacheService(),
    jwk,
    arweaveGateway = new ArweaveGateway({
      endpoint: gatewayUrl,
    }),
    pricing = new PricingService({ gateway: arweaveGateway }),
    arweave = new ArweaveInterface(),
  }: PrepareBundleJobInjectableArch,
  logger = defaultLogger.child({ job: jobLabels.prepareBundle, planId })
): Promise<void> {
  if (!jwk) {
    jwk = await getArweaveWallet();
  }

  let dbDataItems: PlannedDataItem[];
  try {
    dbDataItems = await database.getPlannedDataItemsForPlanId(planId);
  } catch (error) {
    if (error instanceof BundlePlanExistsInAnotherStateWarning) {
      logger.warn(error.message);
      return;
    }
    throw error;
  }

  if (dbDataItems.length === 0) {
    throw Error(
      `No planned data items or for plan id ${planId}!\nReader may be out of sync or this could be an empty bundle plan.`
    );
  }
  const dataItemCount = dbDataItems.length;
  const totalDataItemsSize = dbDataItems.reduce(
    (acc, dataItem) => acc + dataItem.byteCount,
    0
  );
  logger.info(`Preparing data items.`, {
    dataItemCount,
    totalDataItemsSize,
  });

  // Assemble bundle header
  // This could be done in plan job -- or another specific bundleHeader job
  const parallelLimit = pLimit(PARALLEL_LIMIT);
  const dataItemRawIdsAndByteCounts = await Promise.all(
    dbDataItems.map(({ byteCount, dataItemId, signatureType, signature }) => {
      return parallelLimit(async () => {
        logger.debug("Getting raw signature of data item.", {
          dataItemId,
        });
        const sigType =
          signatureType ??
          (await getSignatureTypeOfDataItem(
            objectStore,
            cacheService,
            dataItemId,
            logger
          ));

        const dataItemRawId = signature
          ? // Use signature from db if exists
            await bufferIdFromBufferSignature(signature)
          : await bufferIdFromReadableSignature(
              // Else fallback to raw signature from object store
              await getRawSignatureOfDataItem(
                objectStore,
                cacheService,
                dataItemId,
                sigType,
                logger
              ),
              signatureTypeInfo[sigType].signatureLength
            );

        logger.debug("Parsed data item raw id.", {
          dataItemRawId,
          dataItemId,
        });
        return { dataItemRawId, byteCount };
      });
    })
  );
  logger.debug("Assembling bundle header.");
  const bundleHeaderReadable = await assembleBundleHeader(
    dataItemRawIdsAndByteCounts
  );
  const bundleHeaderBuffer = await streamToBuffer(bundleHeaderReadable);

  // Call pricing service to determine reward and tip settings for bundle
  const txAttributes = await pricing.getTxAttributesForDataItems(dbDataItems);

  logger = logger.child({
    txAttributes,
    totalDataItemsSize,
    dataItemCount,
  });

  const totalPayloadSize = totalBundleSizeFromHeaderInfo(
    bundleHeaderInfoFromBuffer(bundleHeaderBuffer)
  );
  logger.debug("Caching bundle payload.", {
    payloadSize: totalPayloadSize,
  });

  // Assemble bundle payload and get data item attributes generator
  const { payloadReadable, dataItemAttributesPromise } = assembleBundlePayload(
    objectStore,
    cacheService,
    bundleHeaderBuffer,
    logger
  );

  try {
    await putBundlePayload(
      objectStore,
      planId,
      payloadReadable
      // HACK: Attempting to remove totalPayloadSize to appease AWS V3 SDK
      // totalPayloadSize
    );
  } catch (error) {
    // TODO: Determine equivalent error for DynamoDB
    if (isNoSuchKeyS3Error(error)) {
      const dataItemId = error.Key.split("/")[1];

      // TODO: we need to add refund balance here
      await database.updatePlannedDataItemAsFailed({
        dataItemId,
        failedReason: "missing_from_object_store",
      });

      // TODO: This is a hack -- recurse to retry the job without the deleted data item
      await sleep(100); // Sleep to combat replication lag
      return prepareBundleHandler(planId, {
        database,
        objectStore,
        cacheService,
        jwk,
        pricing,
        arweaveGateway,
        arweave,
      });
    }
    logger.error("Failed to cache bundle payload!", {
      error,
    });
    throw error;
  }

  const headerByteCount = bundleHeaderBuffer.byteLength;

  logger.debug("Successfully cached bundle payload.", {
    planId,
  });
  // TODO: OPTIMIZE THIS! Potentially by splitting streams above? Consider stream consumer rates...
  const bundleTx = await arweave.createTransactionFromPayloadStream(
    await getBundlePayload(objectStore, planId),
    txAttributes,
    jwk
  );

  logger.debug("Successfully assembled bundle transaction.", {
    txId: bundleTx.id,
  });
  bundleTx.addTag("Bundle-Format", "binary");
  bundleTx.addTag("Bundle-Version", "2.0.0");

  const premiumFeatureType = dbDataItems[0].premiumFeatureType;
  const bundlerAppName =
    dedicatedBundleTypes[premiumFeatureType as PremiumPaidFeatureType]
      ?.bundlerAppName ?? undefined;
  if (bundlerAppName) {
    bundleTx.addTag("Bundler-App-Name", bundlerAppName);
  }

  bundleTx.addTag("App-Name", process.env.APP_NAME ?? "ArDrive Turbo");
  bundleTx.addTag("App-Version", version);

  // Mint $U
  bundleTx.addTag("App-Name", "SmartWeaveAction");
  bundleTx.addTag("App-Version", "0.3.0"); // cspell:disable
  bundleTx.addTag("Contract", "KTzTXT_ANmF84fWEKHzWURD1LWd9QaFR9yfYUwH2Lxw"); // cspell:enable
  bundleTx.addTag("Input", JSON.stringify({ function: "mint" }));

  await arweave.signTx(bundleTx, jwk);

  logger.debug("Successfully signed bundle transaction.", {
    txId: bundleTx.id,
  });

  // Send the collected data item offsets info to DynamoDB
  try {
    logger.info("[offsets] Awaiting data item attributes...");
    const dataItemAttributes = await dataItemAttributesPromise;

    // Store data item metadata in DynamoDB
    logger.info("[offsets] Storing data item metadata in DynamoDB.", {
      dataItemCount: dataItemAttributes.length,
      bundleId: bundleTx.id,
    });

    let batchNumber = 1;
    for (const attributesBatch of generateArrayChunks(
      dataItemAttributes,
      250
    )) {
      // Enqueue the offsets info to be put into DynamoDB
      try {
        await enqueue(jobLabels.putOffsets, {
          offsets: attributesBatch.map(
            (attributes: DataItemAttributes): DataItemOffsetsInfo => ({
              dataItemId: attributes.dataItemId,
              rootBundleId: bundleTx.id,
              startOffsetInRootBundle: attributes.rawDataItemOffsetInBundle,
              rawContentLength: attributes.rawDataItemSize,
              payloadContentType: attributes.payloadContentType,
              payloadDataStart: attributes.payloadDataStartOffset,
              parentDataItemId: attributes.parentDataItemId,
              startOffsetInParentDataItemPayload:
                attributes.startOffsetInParentDataItemPayload,
            })
          ),
        });
        batchNumber++;
      } catch (error) {
        logger.error("Failed to enqueue offsets for DynamoDB insert", {
          bundleId: bundleTx.id,
          error: error instanceof Error ? error.message : error,
          batchNumber,
        });
        throw error;
      }
    }
  } catch (error) {
    logger.error("[offsets] Failed to collect data item attributes!", {
      bundleId: bundleTx.id,
      error,
    });

    // TODO: Is this the control flow we want?
    throw error;
  }

  bundleTx.data = new Uint8Array(0);
  const serializedBundleTx = JSON.stringify(bundleTx.toJSON());
  const bundleTxBuffer = Buffer.from(serializedBundleTx);

  logger.debug("Updating object-store with bundled transaction.", {
    txId: bundleTx.id,
  });

  await putBundleTx(objectStore, bundleTx.id, Readable.from(bundleTxBuffer));

  try {
    await database.insertNewBundle({
      planId,
      bundleId: bundleTx.id,
      reward: W(bundleTx.reward),
      payloadByteCount: totalPayloadSize,
      headerByteCount,
      transactionByteCount: bundleTxBuffer.byteLength,
    });
  } catch (error) {
    if (error instanceof BundlePlanExistsInAnotherStateWarning) {
      logger.warn(error.message);
      return;
    }
    throw error;
  }
  await enqueue(jobLabels.postBundle, { planId });

  logger.info("Successfully updated object-store with bundle transaction", {
    bundleTx: filterKeysFromObject(bundleTx, [
      "data",
      "chunks",
      "owner",
      "tags",
    ]),
  });
}
export const handler = createQueueHandler(
  jobLabels.prepareBundle,
  (message: { planId: PlanId }) =>
    prepareBundleHandler(message.planId, defaultArchitecture),
  {
    before: async () => {
      defaultLogger.info("Prepare bundle job has been triggered.");
    },
  }
);

export function isNoSuchKeyS3Error(
  error: unknown
): error is { Code: "NoSuchKey"; Key: string } {
  return (error as { Code: string })?.Code === "NoSuchKey";
}
