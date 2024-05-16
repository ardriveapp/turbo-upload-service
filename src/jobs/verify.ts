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
import winston from "winston";

import { defaultArchitecture } from "../arch/architecture";
import { ArweaveGateway, Gateway } from "../arch/arweaveGateway";
import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { ObjectStore } from "../arch/objectStore";
import {
  batchingSize,
  dropBundleTxThresholdNumberOfBlocks,
  gatewayUrl,
  txPermanentThreshold,
} from "../constants";
import defaultLogger from "../logger";
import { PlannedDataItem } from "../types/dbTypes";
import { ByteCount, TransactionId } from "../types/types";
import {
  generateArrayChunks,
  getByteCountBasedRePackThresholdBlockCount,
} from "../utils/common";
import { DataItemsStillPendingWarning } from "../utils/errors";
import { getBundleTx, getS3ObjectStore } from "../utils/objectStoreUtils";

interface VerifyBundleJobArch {
  database?: Database;
  objectStore?: ObjectStore;
  arweaveGateway?: Gateway;
  logger?: winston.Logger;
  batchSize?: number;
}

async function hasBundleBeenPostedLongerThanTheDroppedThreshold(
  objectStore: ObjectStore,
  arweaveGateway: Gateway,
  bundleId: TransactionId,
  transactionByteCount?: ByteCount
): Promise<boolean> {
  const bundleTx = await getBundleTx(
    objectStore,
    bundleId,
    transactionByteCount
  );
  const txAnchor = bundleTx.last_tx;
  const blockHeightOfTxAnchor = await arweaveGateway.getBlockHeightForTxAnchor(
    txAnchor
  );

  const currentBlockHeight = await arweaveGateway.getCurrentBlockHeight();

  return (
    currentBlockHeight - blockHeightOfTxAnchor >
    dropBundleTxThresholdNumberOfBlocks
  );
}

export async function verifyBundleHandler({
  database = new PostgresDatabase(),
  objectStore = getS3ObjectStore(),
  arweaveGateway = new ArweaveGateway({ endpoint: gatewayUrl }),
  logger = defaultLogger.child({ job: "verify-bundle-job" }),
  batchSize = batchingSize,
}: VerifyBundleJobArch): Promise<void> {
  /**
   * NOTE: this locks DB items, but only for the duration of this query.
   * The primary intent is to prevent 2 concurrent executions competing for work.
   * */
  const seededBundles = await database.getSeededBundles();
  if (seededBundles.length === 0) {
    logger.info("No bundles to verify!");
    return;
  }

  for (const bundle of seededBundles) {
    const { planId, bundleId, transactionByteCount, payloadByteCount } = bundle;

    try {
      const transactionStatus = await arweaveGateway.getTransactionStatus(
        bundleId
      );

      if (transactionStatus.status !== "found") {
        if (
          await hasBundleBeenPostedLongerThanTheDroppedThreshold(
            objectStore,
            arweaveGateway,
            bundleId,
            transactionByteCount
          )
        ) {
          logger.warn("Updating bundle as dropped", {
            planId,
            bundleId,
          });
          await database.updateSeededBundleToDropped(planId, bundleId);
        }
      } else {
        // We found the bundle transaction from the arweaveGateway
        const { number_of_confirmations, block_height } =
          transactionStatus.transactionStatus;

        // Ensure bundle has the appropriate confirmations for the permanent threshold
        if (number_of_confirmations >= txPermanentThreshold) {
          const plannedDataItems =
            await database.getPlannedDataItemsForVerification(planId);

          const dataItemBatches = [
            ...generateArrayChunks(plannedDataItems, batchSize),
          ];

          // Start concurrent processes to check GQL for blocks and update data item batches to permanent
          let batchFailedUnexpectedly = false;
          let dataItemsStillPending = false;
          const parallelLimit = pLimit(10);
          const promises = dataItemBatches.map((batch) =>
            parallelLimit(() =>
              checkGQLForBlocksThenUpdateDataItemBatch(
                batch,
                arweaveGateway,
                database,
                bundleId,
                block_height,
                number_of_confirmations,
                payloadByteCount ?? 0,
                logger,
                planId
              ).catch((error) => {
                if (error instanceof DataItemsStillPendingWarning) {
                  dataItemsStillPending = true;
                  return;
                }

                batchFailedUnexpectedly = true;
                logger.error("Error verifying data item batch!", {
                  bundleId,
                  planId,
                  error,
                  dataItemIds: batch.map(({ dataItemId }) => dataItemId),
                });
              })
            )
          );
          await Promise.all(promises);

          if (batchFailedUnexpectedly) {
            logger.error(
              "Batch failed unexpectedly, skipping permanent insert so job will re-run",
              {
                bundleId,
                planId,
              }
            );
            continue;
          } else if (dataItemsStillPending) {
            logger.warn(
              "Some data items do not yet return block_heights, not yet marking bundle as permanent",
              {
                bundleId,
                planId,
              }
            );
            continue;
          }

          const isLastDataItemIndexedOnGQL = true; // all remaining data items are indexed on GQL
          logger.info("Updating bundle as permanent", {
            planId,
            block_height,
            isLastDataItemIndexedOnGQL,
          });
          await database.updateBundleAsPermanent(
            planId,
            block_height,
            isLastDataItemIndexedOnGQL
          );
        }
      }
    } catch (error) {
      logger.error("Error verifying bundle!", {
        bundle,
        error,
      });
    }
  }
}

export async function handler(eventPayload?: unknown) {
  defaultLogger.info(
    `Verify bundle job has been triggered with event payload:`,
    eventPayload
  );
  return verifyBundleHandler(defaultArchitecture);
}

async function checkGQLForBlocksThenUpdateDataItemBatch(
  dataItemBatch: PlannedDataItem[],
  arweaveGateway: Gateway,
  database: Database,
  bundleId: TransactionId,
  block_height: number,
  bundleTxConfirmations: number,
  payloadSize: ByteCount,
  logger: winston.Logger,
  planId: string
) {
  const dataItemGQLResults = await arweaveGateway.getDataItemsFromGQL(
    dataItemBatch.map(({ dataItemId }) => dataItemId)
  );

  const idsToPlannedDataItemsMap = dataItemBatch.reduce(
    (acc, plannedDataItem) => {
      acc[plannedDataItem.dataItemId] = plannedDataItem;
      return acc;
    },
    {} as Record<string, PlannedDataItem>
  );

  const idsToPlannedDataItemsInGQLMap = dataItemGQLResults.reduce(
    (acc, gqlResult) => {
      if (gqlResult.blockHeight) {
        acc[gqlResult.id] = idsToPlannedDataItemsMap[gqlResult.id];
      }
      return acc;
    },
    {} as Record<string, PlannedDataItem>
  );

  const dataItemsInGQL = Object.values(idsToPlannedDataItemsInGQLMap);
  const dataItemsNotInGQL =
    Object.keys(idsToPlannedDataItemsMap).length === dataItemsInGQL.length
      ? []
      : dataItemBatch.filter(
          (dataItem) =>
            idsToPlannedDataItemsInGQLMap[dataItem.dataItemId] === undefined
        );

  logger.debug("Updating data items as permanent", {
    bundleId,
    planId,
    block_height,
    dataItemIds: Object.keys(idsToPlannedDataItemsInGQLMap),
  });
  await database.updateDataItemsAsPermanent({
    dataItemIds: Object.keys(idsToPlannedDataItemsInGQLMap),
    blockHeight: block_height,
    bundleId,
  });
  logger.debug("Updated data items as permanent", {
    bundleId,
    planId,
    block_height,
    dataItemIds: Object.keys(idsToPlannedDataItemsInGQLMap),
  });

  if (dataItemsNotInGQL.length > 0) {
    const notFoundDataItemIds = dataItemsNotInGQL.map(
      ({ dataItemId }) => dataItemId
    );

    const byteCountBasedRepackThresholdBlockCount =
      getByteCountBasedRePackThresholdBlockCount(payloadSize);

    if (bundleTxConfirmations < byteCountBasedRepackThresholdBlockCount) {
      logger.warn(
        "Data items not found on GQL, but data posted within repack threshold... not yet repacking data items, will continue processing",
        {
          bundleTxConfirmations,
          rePackThresholdBlockCount: byteCountBasedRepackThresholdBlockCount,
          bundleId,
          planId,
          block_height,
          notFoundDataItemIds,
        }
      );
      throw new DataItemsStillPendingWarning();
    }

    logger.error("Mismatched data item count!", {
      bundleId,
      planId,
      foundDataItemLength: dataItemsInGQL.length,
      notFoundDataItemLength: notFoundDataItemIds.length,
      notFoundDataItemIds,
    });

    await database.updateDataItemsToBeRePacked(notFoundDataItemIds, bundleId);
  }
}
