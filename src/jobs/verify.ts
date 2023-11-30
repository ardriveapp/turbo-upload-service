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
import winston from "winston";

import { defaultArchitecture } from "../arch/architecture";
import { ArweaveGateway, Gateway } from "../arch/arweaveGateway";
import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { ObjectStore } from "../arch/objectStore";
import {
  dropTxThresholdNumberOfBlocks,
  gatewayUrl,
  txPermanentThreshold,
} from "../constants";
import defaultLogger from "../logger";
import { ByteCount, TransactionId } from "../types/types";
import { getBundleTx, getS3ObjectStore } from "../utils/objectStoreUtils";

interface VerifyBundleJobArch {
  database?: Database;
  objectStore?: ObjectStore;
  gateway?: Gateway;
  logger?: winston.Logger;
}

async function hasBundleBeenPostedLongerThanTheDroppedThreshold(
  objectStore: ObjectStore,
  gateway: Gateway,
  bundleId: TransactionId,
  transactionByteCount?: ByteCount
): Promise<boolean> {
  const bundleTx = await getBundleTx(
    objectStore,
    bundleId,
    transactionByteCount
  );
  const txAnchor = bundleTx.last_tx;
  const blockHeightOfTxAnchor = await gateway.getBlockHeightForTxAnchor(
    txAnchor
  );

  const currentBlockHeight = await gateway.getCurrentBlockHeight();

  return (
    currentBlockHeight - blockHeightOfTxAnchor > dropTxThresholdNumberOfBlocks
  );
}

export async function verifyBundleHandler({
  database = new PostgresDatabase(),
  objectStore = getS3ObjectStore(),
  gateway = new ArweaveGateway({ endpoint: gatewayUrl }),
  logger = defaultLogger.child({ job: "verify-bundle-job" }),
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
    const { planId, bundleId, transactionByteCount } = bundle;

    try {
      const transactionStatus = await gateway.getTransactionStatus(bundleId);

      if (transactionStatus.status !== "found") {
        if (
          await hasBundleBeenPostedLongerThanTheDroppedThreshold(
            objectStore,
            gateway,
            bundleId,
            transactionByteCount
          )
        ) {
          await database.updateBundleAsDropped(planId);
        }
      } else {
        const { number_of_confirmations, block_height } =
          transactionStatus.transactionStatus;
        if (number_of_confirmations >= txPermanentThreshold) {
          const lastDataItem = await database.getLastDataItemInBundle(planId);
          const isLastDataItemIndexedOnGQL =
            await gateway.isTransactionQueryableOnGQL(lastDataItem.dataItemId);

          await database.updateBundleAsPermanent(
            planId,
            block_height,
            isLastDataItemIndexedOnGQL
          );
        }
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : "Unknown error";
      logger.error("Error verifying bundle!", {
        bundle,
        error: message,
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
