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
import { defaultArchitecture } from "../arch/architecture";
import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { ObjectStore } from "../arch/objectStore";
import { createQueueHandler } from "../arch/queues";
import { ArweaveInterface } from "../arweaveJs";
import { jobLabels } from "../constants";
import defaultLogger from "../logger";
import { PlanId, PlannedDataItem, PostedBundle } from "../types/dbTypes";
import { filterKeysFromObject } from "../utils/common";
import { BundleAlreadySeededWarning } from "../utils/errors";
import {
  getBundlePayload,
  getBundleTx,
  getS3ObjectStore,
} from "../utils/objectStoreUtils";

interface SeedBundleJobInjectableArch {
  database?: Database;
  objectStore?: ObjectStore;
  arweave?: ArweaveInterface;
}

export async function seedBundleHandler(
  planId: PlanId,
  {
    database = new PostgresDatabase(),
    objectStore = getS3ObjectStore(),
    arweave = new ArweaveInterface(),
  }: SeedBundleJobInjectableArch,
  logger = defaultLogger.child({ job: "seed-bundle-job", planId })
): Promise<void> {
  let dbResult: {
    bundleToSeed: PostedBundle;
    dataItemsToSeed: PlannedDataItem[];
  };

  try {
    dbResult = await database.getNextBundleAndDataItemsToSeedByPlanId(planId);
  } catch (error) {
    if (error instanceof BundleAlreadySeededWarning) {
      logger.warn(error.message);
      return;
    }
    throw error;
  }

  const { bundleToSeed, dataItemsToSeed } = dbResult;
  const { bundleId, transactionByteCount } = bundleToSeed;

  logger.info("Getting transaction to seed...", {
    bundleToSeed,
    dataItemCount: dataItemsToSeed.length,
  });
  const bundleTx = await getBundleTx(
    objectStore,
    bundleId,
    transactionByteCount
  );

  logger.info("Retrieved bundle tx : ", {
    bundleTx: filterKeysFromObject(bundleTx, [
      "data",
      "chunks",
      "owner",
      "tags",
    ]),
  });

  try {
    await arweave.uploadChunksFromPayloadStream(
      () => getBundlePayload(objectStore, planId),
      bundleTx
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error.";
    logger.error("Error when uploading chunks: ", {
      error: message,
      bundleToSeed,
    });
    throw error;
  }

  logger.info("Finished uploading chunks.", { bundleToSeed });

  await database.insertSeededBundle(bundleId);
}

export const handler = createQueueHandler(
  jobLabels.seedBundle,
  (message: { planId: PlanId }) =>
    seedBundleHandler(message.planId, defaultArchitecture),
  {
    before: async () => {
      defaultLogger.info("Seed bundle job has been triggered.");
    },
  }
);
