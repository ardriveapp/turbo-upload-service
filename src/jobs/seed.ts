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
import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { ObjectStore } from "../arch/objectStore";
import { createQueueHandler } from "../arch/queues";
import { ArweaveInterface } from "../arweaveJs";
import logger from "../logger";
import { PlanId } from "../types/dbTypes";
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
  }: SeedBundleJobInjectableArch
): Promise<void> {
  logger.child({ job: "seed-bundle-job" });

  const dbResult = await database.getNextBundleAndDataItemsToSeedByPlanId(
    planId
  );
  const { bundleToSeed, dataItemsToSeed } = dbResult;
  const { bundleId, transactionByteCount, headerByteCount, payloadByteCount } =
    bundleToSeed;

  logger.info("Getting transaction to seed...", {
    bundleToSeed,
    dataItemsToSeed,
  });
  const bundleTx = await getBundleTx(
    objectStore,
    bundleId,
    transactionByteCount
  );

  logger.info("Retrieved bundle tx : ", { bundleTx });

  try {
    await arweave.uploadChunksFromPayloadStream(
      () =>
        getBundlePayload(
          objectStore,
          planId,
          headerByteCount,
          payloadByteCount
        ),
      bundleTx
    );
  } catch (error) {
    logger.error("Error when uploading chunks: ", { error, bundleToSeed });
    throw error;
  }

  logger.info("Finished uploading chunks.", { bundleToSeed });

  await database.insertSeededBundle(bundleId);
}

export const handler = createQueueHandler(
  "seed-bundle",
  (message: { planId: PlanId }) => seedBundleHandler(message.planId, {}),
  {
    before: async () => {
      logger.info("Seed bundle job has been triggered.");
    },
  }
);
