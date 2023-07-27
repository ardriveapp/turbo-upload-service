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
import { randomUUID } from "crypto";
import pLimit from "p-limit";

import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { enqueue } from "../arch/queues";
import { BundlePacker } from "../bundles/bundlePacker";
import logger from "../logger";

const PARALLEL_LIMIT = 5;
export async function planBundleHandler(
  database: Database = new PostgresDatabase(),
  bundlePacker: BundlePacker = new BundlePacker({})
) {
  logger.child({ job: "plan-bundle-job" });

  /**
   * NOTE: this locks DB items, but only for the duration of this query.
   * The primary intent is to prevent 2 concurrent executions competing for work.
   * */
  const dbDataItems = await database.getNewDataItems();

  if (dbDataItems.length === 0) {
    logger.info("No data items to bundle!");
    return;
  }

  logger.info("Planning data items.", {
    dataItems: dbDataItems,
  });
  const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans(dbDataItems);

  const parallelLimit = pLimit(PARALLEL_LIMIT);
  const insertPromises = bundlePlans.map(({ dataItemIds }) =>
    parallelLimit(async () => {
      const planId = randomUUID();
      await database.insertBundlePlan(planId, dataItemIds);
      await enqueue("prepare-bundle", { planId });
    })
  );

  try {
    await Promise.all(insertPromises);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
  } catch (error: any) {
    logger.error("Bundle plan insert has failed!", {
      message: error.message,
    });
    throw error;
  }
}

export async function handler(eventPayload?: unknown) {
  logger.info("Plan bundle job has been triggered with event payload", {
    event: eventPayload,
  });
  return planBundleHandler();
}
