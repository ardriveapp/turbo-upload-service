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

import { defaultArchitecture } from "../arch/architecture";
import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { enqueue } from "../arch/queues";
import { BundlePacker, PackerBundlePlan } from "../bundles/bundlePacker";
import { dedicatedBundleTypes } from "../constants";
import defaultLogger from "../logger";
import { NewDataItem } from "../types/dbTypes";
import { factorBundlesByTargetSize } from "../utils/planningUtils";

const PARALLEL_LIMIT = 5;
export async function planBundleHandler(
  database: Database = new PostgresDatabase(),
  bundlePacker: BundlePacker = new BundlePacker({}),
  logger = defaultLogger.child({ job: "plan-bundle-job" })
) {
  /**
   * NOTE: this locks DB items, but only for the duration of this query.
   * The primary intent is to prevent 2 concurrent executions competing for work.
   * */
  const dbDataItems = await database.getNewDataItems();

  if (dbDataItems.length === 0) {
    logger.info("No data items to bundle!");
    return;
  }

  const splitDataItemsByFeatureType = dbDataItems.reduce(
    (acc, dataItem) => {
      const premiumFeatureType = dataItem.premiumFeatureType;
      if (Object.keys(dedicatedBundleTypes).includes(premiumFeatureType)) {
        acc[premiumFeatureType]
          ? acc[premiumFeatureType].push(dataItem)
          : (acc[premiumFeatureType] = [dataItem]);
      } else {
        acc["default"].push(dataItem);
      }
      return acc;
    },
    { default: [] } as Record<string, NewDataItem[]>
  );

  logger.info("Planning data items.", {
    dataItemCount: dbDataItems.length,
  });

  const allBundlePlans: PackerBundlePlan[] = [];
  for (const featureType in splitDataItemsByFeatureType) {
    const dataItems = splitDataItemsByFeatureType[featureType];
    const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans(dataItems);
    allBundlePlans.push(...bundlePlans);
  }

  // Separate out the plans that contain overdue data items for expedited preparation
  const { overdueBundlePlans, onTimeBundlePlans } = allBundlePlans.reduce(
    (acc, bundlePlan) => {
      if (bundlePlan.containsOverdueDataItems) {
        acc.overdueBundlePlans.push(bundlePlan);
      } else {
        acc.onTimeBundlePlans.push(bundlePlan);
      }
      return acc;
    },
    {
      overdueBundlePlans: new Array<PackerBundlePlan>(),
      onTimeBundlePlans: new Array<PackerBundlePlan>(),
    }
  );

  // Separate out the plans that aren't the target size
  const { underweightBundlePlans, bundlePlans } = factorBundlesByTargetSize(
    onTimeBundlePlans,
    bundlePacker
  );

  underweightBundlePlans.forEach((underweightBundlePlan) => {
    logger.info(`Not sending under-packed bundle plan for preparation.`, {
      underweightBundlePlan,
    });
  });

  // Expedite the plans containing overdue data item
  overdueBundlePlans.forEach((overdueBundlePlan) => {
    logger.info(`Expediting bundle plan due to overdue data item.`, {
      overdueBundlePlan,
    });
    bundlePlans.push(overdueBundlePlan);
  });

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
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error";
    logger.error("Bundle plan insert has failed!", {
      error: message,
    });
    throw error;
  }
}

export async function handler(eventPayload?: unknown) {
  defaultLogger.info("Plan bundle job has been triggered with event payload", {
    event: eventPayload,
  });
  return planBundleHandler(defaultArchitecture.database);
}
