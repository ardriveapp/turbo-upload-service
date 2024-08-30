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
import { randomUUID } from "crypto";
import pLimit from "p-limit";

import { defaultArchitecture } from "../arch/architecture";
import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { enqueue } from "../arch/queues";
import { BundlePacker, PackerBundlePlan } from "../bundles/bundlePacker";
import { dedicatedBundleTypes, jobLabels } from "../constants";
import defaultLogger from "../logger";
import { NewDataItem } from "../types/dbTypes";
import { generateArrayChunks } from "../utils/common";
import { factorBundlesByTargetSize } from "../utils/planningUtils";

const PARALLEL_LIMIT = 5;
export async function planBundleHandler(
  database: Database = new PostgresDatabase(),
  bundlePacker: BundlePacker = new BundlePacker({}),
  logger = defaultLogger.child({ job: "plan-bundle-job" })
) {
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
      firstDataItemId: underweightBundlePlan.dataItemIds[0],
    });
  });

  // Expedite the plans containing overdue data item
  overdueBundlePlans.forEach((overdueBundlePlan) => {
    logger.debug(`Expediting bundle plan due to overdue data item.`, {
      firstDataItemId: overdueBundlePlan.dataItemIds[0],
    });
    bundlePlans.push(overdueBundlePlan);
  });

  const parallelLimit = pLimit(PARALLEL_LIMIT);
  const insertPromises = bundlePlans.map(({ dataItemIds, totalByteCount }) =>
    parallelLimit(async () => {
      const planId = randomUUID();
      const logBatchSize = 100;
      const dataItemIdBatches = generateArrayChunks(dataItemIds, logBatchSize);
      const numDataItemIdBatches = Math.ceil(dataItemIds.length / logBatchSize);
      let batchNum = 1;
      for (const batch of dataItemIdBatches) {
        logger.info("Plan:", {
          planId,
          dataItemIds: batch,
          totalByteCount,
          numDataItems: dataItemIds.length,
          logBatch: `${batchNum++}/${numDataItemIdBatches}`,
        });
      }
      await database.insertBundlePlan(planId, dataItemIds);
      await enqueue(jobLabels.prepareBundle, { planId });
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
  defaultLogger.info("Plan bundle GO!");
  defaultLogger.debug("Plan bundle event payload:", {
    eventPayload,
  });
  return planBundleHandler(defaultArchitecture.database);
}
