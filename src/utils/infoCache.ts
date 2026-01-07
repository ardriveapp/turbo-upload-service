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
import { ReadThroughPromiseCache } from "@ardrive/ardrive-promise-cache";
import winston from "winston";

import { Database } from "../arch/db/database";
import { MetricRegistry } from "../metricRegistry";
import { DataItemInfo, TransactionId } from "../types/types";

// Caches hits and misses from the db to protect db from spamming
export const dataItemInfoCache = new ReadThroughPromiseCache<
  TransactionId,
  DataItemInfo | undefined,
  { database: Database; logger: winston.Logger }
>({
  cacheParams: {
    cacheCapacity: 10_000,
    cacheTTLMillis: 15_000,
  },
  metricsConfig: {
    cacheName: "status_info_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV || "local",
    },
  },
  readThroughFunction: async (dataItemId, { database, logger }) => {
    try {
      return await database.getDataItemInfo(dataItemId);
    } catch (error) {
      logger.error(`Failed to fetch info for ${dataItemId} from database!`);
      throw error;
    }
  },
});
