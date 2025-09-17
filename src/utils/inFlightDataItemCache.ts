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
import { EphemeralCache } from "@alexsasharegan/simple-cache";
import { ReadThroughPromiseCache } from "@ardrive/ardrive-promise-cache";
import winston from "winston";

import { CacheService } from "../arch/cacheServiceTypes";
import { getElasticacheService } from "../arch/elasticacheService";
import globalLogger from "../logger";
import { MetricRegistry } from "../metricRegistry";
import { TransactionId } from "../types/types";
import { normalizeCacheError } from "../utils/cacheServiceUtils";
import { breakerForCache } from "./cacheServiceUtils";

/**
 * In-flight Data Item Cache:
 *
 * This is a caching system that is used to track in-flight data items: those
 * whose bytes are currently being ingested by the system. It will NOT track
 * data items that are in the process of being bundled. This utility helps to
 * prevent edge cases that might arise when concurrent uploads are attempting
 * to write data for the same data item.
 *
 * The preferred source of truth for this cache is Elasticache. If Elasticache
 * is unavailable or sufficiently degraded, a circuit breaker will allow for
 * fallback to an in-memory cache. This trades off cluster-wide consistency
 * for uptime, which is viewed as a reasonable trade-off for this use case.
 *
 * A ReadThroughPromiseCache is used for cache gets in order to provide for
 * barrier synchronization across concurrent requests for the same data item.
 */

const elasticacheInFlightPrefix = "if_";
function getElasticacheInFlightKey(dataItemId: TransactionId) {
  return `${elasticacheInFlightPrefix}{${dataItemId}}`;
}
const inFlightTtlSeconds = +(process.env.IN_FLIGHT_DATA_ITEM_TTL_SECS ?? 60); // Block attempted duplicates for 1 minute

// In-memory cache for fallback scenarios
const backupCache = EphemeralCache<TransactionId, boolean>(
  1000,
  inFlightTtlSeconds * 1_000
);

const inFlightDataItemCache = new ReadThroughPromiseCache<
  TransactionId,
  boolean,
  {
    cacheService: CacheService;
    logger: winston.Logger;
  }
>({
  cacheParams: {
    cacheCapacity: 10000,
    cacheTTLMillis: inFlightTtlSeconds * 1000, // secs -> ms
  },
  metricsConfig: {
    cacheName: "in_flight_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV || "local",
    },
  },
  readThroughFunction: async (dataItemId, { cacheService, logger }) => {
    logger.debug(
      `READTHROUGH: Checking whether data item ${dataItemId} is in-flight...`
    );
    const fireResult: boolean = await breakerForCache(cacheService)
      .fire(async () => {
        logger.debug(
          `REMOTE: Checking whether data item ${dataItemId} is in-flight...`
        );
        const val = await cacheService.get(
          getElasticacheInFlightKey(dataItemId)
        );
        logger.debug(
          `REMOTE: In-flight data item ${dataItemId} check result: ${val}`
        );
        return val !== null;
      })
      .then((exists) => {
        // Write back to the local cache if necessary
        if (backupCache.get(dataItemId) === undefined) {
          backupCache.write(dataItemId, exists);
        }
        return exists;
      })
      .catch((error) => {
        // Simulate fallback capability of circuit breaker
        // NB: It's a shared circuit breaker so can't use this as its general fallback
        logger.error(
          `Falling back to in-memory cache for in-flight data item ${dataItemId}...`,
          { error: normalizeCacheError(error) }
        );
        return backupCache.read(dataItemId) ?? false;
      });
    logger.debug(
      `READTHROUGH: In-flight data item ${dataItemId} check result: ${fireResult}`
    );
    return fireResult;
  },
});

export async function markInFlight({
  dataItemId,
  cacheService = getElasticacheService(),
  logger = globalLogger,
}: {
  dataItemId: TransactionId;
  cacheService: CacheService;
  logger: winston.Logger;
}): Promise<void> {
  backupCache.write(dataItemId, true);
  await inFlightDataItemCache.put(
    dataItemId,
    (async () => {
      // Write through to Cache Service with "1" as presence flag
      const result = await breakerForCache(cacheService)
        .fire(() => {
          logger.debug(
            `REMOTE: Marking data item ${dataItemId} as in-flight...`
          );
          return cacheService.set(
            getElasticacheInFlightKey(dataItemId),
            "1",
            "EX",
            inFlightTtlSeconds,
            "NX"
          );
        })
        .catch((error) => {
          // An error here indicates a failure to write to Elasticache, but
          // NOT an indication that the NX-write failed due to key existence
          // (null is returned in that case). Therefore, fail gracefully here
          // to allow for the promise cache to be used locally.
          logger.error(
            `Error while marking data item with ID ${dataItemId} as in-flight!`,
            { error: normalizeCacheError(error) }
          );
          return "OK";
        });

      // An OK result means that the data item was not already in-flight
      if (result !== "OK") {
        backupCache.write(dataItemId, true);
        // This results in the promise cache clearing the cache entry,
        // which is fine since a subsequent request for the same ID
        // will result in a cache miss and a read-through back to Elasticache
        throw new Error(
          `Data item with ID ${dataItemId} already marked as in-flight!`
        );
      }

      return true;
    })()
  );
}

export async function removeFromInFlight({
  dataItemId,
  cacheService = getElasticacheService(),
  logger = globalLogger,
}: {
  dataItemId: TransactionId;
  cacheService: CacheService;
  logger: winston.Logger;
}): Promise<void> {
  backupCache.remove(dataItemId);
  try {
    inFlightDataItemCache.remove(dataItemId);
    await breakerForCache(cacheService).fire(() => {
      logger.debug(
        `REMOTE: Marking data item ${dataItemId} as NOT in-flight...`
      );
      return cacheService.del(getElasticacheInFlightKey(dataItemId));
    });
  } catch (error) {
    // The local and remote cache TTLs are sufficiently short that this will be ok in most cases
    logger.error(
      `Error while marking data item with ID ${dataItemId} as NOT in-flight!`,
      { error: normalizeCacheError(error) }
    );
  }
}

export async function dataItemIsInFlight({
  dataItemId,
  cacheService = getElasticacheService(),
  logger = globalLogger,
}: {
  dataItemId: TransactionId;
  cacheService: CacheService;
  logger: winston.Logger;
}): Promise<boolean> {
  return inFlightDataItemCache
    .get(dataItemId, { cacheService, logger })
    .catch((error) => {
      logger.warn(
        `Error while checking if data item with ID ${dataItemId} is in-flight!`,
        { error: normalizeCacheError(error) }
      );
      // Err on the side of permissiveness if the cache is down
      return false;
    });
}
