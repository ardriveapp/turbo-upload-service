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
import CircuitBreaker from "opossum";
import { Readable } from "stream";
import winston from "winston";

import { CacheService, CacheServiceError } from "../arch/cacheServiceTypes";
import { getConfigValue } from "../arch/remoteConfig";
import globalLogger from "../logger";
import {
  MetricRegistry,
  setUpCircuitBreakerListenerMetrics,
} from "../metricRegistry";
import { PayloadInfo, TransactionId } from "../types/types";
import { deserializePayloadInfo, minifyNestedDataItemInfo } from "./common";
import { Deferred } from "./deferred";
import { waitForStreamToEnd } from "./streamUtils";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const calculateSlot = require("cluster-key-slot");

const quarantinedSmallDataItemTTLSecs = +(
  process.env.QUARANTINED_SMALL_DATAITEM_TTL_SECS || 432000
); // 5 days by default

// A helper type that will allow us to pass around closures involving CacheService activities
type CacheServiceTask<T> = () => Promise<T>;

// In the future we may have multiple cache services, so we use a WeakMap to store
// the circuit breaker for each service. WeakMap allows for object keys.
const cacheBreakers = new WeakMap<
  CacheService,
  {
    fire<T>(task: CacheServiceTask<T>): Promise<T>;
    breaker: CircuitBreaker<[CacheServiceTask<unknown>], unknown>;
  }
>();

/**
 * Lazily instantiates and returns a circuit breaker for the given cache service.
 *
 * By wrapping calls to any of the usual CacheService methods in their respective
 * circuit breaker's fire() method, we can share the circuit breaker across the
 * fully variety of CacheService methods while maintaining type safety.
 *
 * IMPORTANT: calls to fire() MUST be passed an `async` function OR return a Promise.
 * If not, the opossum's features related to error management will not work as expected.
 */
export function breakerForCache(cache: CacheService): {
  fire<T>(task: CacheServiceTask<T>): Promise<T>;
  breaker: CircuitBreaker<[CacheServiceTask<unknown>], unknown>;
} {
  const existing = cacheBreakers.get(cache);
  if (existing) return existing;

  // Use a rest parameter to indicate that the argument is a tuple
  const breaker = new CircuitBreaker<[CacheServiceTask<unknown>], unknown>(
    async (...args: [CacheServiceTask<unknown>]) => {
      if (cache.status !== "ready") {
        throw new Error(`Cache service is not ready! Status: ${cache.status}`);
      }
      const [task] = args;
      return task();
    },
    {
      timeout: process.env.NODE_ENV === "local" ? 10_000 : 3000,
      errorThresholdPercentage: 10,
      resetTimeout: 30000,
    }
  );

  // TODO: Generalize the breaker names further as we add other cache services
  setUpCircuitBreakerListenerMetrics("elasticache", breaker, globalLogger);
  breaker.on("timeout", () =>
    globalLogger.error("Elasticache circuit breaker command timed out")
  );

  // This wrapper accomplishes two important things:
  // 1. It allows us to get type-safe returns for the task function passed to fire()
  // 2. It provides access to the breaker itself for external use cases
  const wrapper = {
    fire<T>(task: CacheServiceTask<T>): Promise<T> {
      return breaker.fire(task) as Promise<T>;
    },
    breaker,
  };

  cacheBreakers.set(cache, wrapper);
  return wrapper;
}

export function cacheServiceIsAvailable(cacheService: CacheService): boolean {
  return !breakerForCache(cacheService).breaker.opened;
}

function cacheKeyForRawDataItem({
  dataItemId,
  quarantine = false,
}: {
  dataItemId: TransactionId;
  quarantine?: boolean;
}): string {
  return `${quarantine ? "quarantine_" : ""}raw_{${dataItemId}}`;
}

function cacheKeyForMetadata({
  dataItemId,
  quarantine = false,
}: {
  dataItemId: TransactionId;
  quarantine?: boolean;
}): string {
  return `${quarantine ? "quarantine_" : ""}metadata_{${dataItemId}}`;
}

const smallDataItemCache = new ReadThroughPromiseCache<
  TransactionId,
  Buffer,
  CacheService
>({
  cacheParams: {
    cacheCapacity: 1000,
    cacheTTLMillis: 60_000,
  },
  readThroughFunction: async (dataItemId, cacheService) => {
    const rawData = await breakerForCache(cacheService).fire(() =>
      cacheService.getBuffer(cacheKeyForRawDataItem({ dataItemId }))
    );

    if (!rawData) {
      throw new CacheServiceError(
        `Cached raw data item with ID ${dataItemId} not found!`
      );
    }

    return rawData;
  },
  metricsConfig: {
    cacheName: "small_item_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV ?? "local",
    },
  },
});

const metadataCache = new ReadThroughPromiseCache<
  TransactionId,
  PayloadInfo,
  CacheService
>({
  cacheParams: {
    cacheCapacity: 1000,
    cacheTTLMillis: 60_000,
  },
  readThroughFunction: async (dataItemId, cacheService) => {
    const metadata = await breakerForCache(cacheService).fire(() => {
      return cacheService.get(cacheKeyForMetadata({ dataItemId }));
    });

    if (!metadata) {
      throw new CacheServiceError(
        `Cached metadata for data item with ID ${dataItemId} not found!`
      );
    }

    return deserializePayloadInfo(metadata);
  },
  metricsConfig: {
    cacheName: "small_metadata_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV ?? "local",
    },
  },
});

const smallDataItemExistsCache = new ReadThroughPromiseCache<
  TransactionId,
  boolean,
  CacheService
>({
  cacheParams: {
    cacheCapacity: 1000,
    cacheTTLMillis: 60_000,
  },
  readThroughFunction: async (dataItemId, cacheService) => {
    const existsCount = await breakerForCache(cacheService).fire(() => {
      return cacheService.exists(
        cacheKeyForRawDataItem({ dataItemId }),
        cacheKeyForMetadata({ dataItemId })
      );
    });
    return existsCount === 2;
  },
  metricsConfig: {
    cacheName: "small_item_exists_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV ?? "local",
    },
  },
});

function serializePayloadInfo(payloadInfo: PayloadInfo): string {
  return `${payloadInfo.payloadContentType};${payloadInfo.payloadDataStart}`;
}

export async function cacheSmallDataItem({
  cacheService,
  smallDataItemStream,
  dataItemId,
  rawContentLength,
  payloadContentType,
  payloadDataStart,
  logger,
  deferredIsValid,
}: {
  cacheService: CacheService;
  smallDataItemStream: Readable;
  dataItemId: TransactionId;
  rawContentLength: number;
  payloadContentType: string;
  payloadDataStart: number;
  logger: winston.Logger;
  deferredIsValid: Deferred<boolean>;
}): Promise<void> {
  const smallDataItemBuffer = Buffer.alloc(rawContentLength);
  let offset = 0;

  smallDataItemStream.on("data", (chunk: Buffer) => {
    chunk.copy(smallDataItemBuffer, offset);
    offset += chunk.length;
  });

  // Await the end or error of the stream
  try {
    await waitForStreamToEnd(smallDataItemStream);
  } catch (streamErr) {
    logger.error(`Stream error while caching small data item`, streamErr);
    throw streamErr;
  }

  // Validate size
  if (offset !== rawContentLength) {
    const err = new Error(
      `Data length mismatch (${offset} !== ${rawContentLength}) for small data item with ID ${dataItemId}`
    );
    logger.error(err.message);
    throw err;
  }

  const isValid = await deferredIsValid.promise;

  if (!isValid) {
    logger.error(
      `Data item with ID ${dataItemId} failed validation, not storing in cache`
    );
    return;
  }

  try {
    logger.debug(
      `Storing raw data and metadata for small data item ${dataItemId}...`,
      {
        rawContentLength,
        payloadContentType,
        payloadDataStart,
      }
    );
    const smallDataItemTTLSecs = await getConfigValue(
      "cacheWriteDataItemTtlSecs"
    );
    const results = await breakerForCache(cacheService).fire(async () => {
      try {
        const res = await cacheService
          .multi()
          .set(
            cacheKeyForRawDataItem({ dataItemId }),
            smallDataItemBuffer,
            "EX",
            smallDataItemTTLSecs
          )
          .set(
            cacheKeyForMetadata({ dataItemId }),
            serializePayloadInfo({
              payloadContentType,
              payloadDataStart,
            }),
            "EX",
            smallDataItemTTLSecs
          )
          .exec();
        return res;
      } catch (err) {
        logger.error(`Error while storing data item ${dataItemId}`, {
          error: normalizeCacheError(err),
        });
        throw err;
      }
    });

    const failed = results?.find(([err]) => err);
    if (failed) {
      throw failed[0];
    }

    logger.debug(
      `Stored raw data and metadata for small data item ${dataItemId} in Elasticache.`,
      {
        rawContentLength,
        payloadContentType,
        payloadDataStart,
      }
    );
  } catch (error) {
    logger.error(
      `Failed to store raw data and metadata for ${dataItemId} in Elasticache`,
      { error: normalizeCacheError(error) }
    );
    throw new CacheServiceError(
      `Failed to store ${dataItemId} in Elasticache`,
      error
    );
  }
}

export async function cacheHasDataItem({
  cacheService,
  dataItemId,
  logger,
}: {
  cacheService: CacheService;
  dataItemId: TransactionId;
  logger: winston.Logger;
}): Promise<boolean> {
  return smallDataItemExistsCache
    .get(dataItemId, cacheService)
    .catch((error) => {
      logger.error(
        `Error while checking if data item with ID ${dataItemId} exists in cache!`,
        { error: normalizeCacheError(error) }
      );
      return false;
    });
}

export async function cachedDataItemMetadata(
  cacheService: CacheService,
  dataItemId: TransactionId
): Promise<PayloadInfo> {
  return metadataCache.get(dataItemId, cacheService);
}

export function cachedRawDataItem(
  cacheService: CacheService,
  dataItemId: TransactionId
): Promise<Buffer> {
  return smallDataItemCache.get(dataItemId, cacheService);
}

export async function cachedDataItemReadableRange({
  cacheService,
  dataItemId,
  startOffset,
  endOffsetInclusive,
}: {
  cacheService: CacheService;
  dataItemId: TransactionId;
  startOffset?: number;
  endOffsetInclusive?: number;
}): Promise<{ readable: Readable }> {
  return cachedRawDataItem(cacheService, dataItemId).then((rawData) => {
    return {
      readable: Readable.from(
        (startOffset ?? endOffsetInclusive) !== undefined
          ? rawData.subarray(
              startOffset || 0,
              endOffsetInclusive !== undefined
                ? endOffsetInclusive + 1 // subarray uses exclusive end index
                : undefined
            )
          : rawData
      ),
    };
  });
}

export async function removeDataItemsFromCache(
  cacheService: CacheService,
  dataItemIds: TransactionId[],
  logger: winston.Logger
): Promise<number> {
  const isClustered = cacheService.isCluster;
  const cacheKeys = dataItemIds.reduce((acc, dataItemId) => {
    // Prepare cache keys for raw data item and metadata
    acc.push(cacheKeyForRawDataItem({ dataItemId }));
    acc.push(cacheKeyForMetadata({ dataItemId }));

    // Remove data item from local caches
    smallDataItemCache.remove(dataItemId);
    metadataCache.remove(dataItemId);
    smallDataItemExistsCache.remove(dataItemId);
    return acc;
  }, [] as string[]);

  // Group cache keys by slot if clustered
  const groupedKeys = new Map<number, string[]>();
  if (isClustered) {
    for (const cacheKey of cacheKeys) {
      const slot = calculateSlot(cacheKey);
      if (!groupedKeys.has(slot)) groupedKeys.set(slot, []);
      groupedKeys.get(slot)?.push(cacheKey);
    }
  } else {
    groupedKeys.set(0, cacheKeys);
  }
  logger.debug(
    `Grouped ${cacheKeys.length} cache keys into ${groupedKeys.size} slot groups`
  );

  let totalDeleted = 0;
  const errors: unknown[] = [];
  if (groupedKeys.size > 0) {
    for (const keys of groupedKeys.values()) {
      totalDeleted += await breakerForCache(cacheService).fire(() =>
        cacheService.del(...keys).catch((err) => {
          // Log errors and keep trying to delete other keys
          logger.error(`Error while deleting keys from Elasticache`, {
            error: normalizeCacheError(err),
          });
          errors.push(err);
          return 0;
        })
      );
    }
    // Now throw, potentially impacting circuit breaker
    if (errors.length > 0) {
      throw new CacheServiceError(
        `At least one error occurred while deleting keys`,
        {
          errors,
        }
      );
    }
  }

  return totalDeleted;
}

export async function quarantineCachedDataItem(
  cacheService: CacheService,
  dataItemId: TransactionId,
  logger: winston.Logger
): Promise<void> {
  if (await cacheHasDataItem({ cacheService, dataItemId, logger })) {
    logger.info(`Quarantining data item ${dataItemId} in Elasticache...`);
    try {
      const rawDataItemBuffer = await cachedRawDataItem(
        cacheService,
        dataItemId
      );
      if (rawDataItemBuffer) {
        await breakerForCache(cacheService).fire(() => {
          return cacheService
            .multi()
            .set(
              cacheKeyForRawDataItem({
                dataItemId,
                quarantine: true,
              }),
              rawDataItemBuffer,
              "EX",
              quarantinedSmallDataItemTTLSecs
            )
            .del(cacheKeyForRawDataItem({ dataItemId }))
            .exec();
        });

        MetricRegistry.cacheQuarantineSuccess.inc();
      }
    } catch (error) {
      logger.error(
        `Failed to quarantine data item ${dataItemId} in Elasticache`,
        { error: normalizeCacheError(error) }
      );
      MetricRegistry.cacheQuarantineFailure.inc();
    }

    try {
      const metadata = await cachedDataItemMetadata(cacheService, dataItemId);
      if (metadata) {
        await breakerForCache(cacheService).fire(() => {
          return cacheService
            .multi()
            .set(
              cacheKeyForMetadata({ dataItemId, quarantine: true }),
              serializePayloadInfo(metadata),
              "EX",
              quarantinedSmallDataItemTTLSecs
            )
            .del(cacheKeyForMetadata({ dataItemId }))
            .exec();
        });
      }
    } catch (error) {
      logger.error(
        `Failed to quarantine metadata for data item ${dataItemId} in Elasticache`,
        { error: normalizeCacheError(error) }
      );
    }
  } else {
    logger.info(
      `Quarantine not necessary for data item ${dataItemId} in Elasticache`
    );
  }

  // Purge local caches
  for (const [cacheName, cache] of Object.entries({
    smallDataItemCache,
    metadataCache,
    smallDataItemExistsCache,
  })) {
    try {
      cache.remove(dataItemId);
    } catch (error) {
      logger.error(
        `Failed to remove data item ${dataItemId} from ${cacheName}`,
        { error }
      );
    }
  }
}

export async function cacheNestedDataItemInfo({
  dataItemId,
  parentDataItemId,
  parentPayloadDataStart,
  startOffsetInRawParent,
  rawContentLength,
  payloadContentType,
  payloadDataStart,
  cacheService,
  logger,
}: {
  dataItemId: TransactionId;
  parentDataItemId: TransactionId;
  parentPayloadDataStart: number;
  startOffsetInRawParent: number;
  rawContentLength: number;
  payloadContentType: string;
  payloadDataStart: number;
  cacheService: CacheService;
  logger: winston.Logger;
}) {
  const payloadString = JSON.stringify(
    minifyNestedDataItemInfo({
      parentDataItemId,
      parentPayloadDataStart,
      startOffsetInRawParent,
      rawContentLength,
      payloadContentType,
      payloadDataStart,
    })
  );

  try {
    const nestedDataItemTTLSecs = await getConfigValue(
      "cacheWriteNestedDataItemTtlSecs"
    );
    await breakerForCache(cacheService).fire(() => {
      logger.debug(
        `Storing nested data item offsets for ${dataItemId} in Elasticache...`,
        {
          payloadString,
        }
      );
      return cacheService.set(
        `offsets_{${dataItemId}}`,
        payloadString,
        "EX",
        nestedDataItemTTLSecs
      );
    });
  } catch (error) {
    const errMsg = `Failed to store nested data item offsets ${dataItemId} in Elasticache`;
    logger.error(errMsg, { error: normalizeCacheError(error) });
    throw new CacheServiceError(errMsg, error);
  }
  logger.debug(
    `Cached nested data item offsets for ${dataItemId} in Elasticache`,
    {
      parentDataItemId,
      parentPayloadDataStart,
      startOffsetInRawParent,
      rawContentLength,
      payloadContentType,
      payloadDataStart,
    }
  );
}

export function normalizeCacheError(error: unknown) {
  if (!(error instanceof Error)) return error;

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const e = error as any;
  return {
    name: e.name,
    message: e.message,
    stack: e.stack,
    code: e.code,
    command: e.command,
    lastNodeError: e?.lastNodeError?.message,
    lastNodeStack: e?.lastNodeError?.stack,
  };
}
