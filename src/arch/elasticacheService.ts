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
import { Redis } from "ioredis";

import globalLogger from "../logger";
import { CacheService, stubCacheService } from "./cacheServiceTypes";

const host = process.env.ELASTICACHE_HOST || "redis";
const redis: Redis =
  process.env.ELASTICACHE_NO_CLUSTERING === "true"
    ? new Redis({
        host,
        port: parseInt(process.env.ELASTICACHE_PORT || "6379"),
        password: process.env.ELASTICACHE_PASSWORD || undefined,
        tls: process.env.ELASTICACHE_USE_TLS === "true" ? {} : undefined,
      })
    : (new Redis.Cluster(
        [
          {
            host,
            port: parseInt(process.env.ELASTICACHE_PORT || "6379"),
          },
        ],
        {
          dnsLookup: (address, callback) => callback(null, address),
          redisOptions: {
            tls: process.env.ELASTICACHE_USE_TLS === "true" ? {} : undefined,
            password: process.env.ELASTICACHE_PASSWORD || undefined,
          },
        }
      ) as unknown as Redis); // HACK to avoid problems with CacheService presenting as a union type

redis.on("connect", () =>
  globalLogger.info(`Connected to Elasticache at ${host}!`)
);
redis.on("ready", () => globalLogger.info(`Elasticache at ${host} is ready!`));
redis.on("close", () => {
  globalLogger.info(`Connection to Elasticache at ${host} closed.`);
});
redis.on("reconnecting", () => {
  globalLogger.info(`Reconnecting to Elasticache at ${host}...`);
});
redis.on("error", (error) => {
  globalLogger.error(`Connection error with Elasticache at host ${host}`, {
    error,
  });
});

export function getElasticacheService(): CacheService {
  if (process.env.NODE_ENV === "test") {
    return stubCacheService;
  }
  return redis;
}
