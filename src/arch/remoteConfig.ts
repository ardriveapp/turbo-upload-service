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
import { GetParameterCommand } from "@aws-sdk/client-ssm";
import CircuitBreaker from "opossum";

import globalLogger from "../logger";
import {
  MetricRegistry,
  setUpCircuitBreakerListenerMetrics,
} from "../metricRegistry";
import { ssmClient } from "./ssmClient";

const configSpec = {
  cacheReadDataItemSamplingRate: {
    default: 1.0,
    env: "CACHE_READ_DATA_ITEM_SAMPLING_RATE",
  },
  cacheWriteDataItemSamplingRate: {
    default: 1.0,
    env: "CACHE_WRITE_DATA_ITEM_SAMPLING_RATE",
  },
  cacheWriteDataItemTtlSecs: {
    default: 3600,
    env: "CACHE_WRITE_DATA_ITEM_TTL_SECS",
  },
  cacheWriteNestedDataItemSamplingRate: {
    default: 1.0,
    env: "CACHE_WRITE_NESTED_DATA_ITEM_SAMPLING_RATE",
  },
  cacheWriteNestedDataItemTtlSecs: {
    default: 3600,
    env: "CACHE_WRITE_NESTED_DATA_ITEM_TTL_SECS",
  },
  cacheDataItemBytesThreshold: {
    default: 256 * 1024, // 256 KiB
    env: "CACHE_DATAITEM_BYTES_THRESHOLD",
  },
  fsBackupWriteDataItemSamplingRate: {
    default: 0.0,
    env: "FS_BACKUP_WRITE_DATA_ITEM_SAMPLING_RATE",
  },
  fsBackupWriteNestedDataItemSamplingRate: {
    default: 0.0,
    env: "FS_BACKUP_WRITE_NESTED_DATA_ITEM_SAMPLING_RATE",
  },
  objStoreDataItemSamplingRate: {
    default: 1.0,
    env: "OBJ_STORE_DATA_ITEM_SAMPLING_RATE",
  },
  objStoreNestedDataItemSamplingRate: {
    default: 1.0,
    env: "OBJ_STORE_NESTED_DATA_ITEM_SAMPLING_RATE",
  },
  dynamoWriteDataItemSamplingRate: {
    default: 1.0,
    env: "DYNAMO_WRITE_DATA_ITEM_SAMPLING_RATE",
  },
  dynamoWriteDataItemTtlSecs: {
    default: 604800, // 7 days
    env: "DYNAMO_WRITE_DATA_ITEM_TTL_SECS",
  },
  dynamoWriteNestedDataItemSamplingRate: {
    default: 1.0,
    env: "DYNAMO_WRITE_NESTED_DATA_ITEM_SAMPLING_RATE",
  },
  dynamoWriteOffsetsTtlSecs: {
    default: 31536000, // 365 days
    env: "DYNAMO_WRITE_OFFSETS_TTL_SECS",
  },
  dynamoDataItemBytesThreshold: {
    default: 10240, // 10 KiB
    env: "DYNAMO_DATA_ITEM_BYTES_THRESHOLD",
  },
  inFlightDataItemTtlSecs: { default: 60, env: "IN_FLIGHT_DATA_ITEM_TTL_SECS" },
  offsetsCollectionTimeoutMs: {
    default: 60_000,
    env: "OFFSETS_COLLECTION_TIMEOUT_MS",
  },
} as const;

type ConfigSpec = typeof configSpec;
export type UploadSvcConfig = {
  [K in keyof ConfigSpec]: number;
};
export type ConfigKey = keyof UploadSvcConfig;

export const ConfigKeys = Object.fromEntries(
  Object.keys(configSpec).map((k) => [k, k])
) as { [K in ConfigKey]: K };

const defaultRemoteConfig: UploadSvcConfig = Object.fromEntries(
  Object.entries(configSpec).map(([key, { default: def, env }]) => {
    const raw = process.env[env];
    const parsed = raw !== undefined ? Number(raw) : def;
    return [key, isNaN(parsed) ? def : parsed];
  })
) as UploadSvcConfig;

type PartialRemoteConfig = Partial<UploadSvcConfig>;

function parseAndMergeRemoteConfig(raw: string): UploadSvcConfig {
  let parsed: PartialRemoteConfig = {};
  try {
    parsed = JSON.parse(raw);
  } catch (error) {
    globalLogger.error("Failed to parse remote config JSON from SSM:", {
      error,
    });
  }
  return {
    ...defaultRemoteConfig,
    ...parsed,
  };
}

const configListeners = new Map<ConfigKey, Set<(value: number) => void>>();

function notifyListeners(updated: UploadSvcConfig) {
  for (const key of Object.keys(updated) as ConfigKey[]) {
    const newValue = updated[key];
    const listeners = configListeners.get(key);
    if (listeners) {
      for (const cb of listeners) {
        try {
          cb(newValue);
        } catch (err) {
          globalLogger.error("Config listener callback failed", { key, err });
        }
      }
    }
  }
}

let lastKnownGoodConfig: UploadSvcConfig | undefined;

const remoteCfgCache = new ReadThroughPromiseCache<void, UploadSvcConfig>({
  cacheParams: {
    cacheCapacity: 1,
    cacheTTLMillis: 180_000, // 3 minutes,
  },
  metricsConfig: {
    cacheName: "remote_cfg_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV || "local",
    },
  },
  readThroughFunction: async () => {
    if (process.env.NODE_ENV === "test") {
      return defaultRemoteConfig; // Skip SSM in tests
    }

    const result = await ssmClient.send(
      new GetParameterCommand({ Name: "/upload-service/remote-config" })
    );
    if (!result.Parameter?.Value) {
      throw new Error("No remote config value found in SSM");
    }
    const remoteCfg = parseAndMergeRemoteConfig(result.Parameter.Value);

    globalLogger.info("Retrieved cfg from SSM", {
      latestCfg: remoteCfg,
      prevCfg: lastKnownGoodConfig ?? defaultRemoteConfig,
    });
    lastKnownGoodConfig = remoteCfg;
    notifyListeners(remoteCfg);
    return remoteCfg;
  },
});

const configBreaker = new CircuitBreaker(() => remoteCfgCache.get(), {
  timeout: process.env.NODE_ENV === "local" ? 8_000 : 3_000,
  errorThresholdPercentage: 20,
  resetTimeout: 30_000, // 30 seconds
});
setUpCircuitBreakerListenerMetrics("remoteConfig", configBreaker, globalLogger);
configBreaker.fallback(() => {
  globalLogger.debug(
    "Breaker open! Falling back to last known good config or defaults."
  );
  return lastKnownGoodConfig ?? defaultRemoteConfig;
});

export async function getConfigValue<K extends ConfigKey>(
  key: K
): Promise<UploadSvcConfig[K]> {
  const config = await configBreaker.fire().catch((error) => {
    globalLogger.error(
      "Failed to get remote config. Falling back to last known good config or defaults.",
      { error }
    );
    return lastKnownGoodConfig ?? defaultRemoteConfig;
  });
  return config[key];
}

export function onConfigChange<K extends ConfigKey>(
  key: K,
  cb: (value: UploadSvcConfig[K]) => void
): void {
  const listeners = configListeners.get(key);
  if (listeners) {
    listeners.add(cb);
  } else {
    configListeners.set(key, new Set([cb]));
  }
}
