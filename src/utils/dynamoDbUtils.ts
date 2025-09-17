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
import {
  AttributeValue,
  DeleteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  GetItemCommandOutput,
  PutItemCommand,
} from "@aws-sdk/client-dynamodb";
import CircuitBreaker from "opossum";
import { Readable } from "stream";
import winston from "winston";
import { gunzipSync, gzipSync } from "zlib";

import { ConfigKeys, getConfigValue } from "../arch/remoteConfig";
import globalLogger from "../logger";
import {
  MetricRegistry,
  setUpCircuitBreakerListenerMetrics,
} from "../metricRegistry";
import {
  DataItemOffsetsInfo,
  PayloadInfo,
  TransactionId,
} from "../types/types";

export const cacheTableName =
  process.env.DDB_DATA_ITEM_TABLE ??
  `upload-service-cache-${process.env.NODE_ENV || "local"}`;
export const offsetsTableName =
  process.env.DDB_OFFSETS_TABLE ??
  `upload-service-offsets-${process.env.NODE_ENV || "local"}`;

const endpoint = process.env.AWS_ENDPOINT;
const awsRegion = process.env.AWS_REGION ?? "us-east-1";
const awsCredentials =
  process.env.AWS_ACCESS_KEY_ID !== undefined &&
  process.env.AWS_SECRET_ACCESS_KEY !== undefined
    ? {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        ...(process.env.AWS_SESSION_TOKEN
          ? {
              sessionToken: process.env.AWS_SESSION_TOKEN,
            }
          : {}),
      }
    : undefined;

const dynamoClient = new DynamoDBClient({
  endpoint,
  region: awsRegion,
  credentials: awsCredentials,
});

type DynamoTask<T> = () => Promise<T>;

const dynamoBreakers = new WeakMap<
  DynamoDBClient,
  {
    fire<T>(task: DynamoTask<T>): Promise<T>;
    breaker: CircuitBreaker<[DynamoTask<unknown>], unknown>;
  }
>();

export function breakerForDynamo(client: DynamoDBClient): {
  fire<T>(task: DynamoTask<T>): Promise<T>;
  breaker: CircuitBreaker<[DynamoTask<unknown>], unknown>;
} {
  const existing = dynamoBreakers.get(client);
  if (existing) return existing;

  const breaker = new CircuitBreaker<[DynamoTask<unknown>], unknown>(
    async (...args: [DynamoTask<unknown>]) => {
      const [task] = args;
      return task();
    },
    {
      timeout: process.env.NODE_ENV === "local" ? 10_000 : 3000,
      errorThresholdPercentage: 10,
      resetTimeout: 30_000,
    }
  );

  setUpCircuitBreakerListenerMetrics("dynamodb", breaker, globalLogger);
  breaker.on("timeout", () =>
    globalLogger.error("DynamoDB circuit breaker command timed out")
  );

  const wrapper = {
    fire<T>(task: DynamoTask<T>): Promise<T> {
      return breaker.fire(task) as Promise<T>;
    },
    breaker,
  };

  dynamoBreakers.set(client, wrapper);
  return wrapper;
}

export function dynamoAvailable(): boolean {
  return !breakerForDynamo(dynamoClient).breaker.opened;
}

const dynamoDataItemCache = new ReadThroughPromiseCache<
  TransactionId,
  { buffer: Buffer; info: PayloadInfo },
  { dynamoClient: DynamoDBClient; logger: winston.Logger }
>({
  cacheParams: {
    cacheCapacity: 1000,
    cacheTTLMillis: 60_000,
  },
  readThroughFunction: async (dataItemId, { dynamoClient, logger }) => {
    try {
      const res = (await breakerForDynamo(dynamoClient).fire(async () => {
        return dynamoClient.send(
          new GetItemCommand({
            TableName: cacheTableName,
            Key: { Id: { B: idToBinary(dataItemId) } },
          })
        );
      })) as GetItemCommandOutput;

      if (!res.Item) {
        throw new Error(
          `Data item with ID ${dataItemId} not found in DynamoDB!`
        );
      }

      const buffer =
        res.Item.D && res.Item.D.B
          ? Buffer.from(gunzipSync(res.Item.D.B as Uint8Array))
          : Buffer.alloc(0);
      if (!res.Item.P?.N) {
        throw new Error(`Data item ${dataItemId} has no payload start!`);
      }
      const payloadDataStart = +(res.Item.P?.N ?? 0);
      if (!res.Item.C?.S) {
        logger.error(`Data item ${dataItemId} has no content type!`);
      }
      const payloadContentType = res.Item.C?.S ?? "application/octet-stream";

      return {
        buffer,
        info: { payloadDataStart, payloadContentType },
      };
    } catch (error) {
      logger.error(`Error retrieving data item ${dataItemId} from DynamoDB`, {
        error,
      });
      throw error;
    }
  },
  metricsConfig: {
    cacheName: "ddb_item_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV || "local",
    },
  },
});

const dynamoDataItemExistsCache = new ReadThroughPromiseCache<
  TransactionId,
  boolean,
  { dynamoClient: DynamoDBClient; logger: winston.Logger }
>({
  cacheParams: {
    cacheCapacity: 1000,
    cacheTTLMillis: 60_000,
  },
  readThroughFunction: async (dataItemId, { dynamoClient, logger }) => {
    // Hack to keep dynamo out of unit tests
    if (process.env.NODE_ENV === "test") {
      logger.debug(
        `Skipping dynamoDataItemExistsCache check for data item ${dataItemId} in test environment`
      );
      return false;
    }

    try {
      const res = (await breakerForDynamo(dynamoClient).fire(async () => {
        return dynamoClient.send(
          new GetItemCommand({
            TableName: cacheTableName,
            Key: { Id: { B: idToBinary(dataItemId) } },
            ProjectionExpression: "Id",
          })
        );
      })) as GetItemCommandOutput;

      return !!res.Item;
    } catch (error) {
      logger.error(
        `Error checking existence of data item ${dataItemId} in DynamoDB`,
        {
          error,
        }
      );
      throw error;
    }
  },
  metricsConfig: {
    cacheName: "ddb_item_exists_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV || "local",
    },
  },
});

const dynamoOffsetsCache = new ReadThroughPromiseCache<
  TransactionId,
  DataItemOffsetsInfo | undefined,
  { dynamoClient: DynamoDBClient; logger: winston.Logger }
>({
  cacheParams: {
    cacheCapacity: 1000,
    cacheTTLMillis: 60_000,
  },
  readThroughFunction: async (dataItemId, { dynamoClient, logger }) => {
    try {
      const res = (await breakerForDynamo(dynamoClient).fire(async () => {
        return dynamoClient.send(
          new GetItemCommand({
            TableName: offsetsTableName,
            Key: { Id: { B: idToBinary(dataItemId) } },
          })
        );
      })) as GetItemCommandOutput;

      if (res.Item) {
        // TODO: Stricter deserialization checks
        return {
          dataItemId,
          parentDataItemId: res.Item.PId?.B
            ? Buffer.from(res.Item.PId.B).toString("base64url")
            : undefined,
          startOffsetInParentDataItemPayload: res.Item.SP?.N
            ? +res.Item.SP.N
            : undefined,
          rootBundleId: res.Item.RId?.B
            ? Buffer.from(res.Item.RId.B).toString("base64url")
            : undefined,
          startOffsetInRootBundle: res.Item.SR?.N ? +res.Item.SR.N : undefined,
          rawContentLength: +(res.Item.S?.N ?? 0),
          payloadContentType: res.Item.C?.S ?? "application/octet-stream",
          payloadDataStart: +(res.Item.P?.N ?? 0),
        };
      }
    } catch (error) {
      logger.error(`Error retrieving offsets for data item ${dataItemId}`, {
        error,
      });
      throw error;
    }

    logger.debug(
      `Offsets for data item with ID ${dataItemId} not found in DynamoDB`
    );

    // Throw so the cache doesn't cache the miss
    throw new Error(
      `Offsets for data item with ID ${dataItemId} not found in DynamoDB!`
    );
  },
  metricsConfig: {
    cacheName: "ddb_offsets_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV || "local",
    },
  },
});

export async function putDynamoDataItem(params: {
  dataItemId: TransactionId;
  data: Buffer;
  size: number;
  payloadStart: number;
  contentType: string;
  logger: winston.Logger;
}): Promise<void> {
  const { dataItemId, data, size, payloadStart, contentType, logger } = params;
  await breakerForDynamo(dynamoClient).fire(async () => {
    const expiresAt =
      Math.floor(Date.now() / 1000) +
      (await getConfigValue(ConfigKeys.dynamoWriteDataItemTtlSecs));
    await dynamoClient.send(
      new PutItemCommand({
        TableName: cacheTableName,
        Item: {
          Id: { B: idToBinary(dataItemId) }, // Data Item ID
          S: { N: size.toString() }, // Size of the raw data item
          P: { N: payloadStart.toString() }, // Payload Start offset
          C: { S: contentType }, // Content Type
          D: { B: gzipSync(data) }, // Gzipped raw data item
          X: { N: expiresAt.toString() }, // Expiration timestamp
        },
      })
    );
  });
  logger.debug(`Stored data item ${dataItemId} in dynamodb`);
}

export async function getDynamoDataItem({
  dataItemId,
  logger,
}: {
  dataItemId: TransactionId;
  logger: winston.Logger;
}): Promise<{ buffer: Buffer; info: PayloadInfo } | undefined> {
  return dynamoDataItemCache
    .get(dataItemId, {
      dynamoClient,
      logger,
    })
    .catch(() => undefined);
}

// Primarily used for testing purposes
export async function deleteDynamoDataItem(
  dataItemId: TransactionId,
  logger: winston.Logger
): Promise<void> {
  await breakerForDynamo(dynamoClient).fire(async () => {
    logger.debug(`Deleting data item ${dataItemId} from DynamoDB...`);
    try {
      await dynamoClient.send(
        new DeleteItemCommand({
          TableName: cacheTableName,
          Key: {
            Id: { B: idToBinary(dataItemId) },
          },
        })
      );
      dynamoDataItemCache.remove(dataItemId);
      dynamoDataItemExistsCache.remove(dataItemId);
    } catch (error) {
      logger.error(`Error deleting data item ${dataItemId} from DynamoDB`, {
        error,
      });
    }
  });
}

// Primarily used for testing purposes
export async function deleteDynamoDataItemOffsets(
  dataItemId: TransactionId,
  logger: winston.Logger
): Promise<void> {
  await breakerForDynamo(dynamoClient).fire(async () => {
    logger.debug(
      `Deleting offsets for data item ${dataItemId} from DynamoDB...`
    );
    try {
      await dynamoClient.send(
        new DeleteItemCommand({
          TableName: offsetsTableName,
          Key: {
            Id: { B: idToBinary(dataItemId) },
          },
        })
      );
      dynamoOffsetsCache.remove(dataItemId);
    } catch (error) {
      logger.error(
        `Error deleting offsets for data item ${dataItemId} from DynamoDB`,
        {
          error,
        }
      );
    }
  });
}

export async function dynamoHasDataItem(
  dataItemId: TransactionId,
  logger: winston.Logger
): Promise<boolean> {
  return dynamoDataItemExistsCache
    .get(dataItemId, {
      dynamoClient,
      logger,
    })
    .catch(() => false);
}

export async function dynamoReadableRange({
  dataItemId,
  start,
  inclusiveEnd,
  logger,
}: {
  dataItemId: TransactionId;
  start: number;
  inclusiveEnd?: number;
  logger: winston.Logger;
}): Promise<Readable | undefined> {
  const item = await getDynamoDataItem({ dataItemId, logger });
  if (!item) return undefined;
  const end =
    inclusiveEnd !== undefined ? inclusiveEnd + 1 : item.buffer.length;
  return Readable.from(item.buffer.subarray(start, end));
}

export async function dynamoPayloadInfo({
  dataItemId,
  logger,
}: {
  dataItemId: TransactionId;
  logger: winston.Logger;
}): Promise<PayloadInfo | undefined> {
  return dynamoDataItemCache
    .get(dataItemId, {
      dynamoClient,
      logger,
    })
    .then((item) => item.info)
    .catch(() => undefined);
}

export function idToBinary(dataItemId: TransactionId): Uint8Array {
  return Buffer.from(dataItemId, "base64url");
}

export async function putDynamoOffsetsInfo({
  dataItemId,
  parentDataItemId,
  startOffsetInParentDataItemPayload,
  rawContentLength,
  payloadContentType,
  payloadDataStart,
  rootBundleId,
  startOffsetInRootBundle,
  logger,
}: DataItemOffsetsInfo & {
  logger: winston.Logger;
}): Promise<void> {
  const expiresAt =
    Math.floor(Date.now() / 1000) +
    (await getConfigValue(ConfigKeys.dynamoWriteOffsetsTtlSecs));

  const putCommand = new PutItemCommand({
    TableName: offsetsTableName,
    Item: dataItemOffsetsInfoToDdbItem(
      {
        dataItemId,
        parentDataItemId,
        startOffsetInParentDataItemPayload,
        rawContentLength,
        payloadContentType,
        payloadDataStart,
        rootBundleId,
        startOffsetInRootBundle,
      },
      expiresAt
    ),
  });

  await breakerForDynamo(dynamoClient).fire(async () => {
    logger.debug(`Storing nested data item offsets to DynamoDB...`, {
      dataItemId,
      parentDataItemId,
      startOffsetInParentDataItemPayload,
      rawContentLength,
      payloadContentType,
      payloadDataStart,
      rootBundleId,
      startOffsetInRootBundle,
      expiresAt,
    });
    await dynamoClient.send(putCommand);
  });
}

export async function getDynamoOffsetsInfo(
  dataItemId: TransactionId,
  logger: winston.Logger
): Promise<DataItemOffsetsInfo | undefined> {
  return dynamoOffsetsCache
    .get(dataItemId, {
      dynamoClient,
      logger,
    })
    .catch(() => undefined);
}

// Primarily used for testing purposes
export async function deleteDynamoOffsetsInfo(
  dataItemId: TransactionId,
  logger: winston.Logger
): Promise<void> {
  await breakerForDynamo(dynamoClient).fire(async () => {
    logger.debug(
      `Deleting offsets for data item ${dataItemId} from DynamoDB...`
    );
    try {
      await dynamoClient.send(
        new DeleteItemCommand({
          TableName: offsetsTableName,
          Key: {
            Id: { B: idToBinary(dataItemId) },
          },
        })
      );
    } catch (error) {
      logger.error(`Error deleting offsets for data item ${dataItemId}`, {
        error,
      });
    }
  });
}

export function dataItemOffsetsInfoToDdbItem(
  offsetsInfo: DataItemOffsetsInfo,
  expiresAt: number
): Record<string, AttributeValue> {
  return {
    Id: { B: idToBinary(offsetsInfo.dataItemId) },
    ...(offsetsInfo.parentDataItemId &&
    offsetsInfo.startOffsetInParentDataItemPayload
      ? {
          PId: {
            B: idToBinary(offsetsInfo.parentDataItemId),
          },
          SP: {
            N: offsetsInfo.startOffsetInParentDataItemPayload.toString(),
          },
        }
      : {}),
    ...(offsetsInfo.rootBundleId && offsetsInfo.startOffsetInRootBundle
      ? {
          SR: {
            N: offsetsInfo.startOffsetInRootBundle.toString(),
          },
          RId: {
            B: idToBinary(offsetsInfo.rootBundleId),
          },
        }
      : {}),
    S: { N: offsetsInfo.rawContentLength.toString() },
    P: { N: offsetsInfo.payloadDataStart.toString() },
    C: { S: offsetsInfo.payloadContentType },
    X: { N: expiresAt.toString() }, // Expiration timestamp
  };
}
