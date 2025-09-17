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
import {
  BatchWriteItemCommand,
  DynamoDBClient,
  ProvisionedThroughputExceededException,
  WriteRequest,
} from "@aws-sdk/client-dynamodb";
import { Message } from "@aws-sdk/client-sqs";
import winston from "winston";

import { ConfigKeys, getConfigValue } from "../arch/remoteConfig";
import { DataItemOffsetsInfo } from "../types/types";
import { generateArrayChunks, sleep } from "../utils/common";
import {
  dataItemOffsetsInfoToDdbItem,
  offsetsTableName,
} from "../utils/dynamoDbUtils";

const client = new DynamoDBClient({});

/**
 * Handler for processing SQS messages containing offset data and writing them to DynamoDB.
 *
 * Business Logic:
 * 1. Parse the SQS message body to extract an array of offset objects.
 * 2. Validate that offsets are present; log and throw if missing.
 * 3. Determine the TTL (time-to-live) for the DynamoDB items from configuration.
 * 4. Chunk the offsets into batches of 25 (DynamoDB batch write limit).
 * 5. For each batch:
 *    a. Map each offset object to a DynamoDB PutRequest item, converting fields as needed.
 *    b. Send the batch to DynamoDB using BatchWriteItemCommand.
 *    c. Log errors and rethrow if any batch fails.
 */
export async function putOffsetsSQSHandler(
  message: Message,
  logger: winston.Logger
) {
  const offsetsToBatchAndSend = JSON.parse(message.Body || "{}").offsets as
    | DataItemOffsetsInfo[]
    | undefined;

  if (!offsetsToBatchAndSend) {
    logger.error(
      "No offsets found in message body, cannot batch and send offsets",
      {
        messageBodyKeys: Object.keys(message.Body || {}),
      }
    );
    throw new Error("No offsets to batch and send");
  }

  // Chunk the offsets into batches of 25, the max dynamodb batch size
  const ddbWriteBatchSize = 25;
  const expiresAt =
    Math.floor(Date.now() / 1000) +
    (await getConfigValue(ConfigKeys.dynamoWriteOffsetsTtlSecs));

  logger.info("Putting offsets into DynamoDB...", {
    offsetsCount: offsetsToBatchAndSend.length,
    expiresAt,
    numBatches: Math.ceil(offsetsToBatchAndSend.length / ddbWriteBatchSize),
  });
  let batchNumber = 1;
  for (const batchToPut of generateArrayChunks(
    offsetsToBatchAndSend,
    ddbWriteBatchSize
  )) {
    const ddbPutRequests = batchToPut.map(
      ({
        dataItemId,
        parentDataItemId,
        startOffsetInParentDataItemPayload,
        rawContentLength,
        payloadDataStart,
        payloadContentType,
        rootBundleId,
        startOffsetInRootBundle,
      }) => ({
        PutRequest: {
          Item: dataItemOffsetsInfoToDdbItem(
            {
              dataItemId,
              parentDataItemId,
              startOffsetInParentDataItemPayload,
              rawContentLength,
              payloadDataStart,
              payloadContentType,
              rootBundleId,
              startOffsetInRootBundle,
            },
            expiresAt
          ),
        },
      })
    );

    try {
      await batchWriteAll(client, offsetsTableName, ddbPutRequests, logger);
    } catch (error) {
      logger.error("Error putting offsets into DynamoDB", {
        error,
        batchNumber,
      });
      throw error;
    }
    batchNumber++;
  }
}

// TODO: Get an efficient shuffler
function shuffle<T>(array: T[]): T[] {
  for (let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
  return array;
}

async function batchWriteAll(
  client: DynamoDBClient,
  table: string,
  puts: WriteRequest[],
  logger: winston.Logger
): Promise<void> {
  const batches = [puts];
  let attempt = 0;

  while (batches.length > 0) {
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const currentBatch = batches.shift()!;
    try {
      const resp = await client.send(
        new BatchWriteItemCommand({ RequestItems: { [table]: currentBatch } })
      );

      const unprocessed = shuffle(resp.UnprocessedItems?.[table] ?? []);
      if (unprocessed.length > 0) {
        // Split if needed, but keep all work in the same promise chain
        if (unprocessed.length > 1) {
          const half = Math.ceil(unprocessed.length / 2);
          batches.push(unprocessed.slice(0, half));
          batches.push(unprocessed.slice(half));
        } else {
          batches.push(unprocessed);
        }

        attempt++;
        const waitMs = calculateBackoff(attempt);
        logger.debug("Retrying unprocessed DynamoDB batch", {
          attempt,
          count: unprocessed.length,
          waitMs,
        });
        await sleep(waitMs);
      } else {
        attempt = 0; // Reset attempt counter on success
      }
    } catch (error: any) {
      if (
        error &&
        ([
          "ProvisionedThroughputExceededException",
          "ThrottlingException",
          "RequestLimitExceeded",
        ].includes(error.name) ||
          error instanceof ProvisionedThroughputExceededException)
      ) {
        attempt++;
        const waitMs = calculateBackoff(attempt);
        logger.debug("Throttling exception, backing off before retry", {
          attempt,
          waitMs,
        });
        batches.unshift(currentBatch); // Retry the same batch
        await sleep(waitMs);
      } else {
        throw error;
      }
    }
  }
}

function calculateBackoff(attempt: number): number {
  const base = 25; // ms
  const cap = 2000; // ms
  return Math.min(cap, base * 2 ** attempt) * (0.5 + Math.random());
}
