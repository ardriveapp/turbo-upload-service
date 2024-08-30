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
  DeleteMessageBatchCommand,
  Message,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { Consumer } from "sqs-consumer";
import winston from "winston";

import { Database } from "../../../../src/arch/db/database";
import { EnqueuedNewDataItem, getQueueUrl } from "../../../../src/arch/queues";
import { jobLabels } from "../../../../src/constants";
import { newDataItemBatchInsertHandler } from "../../../../src/jobs/newDataItemBatchInsert";
import { MetricRegistry } from "../../../../src/metricRegistry";
import { generateArrayChunks } from "../../../../src/utils/common";
import { fulfillmentJobHandler } from "../utils/jobHandler";

const maxSqsBatchSize = 10;
const batchSizeToInsert = +(
  process.env.NEW_DATA_ITEM_BATCH_SIZE_THRESHOLD ?? 100
);
const maxWaitTimeForBatchMs = +(
  process.env.NEW_DATA_ITEM_BATCH_WAIT_TIME_MS ?? 5000
); // 5 seconds

type NewDataItemMessage = EnqueuedNewDataItem & {
  messageId: string;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  receiptHandle: any;
};

// Global scope to share dequeued messages between multiple queue consumers
let dequeuedDataItemMessages: NewDataItemMessage[] = [];
let timeoutId: NodeJS.Timeout | null = null;

export function createNewDataItemBatchInsertQueue({
  database,
  logger,
  sqsClient,
}: {
  database: Database;
  logger: winston.Logger;
  sqsClient: SQSClient;
}) {
  const newDataItemBatchInsertQueueUrl = getQueueUrl(jobLabels.newDataItem);
  const newDataItemBatchInsertLogger = logger.child({
    queue: jobLabels.newDataItem,
  });

  const insertCachedItems = async () => {
    if (timeoutId) {
      clearTimeout(timeoutId);
      timeoutId = null;
    }

    newDataItemBatchInsertLogger.debug("Inserting buffered items", {
      messageCount: dequeuedDataItemMessages.length,
    });

    if (dequeuedDataItemMessages.length === 0) return;

    const dataItemBatchToProcess = dequeuedDataItemMessages;
    dequeuedDataItemMessages = [];

    try {
      // Process the batch
      await fulfillmentJobHandler(
        () =>
          newDataItemBatchInsertHandler({
            dataItemBatch: dataItemBatchToProcess,
            logger: newDataItemBatchInsertLogger,
            uploadDatabase: database,
          }),
        jobLabels.newDataItem
      );

      // Delete the messages from the queue in batches of maxSqsBatchSize
      for (const receiptChunk of generateArrayChunks(
        dataItemBatchToProcess,
        maxSqsBatchSize
      )) {
        await sqsClient.send(
          new DeleteMessageBatchCommand({
            QueueUrl: newDataItemBatchInsertQueueUrl,
            Entries: receiptChunk.map((m) => ({
              Id: m.messageId,
              ReceiptHandle: m.receiptHandle,
            })),
          })
        );
      }

      MetricRegistry.newDataItemInsertBatchSizes.observe(
        dataItemBatchToProcess.length
      );
    } catch (error) {
      // On error, messages will be reprocessed when the visibility timeout expires
      newDataItemBatchInsertLogger.error(
        "Error processing data item batch",
        error
      );
    }
  };

  const addToCache = async (
    enqueuedDataItems: NewDataItemMessage[]
  ): Promise<void> => {
    dequeuedDataItemMessages.push(...enqueuedDataItems);

    if (dequeuedDataItemMessages.length >= batchSizeToInsert) {
      // Insert immediately if we have enough items
      return insertCachedItems();
    } else if (!timeoutId) {
      // Otherwise, set a timeout to insert the items after the max wait time
      timeoutId = setTimeout(() => {
        void insertCachedItems();
      }, maxWaitTimeForBatchMs);
    }
  };

  return {
    consumer: Consumer.create({
      queueUrl: newDataItemBatchInsertQueueUrl,
      sqs: sqsClient,
      handleMessageBatch: async (messages: Message[]) => {
        newDataItemBatchInsertLogger.debug(
          "New data item batch insert SQS handler has been triggered.",
          {
            messages,
          }
        );

        const enqueuedMessages = messages
          .map((message) => {
            if (!message.Body || !message.MessageId || !message.ReceiptHandle) {
              newDataItemBatchInsertLogger.error(
                "Message is missing required fields. Ignoring...",
                message
              );
              return undefined;
            }
            return {
              ...(JSON.parse(message.Body) as EnqueuedNewDataItem),
              messageId: message.MessageId,
              receiptHandle: message.ReceiptHandle,
            };
          })
          .filter((m) => !!m) as NewDataItemMessage[];

        await addToCache(enqueuedMessages);
      },
      batchSize: maxSqsBatchSize,
      terminateVisibilityTimeout: true,
      shouldDeleteMessages: false,
    }),
    logger: newDataItemBatchInsertLogger,
  };
}
