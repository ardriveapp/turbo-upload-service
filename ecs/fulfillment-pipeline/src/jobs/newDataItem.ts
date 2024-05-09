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
import { Message, SQSClient } from "@aws-sdk/client-sqs";
import { Consumer } from "sqs-consumer";
import winston from "winston";

import { Database } from "../../../../src/arch/db/database";
import { EnqueuedNewDataItem, getQueueUrl } from "../../../../src/arch/queues";
import { newDataItemBatchInsertHandler } from "../../../../src/jobs/newDataItemBatchInsert";
import {
  defaultSQSOptions,
  stubQueueHandler,
} from "../utils/queueHandlerConfig";

export function createNewDataItemBatchInsertQueue({
  database,
  logger,
}: {
  database: Database;
  logger: winston.Logger;
}) {
  const newDataItemBatchInsertQueueUrl = getQueueUrl("new-data-item");
  const newDataItemBatchInsertLogger = logger.child({
    queue: "new-data-item",
  });
  return {
    consumer: Consumer.create({
      queueUrl: newDataItemBatchInsertQueueUrl,
      sqs: new SQSClient(defaultSQSOptions),
      handleMessageBatch: async (messages: Message[]) => {
        newDataItemBatchInsertLogger.debug(
          "New data item batch insert sqs handler has been triggered.",
          {
            messages,
          }
        );
        return newDataItemBatchInsertHandler({
          dataItemBatch: messages
            .map((message) => {
              if (!message.Body) {
                newDataItemBatchInsertLogger.error(
                  "Message body is undefined!",
                  message
                );
                return undefined;
              }
              return JSON.parse(message.Body);
            })
            .filter((m) => !!m) as EnqueuedNewDataItem[],
          logger: newDataItemBatchInsertLogger,
          uploadDatabase: database,
        });
      },
      batchSize: 10, // TODO: we could batch further, but aws limits us at 10 here
      terminateVisibilityTimeout: true, // Retry inserts immediately on processing error
    }),
    queueUrl: newDataItemBatchInsertQueueUrl, // unused
    handler: stubQueueHandler, // unused
    logger: newDataItemBatchInsertLogger,
  };
}
