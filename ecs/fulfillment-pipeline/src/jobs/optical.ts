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

import { getQueueUrl } from "../../../../src/arch/queues";
import { opticalPostHandler } from "../../../../src/jobs/optical-post";
import {
  defaultSQSOptions,
  stubQueueHandler,
} from "../utils/queueHandlerConfig";

export function createOpticalConsumerQueue(logger: winston.Logger) {
  const opticalQueueUrl = getQueueUrl("optical-post");
  const opticalPostLogger = logger.child({ queue: "optical-post" });
  return {
    consumer: Consumer.create({
      queueUrl: opticalQueueUrl,
      handleMessageBatch: async (messages: Message[]) => {
        opticalPostLogger.debug(
          "Optical post sqs handler has been triggered.",
          {
            messages,
          }
        );
        return opticalPostHandler({
          // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          stringifiedDataItemHeaders: messages.map((message) => message.Body!),
          logger: opticalPostLogger,
        });
      },
      sqs: new SQSClient(defaultSQSOptions),
      batchSize: 10, // TODO: Tune as needed - starting with value in terraform
      // NOTE: this causes messages that experience processing_error to be reprocessed right away, we may want to create a small delay to avoid them constantly failing and blocking the queue
      terminateVisibilityTimeout: true,
      pollingWaitTimeMs: 1000,
      visibilityTimeout: 120,
    }),
    queueUrl: opticalQueueUrl, // unused
    handler: stubQueueHandler, // unused
    logger: opticalPostLogger,
  };
}
