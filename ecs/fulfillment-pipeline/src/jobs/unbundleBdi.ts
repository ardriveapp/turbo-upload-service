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
import { unbundleBDISQSHandler } from "../../../../src/jobs/unbundle-bdi";
import {
  defaultSQSOptions,
  stubQueueHandler,
} from "../utils/queueHandlerConfig";

export function createUnbundleBDIQueueConsumer(logger: winston.Logger) {
  const unbundleBDIQueueUrl = getQueueUrl("unbundle-bdi");
  const unbundleBDILogger = logger.child({
    queue: "unbundle-bdi",
  });
  return {
    consumer: Consumer.create({
      queueUrl: unbundleBDIQueueUrl,
      sqs: new SQSClient(defaultSQSOptions),
      handleMessageBatch: async (messages: Message[]) => {
        unbundleBDILogger.debug(
          "Unbundle BDIs batch sqs handler has been triggered.",
          {
            messages,
          }
        );
        return unbundleBDISQSHandler(messages, unbundleBDILogger);
      },
      batchSize: 10,
      terminateVisibilityTimeout: true, // Re-enqueue failures immediately on processing error
    }),
    queueUrl: unbundleBDIQueueUrl, // unused
    handler: stubQueueHandler, // unused
    logger: unbundleBDILogger,
  };
}
