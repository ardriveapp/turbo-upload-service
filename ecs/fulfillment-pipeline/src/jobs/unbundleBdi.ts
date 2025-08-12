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

import { CacheService } from "../../../../src/arch/cacheServiceTypes";
import { getQueueUrl } from "../../../../src/arch/queues";
import { jobLabels } from "../../../../src/constants";
import { unbundleBDISQSHandler } from "../../../../src/jobs/unbundle-bdi";
import { fulfillmentJobHandler } from "../utils/jobHandler";
import { defaultSQSOptions } from "../utils/queueHandlerConfig";

export function createUnbundleBDIQueueConsumer(
  logger: winston.Logger,
  cacheService: CacheService
) {
  const unbundleBDIQueueUrl = getQueueUrl(jobLabels.unbundleBdi);
  const unbundleBDILogger = logger.child({
    queue: jobLabels.unbundleBdi,
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
        return fulfillmentJobHandler(
          () =>
            unbundleBDISQSHandler(messages, unbundleBDILogger, cacheService),
          jobLabels.unbundleBdi
        );
      },
      batchSize: 10,
      terminateVisibilityTimeout: true, // Re-enqueue failures immediately on processing error
    }),
    logger: unbundleBDILogger,
  };
}
