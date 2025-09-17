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
import { jobLabels } from "../../../../src/constants";
import { putOffsetsSQSHandler } from "../../../../src/jobs/putOffsets";
import { fulfillmentJobHandler } from "../utils/jobHandler";
import { defaultSQSOptions } from "../utils/queueHandlerConfig";

export function createPutOffsetsQueueConsumer(logger: winston.Logger) {
  const putOffsetsQueueUrl = getQueueUrl(jobLabels.putOffsets);
  const putOffsetsLogger = logger.child({
    queue: jobLabels.putOffsets,
  });
  return {
    consumer: Consumer.create({
      queueUrl: putOffsetsQueueUrl,
      sqs: new SQSClient(defaultSQSOptions),
      handleMessage: async (message: Message) => {
        putOffsetsLogger.debug("Put Offsets sqs handler has been triggered.", {
          message,
        });
        return fulfillmentJobHandler(
          () => putOffsetsSQSHandler(message, putOffsetsLogger),
          jobLabels.putOffsets
        );
      },
      terminateVisibilityTimeout: true, // Re-enqueue failures immediately on processing error
    }),
    logger: putOffsetsLogger,
  };
}
