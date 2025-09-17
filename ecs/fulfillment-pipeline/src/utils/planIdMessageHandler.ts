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
import { Message, SQSClient, SQSClientConfig } from "@aws-sdk/client-sqs";
import { Consumer } from "sqs-consumer";
import winston from "winston";

import { ArweaveGateway } from "../../../../src/arch/arweaveGateway";
import { Database } from "../../../../src/arch/db/database";
import { ObjectStore } from "../../../../src/arch/objectStore";
import { PaymentService } from "../../../../src/arch/payment";
import { fulfillmentJobHandler } from "./jobHandler";
import { QueueHandlerConfig, defaultSQSOptions } from "./queueHandlerConfig";

// A utility function for running message handlers driven by a planId field
export const planIdMessageHandler = ({
  message,
  logger,
  queue,
  database,
  objectStore,
  paymentService,
  arweaveGateway,
}: {
  message: Message;
  logger: winston.Logger;
  queue: QueueHandlerConfig;
  database: Database;
  objectStore: ObjectStore;
  paymentService: PaymentService;
  arweaveGateway: ArweaveGateway;
}) => {
  const messageLogger = logger.child({
    messageId: message.MessageId,
  });

  let planId = undefined;

  if (!message.Body) throw new Error("message body is undefined");

  try {
    planId = JSON.parse(message.Body).planId;
  } catch (error) {
    messageLogger.error(
      "error caught while parsing message body",
      error,
      message
    );
  }

  if (!planId) {
    throw new Error("message did NOT include an 'planId' field!");
  }

  // attach plan id to queue logger
  return queue.handler(
    planId,
    {
      database,
      objectStore,
      paymentService,
      arweaveGateway,
    },
    // provide our message logger to the handler
    messageLogger.child({ planId })
  );
};

export function createPlanIdHandlingSQSConsumer({
  queue,
  sqsOptions = defaultSQSOptions,
  database,
  objectStore,
  paymentService,
  arweaveGateway,
  logger,
}: {
  queue: QueueHandlerConfig;
  sqsOptions?: Partial<SQSClientConfig>;
  database: Database;
  objectStore: ObjectStore;
  paymentService: PaymentService;
  arweaveGateway: ArweaveGateway;
  logger: winston.Logger;
}) {
  const { queueUrl, consumerOptions } = queue;
  return Consumer.create({
    queueUrl,
    handleMessage: (message: Message) =>
      fulfillmentJobHandler(
        () =>
          planIdMessageHandler({
            message,
            logger,
            queue,
            database,
            objectStore,
            paymentService,
            arweaveGateway,
          }),
        queue.jobName
      ),
    sqs: new SQSClient(sqsOptions),
    batchSize: 1,
    // NOTE: this causes messages that experience processing_error to be reprocessed right away, we may want to create a small delay to avoid them constantly failing and blocking the queue
    terminateVisibilityTimeout: true,
    ...consumerOptions,
  });
}
