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

import { ArweaveGateway } from "../../../../src/arch/arweaveGateway";
import { Database } from "../../../../src/arch/db/database";
import { ObjectStore } from "../../../../src/arch/objectStore";
import { PaymentService } from "../../../../src/arch/payment";
import { getQueueUrl } from "../../../../src/arch/queues";
import { gatewayUrl } from "../../../../src/constants";
import { finalizeMultipartUploadWithQueueMessage } from "../../../../src/routes/multiPartUploads";
import {
  DataItemExistsWarning,
  InsufficientBalance,
} from "../../../../src/utils/errors";
import { getArweaveWallet } from "../../../../src/utils/getArweaveWallet";
import {
  defaultSQSOptions,
  stubQueueHandler,
} from "../utils/queueHandlerConfig";

export function createFinalizeUploadConsumerQueue({
  logger,
  database,
  objectStore,
  paymentService,
}: {
  logger: winston.Logger;
  database: Database;
  objectStore: ObjectStore;
  paymentService: PaymentService;
}) {
  const finalizeUploadQueueUrl = getQueueUrl("finalize-upload");
  const finalizeUploadLogger = logger.child({ queue: "finalize-upload" });
  return {
    consumer: Consumer.create({
      queueUrl: finalizeUploadQueueUrl,
      handleMessage: async (message: Message) => {
        finalizeUploadLogger.info(
          "Finalize upload sqs handler has been triggered.",
          {
            message,
          }
        );
        try {
          await finalizeMultipartUploadWithQueueMessage({
            message,
            logger: finalizeUploadLogger,
            objectStore,
            paymentService,
            database,
            getArweaveWallet,
            arweaveGateway: new ArweaveGateway({
              endpoint: gatewayUrl,
            }),
          });
        } catch (error) {
          if (error instanceof DataItemExistsWarning) {
            finalizeUploadLogger.warn("Data item already exists", {
              error: error.message,
              messageId: message.MessageId,
              messageBody: message.Body,
            });
            return;
          } else if (error instanceof InsufficientBalance) {
            return;
          }
          throw error;
        }
      },
      sqs: new SQSClient(defaultSQSOptions),
      // NOTE: this causes messages that experience processing_error to be reprocessed right away, we may want to create a small delay to avoid them constantly failing and blocking the queue
      terminateVisibilityTimeout: true,
      heartbeatInterval: 20,
      visibilityTimeout: 30,
      pollingWaitTimeMs: 500,
    }),
    queueUrl: finalizeUploadQueueUrl, // unused
    handler: stubQueueHandler, // unused
    logger: finalizeUploadLogger,
  };
}
