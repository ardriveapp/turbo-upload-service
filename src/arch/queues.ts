/**
 * Copyright (C) 2022-2023 Permanent Data Solutions, Inc. All Rights Reserved.
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
  DeleteMessageBatchRequestEntry,
  SQSClient,
  SendMessageCommand,
} from "@aws-sdk/client-sqs";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";
import { SQSEvent, SQSHandler, SQSRecord } from "aws-lambda";
import * as https from "https";

import logger from "../logger";
import { PlanId } from "../types/dbTypes";
import { DataItemId } from "../types/types";
import { isTestEnv } from "../utils/common";
import { SignedDataItemHeader } from "../utils/opticalUtils";

type QueueType =
  | "prepare-bundle"
  | "post-bundle"
  | "seed-bundle"
  | "optical-post"
  | "unbundle-bdi";

type SQSQueueUrl = string;

export const queues: { [key in QueueType]: SQSQueueUrl } = {
  "prepare-bundle": process.env.SQS_PREPARE_BUNDLE_URL!,
  "post-bundle": process.env.SQS_POST_BUNDLE_URL!,
  "seed-bundle": process.env.SQS_SEED_BUNDLE_URL!,
  "optical-post": process.env.SQS_OPTICAL_URL!,
  "unbundle-bdi": process.env.SQS_UNBUNDLE_BDI_URL!,
};

type PlanMessage = { planId: PlanId };

type QueueTypeToMessageType = {
  "prepare-bundle": PlanMessage;
  "post-bundle": PlanMessage;
  "seed-bundle": PlanMessage;
  "optical-post": SignedDataItemHeader;
  "unbundle-bdi": DataItemId;
};

const sqs = new SQSClient({
  maxAttempts: 3,
  requestHandler: new NodeHttpHandler({
    httpsAgent: new https.Agent({
      keepAlive: true,
    }),
  }),
});

export const getQueueUrl = (type: QueueType): SQSQueueUrl => {
  return queues[type];
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function* chunks(arr: any[], n: number) {
  for (let i = 0; i < arr.length; i += n) {
    yield arr.slice(i, i + n);
  }
}

export const enqueue = async <T extends QueueType>(
  queueType: T,
  message: QueueTypeToMessageType[T]
) => {
  if (isTestEnv()) {
    logger.error("Skipping SQS Enqueue since we are on test environment");
    return;
  }

  const sendMsgCmd = new SendMessageCommand({
    QueueUrl: getQueueUrl(queueType),
    MessageBody: JSON.stringify(message),
  });
  await sqs.send(sendMsgCmd);
};

export const deleteMessages = async (
  queueType: QueueType,
  receipts: DeleteMessageBatchRequestEntry[]
) => {
  if (!receipts.length) {
    return;
  }
  const queueUrl = getQueueUrl(queueType);
  for (const receiptChunk of chunks(receipts, 10)) {
    const deleteCmd = new DeleteMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: receiptChunk,
    });
    await sqs.send(deleteCmd);
  }
};

export const createQueueHandler = <T extends QueueType>(
  queueType: T,
  handler: (
    message: QueueTypeToMessageType[T],
    sqsMessage: SQSRecord
  ) => Promise<void>,
  hooks?: {
    before?: () => Promise<void>;
    after?: () => Promise<void>;
  }
): SQSHandler => {
  return async (event: SQSEvent) => {
    if (hooks && hooks.before) {
      await hooks.before();
    }
    try {
      if (!event) {
        logger.info(`[sqs-handler] invalid SQS messages received`, { event });
        throw new Error("Queue handler: invalid SQS messages received");
      }

      logger.info(`[sqs-handler] received messages`, {
        count: event.Records.length,
        source: event.Records[0].eventSourceARN,
      });

      const receipts: { Id: string; ReceiptHandle: string }[] = [];

      const errors: Error[] = [];

      await Promise.all(
        event.Records.map(async (sqsMessage: SQSRecord) => {
          logger.info(`[sqs-handler] processing message`, { sqsMessage });
          try {
            await handler(
              JSON.parse(sqsMessage.body) as QueueTypeToMessageType[T],
              sqsMessage
            );
            receipts.push({
              Id: sqsMessage.messageId,
              ReceiptHandle: sqsMessage.receiptHandle,
            });
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
          } catch (error: any) {
            logger.error(`[sqs-handler] error processing message`, {
              event,
              error,
            });
            errors.push(error);
          }
        })
      );

      logger.info(`[sqs-handler] queue handler complete`, {
        successful: receipts.length,
        failed: event.Records.length - receipts.length,
      });

      await deleteMessages(queueType, receipts);

      if (receipts.length !== event.Records.length) {
        logger.warn(
          `Failed to process ${event.Records.length - receipts.length} messages`
        );

        // If all the errors are the same then fail the whole queue with a more specific error message
        if (errors.every((error) => error.message == errors[0].message)) {
          throw new Error(
            `Failed to process SQS messages: ${errors[0].message}`
          );
        }

        throw new Error(`Failed to process SQS messages`);
      }
    } finally {
      if (hooks && hooks.after) {
        await hooks.after();
      }
    }
  };
};
