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
import { Message, SQSClient, SQSClientConfig } from "@aws-sdk/client-sqs";
import { config } from "dotenv";
import { Consumer, ConsumerOptions } from "sqs-consumer";
import winston from "winston";

import { Architecture } from "../../../src/arch/architecture";
import { PostgresDatabase } from "../../../src/arch/db/postgres";
import { FileSystemObjectStore } from "../../../src/arch/fileSystemObjectStore";
import { TurboPaymentService } from "../../../src/arch/payment";
import { migrateOnStartup } from "../../../src/constants";
import { prepareBundleHandler } from "../../../src/jobs/prepare";
import { seedBundleHandler } from "../../../src/jobs/seed";
import globalLogger from "../../../src/logger";
import { isTestEnv } from "../../../src/utils/common";
import { getS3ObjectStore } from "../../../src/utils/objectStoreUtils";

config();

type Queue = {
  queueUrl: string;
  handler: (
    planId: string,
    // TODO: Provide defaults for these vs generating them for each handler
    arch: Partial<Architecture>,
    logger?: winston.Logger
  ) => Promise<void>;
  logger: winston.Logger;
  consumerOptions?: Partial<ConsumerOptions>;
};

const prepareBundleQueueUrl = process.env.SQS_PREPARE_BUNDLE_URL;
const seedBundleQueueUrl = process.env.SQS_SEED_BUNDLE_URL;
if (!prepareBundleQueueUrl) {
  throw new Error("Missing required prepare bundle queue url!");
}
if (!seedBundleQueueUrl) {
  throw new Error("Missing required seed bundle queue url!");
}

const uploadDatabase = new PostgresDatabase({
  migrate: migrateOnStartup,
});
const objectStore =
  // If on test NODE_ENV or if no DATA_ITEM_BUCKET variable is set, use Local File System
  isTestEnv() || !process.env.DATA_ITEM_BUCKET
    ? new FileSystemObjectStore()
    : getS3ObjectStore();
const paymentService = new TurboPaymentService();

export const queues: Queue[] = [
  {
    queueUrl: prepareBundleQueueUrl,
    handler: prepareBundleHandler,
    logger: globalLogger.child({ queue: "prepare-bundle" }),
    consumerOptions: {
      pollingWaitTimeMs: 1000,
      visibilityTimeout: 360,
      heartbeatInterval: 30,
    },
  },
  {
    queueUrl: seedBundleQueueUrl,
    handler: seedBundleHandler,
    logger: globalLogger.child({ queue: "seed-bundle" }),
    consumerOptions: {
      pollingWaitTimeMs: 10,
      visibilityTimeout: 360,
      heartbeatInterval: 30,
    },
  },
];

const planIdMessageHandler = (message: Message, { handler, logger }: Queue) => {
  logger.info("new message", message);

  let planId = undefined;
  try {
    planId = JSON.parse(message.Body!).planId;
  } catch (error) {
    logger.error("error caught while parsing message body", error, message);
  }

  if (planId) {
    return handler(
      planId,
      { logger, database: uploadDatabase, objectStore, paymentService },
      logger
    );
  } else {
    throw new Error("message did NOT include an 'planId' field!");
  }
};

function createSQSConsumer({
  queue,
  sqsOptions = { region: "us-east-1", maxAttempts: 3 },
}: {
  queue: Queue;
  sqsOptions?: Partial<SQSClientConfig>;
}) {
  const { queueUrl, consumerOptions } = queue;
  return Consumer.create({
    queueUrl,
    handleMessage: (message: Message) => planIdMessageHandler(message, queue),
    sqs: new SQSClient(sqsOptions),
    batchSize: 1,
    terminateVisibilityTimeout: true,
    ...consumerOptions,
  });
}

type ConsumerQueue = Queue & { consumer: Consumer };

const consumers: ConsumerQueue[] = queues.map((queue) => ({
  consumer: createSQSConsumer({
    queue,
  }),
  ...queue,
}));

let shouldExit = false;
let numInflightMessages = 0;
let runningConsumers = 0;

const maybeExit = () => {
  if (shouldExit && numInflightMessages === 0 && runningConsumers === 0) {
    globalLogger.info(
      "Should Exit is true and there are no in flight messages or running consumers, exiting...",
      {
        numInflightMessages,
        runningConsumers,
      }
    );
    process.exit(0);
  }
};

function registerEventHandlers({ consumer, logger }: ConsumerQueue) {
  consumer.on("error", (error, message) => {
    logger.error(`[SQS] ERROR`, error, message);
  });

  consumer.on("processing_error", (error: { message: string }, message) => {
    numInflightMessages -= 1;
    logger.error(`[SQS] PROCESSING ERROR`, error, message);
    maybeExit();
  });

  consumer.on("message_received", (message) => {
    numInflightMessages += 1;
    logger.info(`[SQS] Message received`, message);
  });

  consumer.on("message_processed", (message) => {
    numInflightMessages -= 1;
    logger.info(`[SQS] Message processed`, message);
    maybeExit();
  });

  consumer.on("stopped", () => {
    logger.warn(`[SQS] Consumer has been STOPPED!`);
    runningConsumers -= 1;
    maybeExit();
  });

  consumer.on("started", () => {
    logger.info(`[SQS] Consumer Started!`);
    runningConsumers += 1;
  });

  consumer.on("empty", () => {
    logger.info(`[SQS] Queue is empty!`);
  });
}

function startQueueListeners(consumers: ConsumerQueue[]) {
  for (const consumerQueue of consumers) {
    const { logger, consumer } = consumerQueue;
    logger.info("Registering queue...");
    registerEventHandlers(consumerQueue);
    logger.info("Starting queue...");
    consumer.start();
  }
}

function stopQueueListeners(consumers: ConsumerQueue[]) {
  for (const consumerQueue of consumers) {
    const { logger, consumer } = consumerQueue;
    logger.info("Stopping queue...");
    consumer.stop();
  }
}

process.on("SIGTERM", () => {
  globalLogger.info("SIGTERM signal received. Stopping consumers...", {
    numInflightMessages,
    runningConsumers,
  });
  shouldExit = true;
  stopQueueListeners(consumers);
});

process.on("SIGINT", () => {
  globalLogger.info("SIGINT signal received. Stopping consumers...", {
    numInflightMessages,
    runningConsumers,
  });
  shouldExit = true;
  stopQueueListeners(consumers);
});

process.on("uncaughtException", (error) => {
  globalLogger.error("Uncaught exception", error);
});

process.on("beforeExit", (exitCode) => {
  globalLogger.info(
    `Exiting the fulfillment pipeline process with exit code ${exitCode}`,
    {
      numInflightMessages,
      runningConsumers,
    }
  );
});

(() => {
  globalLogger.info("Starting fulfillment-pipeline service...");
  startQueueListeners(consumers);
})();
