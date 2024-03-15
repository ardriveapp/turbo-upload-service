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
import { Consumer, ConsumerOptions } from "sqs-consumer";
import winston from "winston";

import { Architecture } from "../../../src/arch/architecture";
import { ArweaveGateway } from "../../../src/arch/arweaveGateway";
import { PostgresDatabase } from "../../../src/arch/db/postgres";
import { FileSystemObjectStore } from "../../../src/arch/fileSystemObjectStore";
import { TurboPaymentService } from "../../../src/arch/payment";
import { getQueueUrl } from "../../../src/arch/queues";
import { migrateOnStartup } from "../../../src/constants";
import { opticalPostHandler } from "../../../src/jobs/optical-post";
import { prepareBundleHandler } from "../../../src/jobs/prepare";
import { seedBundleHandler } from "../../../src/jobs/seed";
import globalLogger from "../../../src/logger";
import { finalizeMultipartUploadWithQueueMessage } from "../../../src/routes/multiPartUploads";
import { isTestEnv } from "../../../src/utils/common";
import { loadConfig } from "../../../src/utils/config";
import { getArweaveWallet } from "../../../src/utils/getArweaveWallet";
import { getS3ObjectStore } from "../../../src/utils/objectStoreUtils";

type Queue = {
  queueUrl: string;
  handler: (
    planId: string,
    arch: Partial<Omit<Architecture, "logger">>,
    logger: winston.Logger
  ) => Promise<void>;
  logger: winston.Logger;
  consumerOptions?: Partial<ConsumerOptions>;
};

// let otelExporter: OTELExporter | undefined; // eslint-disable-line

// TODO: move to top level await
loadConfig()
  .then(() => {
    // TODO: enable OTEL when we have a clear set of desired traces
    // sets up our OTEL exporter
    // otelExporter = new OTELExporter({
    //   apiKey: process.env.HONEYCOMB_API_KEY,
    //   serviceName: "fulfillment-pipeline",
    // });
  })
  .catch(() => {
    globalLogger.error("Failed to load config!");
  });

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
  // todo: pass otel exporter
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

const planIdMessageHandler = ({
  message,
  logger,
  queue,
}: {
  message: Message;
  logger: winston.Logger;
  queue: Queue;
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
      database: uploadDatabase,
      objectStore,
      paymentService,
    },
    // provide our message logger to the handler
    messageLogger.child({ planId })
  );
};

const defaultSQSOptions = {
  region: "us-east-1",
  maxAttempts: 3,
};

function createSQSConsumer({
  queue,
  sqsOptions = defaultSQSOptions,
}: {
  queue: Queue;
  sqsOptions?: Partial<SQSClientConfig>;
}) {
  const { queueUrl, consumerOptions, logger } = queue;
  return Consumer.create({
    queueUrl,
    handleMessage: (message: Message) =>
      planIdMessageHandler({
        message,
        logger,
        queue,
      }),
    sqs: new SQSClient(sqsOptions),
    batchSize: 1,
    // NOTE: this causes messages that experience processing_error to be reprocessed right away, we may want to create a small delay to avoid them constantly failing and blocking the queue
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

const stubQueueHandler = async (
  _: string,
  __: Partial<Omit<Architecture, "logger">>,
  ___: winston.Logger
) => {
  return;
};

function createOpticalConsumerQueue() {
  const opticalQueueUrl = getQueueUrl("optical-post");
  const opticalPostLogger = globalLogger.child({ queue: "optical-post" });
  return {
    consumer: Consumer.create({
      queueUrl: opticalQueueUrl,
      handleMessageBatch: async (messages: Message[]) => {
        opticalPostLogger.info("Optical post sqs handler has been triggered.", {
          messages,
        });
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

function createFinalizeUploadConsumerQueue() {
  const finalizeUploadQueueUrl = getQueueUrl("finalize-upload");
  const finalizeUploadLogger = globalLogger.child({ queue: "finalize-upload" });
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
        return finalizeMultipartUploadWithQueueMessage({
          message,
          logger: finalizeUploadLogger,
          objectStore,
          paymentService,
          database: uploadDatabase,
          getArweaveWallet,
          arweaveGateway: new ArweaveGateway({}),
        });
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

function registerEventHandlers({ consumer, logger }: ConsumerQueue) {
  consumer.on(
    "error",
    (error: unknown, message: void | Message | Message[]) => {
      logger.error(`[SQS] ERROR`, error, message);
    }
  );

  consumer.on(
    "processing_error",
    (error: { message: string }, message: void | Message | Message[]) => {
      numInflightMessages -= 1;
      logger.error(`[SQS] PROCESSING ERROR`, error, message);
      maybeExit();
    }
  );

  consumer.on("message_received", (message: void | Message | Message[]) => {
    numInflightMessages += 1;
    logger.info(`[SQS] Message received`);
    logger.debug(`[SQS] Received message contents:`, message);
  });

  consumer.on("message_processed", (message: void | Message | Message[]) => {
    numInflightMessages -= 1;
    logger.info(`[SQS] Message processed`);
    logger.debug(`[SQS] Processed message contents:`, message);
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
    logger.debug(`[SQS] Queue is empty!`);
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

// start the listeners
const numFinalizeUploadConsumers = +(
  process.env.NUM_FINALIZE_UPLOAD_CONSUMERS ?? 10
);
const finalizeUploadConsumers: ConsumerQueue[] = Array.from(
  { length: numFinalizeUploadConsumers },
  createFinalizeUploadConsumerQueue
);
consumers.push(createOpticalConsumerQueue());
globalLogger.info(
  `Starting up ${finalizeUploadConsumers.length} finalize-upload consumers...`
);
consumers.push(...finalizeUploadConsumers);

globalLogger.info("Starting fulfillment-pipeline service...");
startQueueListeners(consumers);
