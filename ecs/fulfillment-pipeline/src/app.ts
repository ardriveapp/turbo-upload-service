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
import Koa from "koa";
import Router from "koa-router";
import * as promClient from "prom-client";
import { Consumer } from "sqs-consumer";
import { Logger } from "winston";

import { ArweaveGateway } from "../../../src/arch/arweaveGateway";
import { PostgresDatabase } from "../../../src/arch/db/postgres";
import { TurboPaymentService } from "../../../src/arch/payment";
import { jobLabels, migrateOnStartup } from "../../../src/constants";
import { postBundleHandler } from "../../../src/jobs/post";
import { prepareBundleHandler } from "../../../src/jobs/prepare";
import { seedBundleHandler } from "../../../src/jobs/seed";
import globalLogger from "../../../src/logger";
import { MetricRegistry } from "../../../src/metricRegistry";
import { loadConfig } from "../../../src/utils/config";
import { getS3ObjectStore } from "../../../src/utils/objectStoreUtils";
import { createFinalizeUploadConsumerQueue } from "./jobs/finalize";
import { createNewDataItemBatchInsertQueue } from "./jobs/newDataItem";
import { createOpticalConsumerQueue } from "./jobs/optical";
import { PlanBundleJobScheduler } from "./jobs/plan";
import { createUnbundleBDIQueueConsumer } from "./jobs/unbundleBdi";
import { VerifyBundleJobScheduler } from "./jobs/verify";
import { JobScheduler } from "./utils/jobScheduler";
import { createPlanIdHandlingSQSConsumer } from "./utils/planIdMessageHandler";
import {
  QueueHandlerConfig,
  defaultSQSOptions,
} from "./utils/queueHandlerConfig";

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

// Enforce required environment variables
const prepareBundleQueueUrl = process.env.SQS_PREPARE_BUNDLE_URL;
const postBundleQueueUrl = process.env.SQS_POST_BUNDLE_URL;
const seedBundleQueueUrl = process.env.SQS_SEED_BUNDLE_URL;
if (!prepareBundleQueueUrl) {
  throw new Error("Missing required prepare bundle queue url!");
}
if (!postBundleQueueUrl) {
  throw new Error("Missing required post bundle queue url!");
}
if (!seedBundleQueueUrl) {
  throw new Error("Missing required seed bundle queue url!");
}

// Set up dependencies
const uploadDatabase = new PostgresDatabase({
  migrate: migrateOnStartup,
  // todo: pass otel exporter
});
const objectStore = getS3ObjectStore();
const paymentService = new TurboPaymentService();
const arweaveGateway = new ArweaveGateway();

// Set up queue handler configurations for jobs based on a planId
const planIdQueueHandlerConfigs: QueueHandlerConfig[] = [
  {
    queueUrl: prepareBundleQueueUrl,
    jobName: jobLabels.prepareBundle,
    handler: prepareBundleHandler,
    consumerOptions: {
      pollingWaitTimeMs: 1000,
      visibilityTimeout: 360,
      heartbeatInterval: 30,
    },
  },
  {
    queueUrl: postBundleQueueUrl,
    jobName: jobLabels.postBundle,
    handler: postBundleHandler,
    consumerOptions: {
      pollingWaitTimeMs: 1000,
      visibilityTimeout: 90,
    },
  },
  {
    queueUrl: seedBundleQueueUrl,
    jobName: jobLabels.seedBundle,
    handler: seedBundleHandler,
    consumerOptions: {
      pollingWaitTimeMs: 10,
      visibilityTimeout: 360,
      heartbeatInterval: 30,
    },
  },
];

type ConsumerQueue = { consumer: Consumer; logger: Logger };

const consumers: ConsumerQueue[] = planIdQueueHandlerConfigs.map((queue) => {
  const logger = globalLogger.child({ queue: queue.jobName });
  return {
    logger,
    consumer: createPlanIdHandlingSQSConsumer({
      logger,
      queue,
      database: uploadDatabase,
      objectStore,
      paymentService,
      arweaveGateway,
    }),
  };
});

let shouldExit = false;
let numInflightMessages = 0;
let runningConsumers = 0;
let planBundleJobScheduler: PlanBundleJobScheduler | undefined;
let verifyBundleJobScheduler: VerifyBundleJobScheduler | undefined;

const maybeExit = () => {
  if (shouldExit) {
    planBundleJobScheduler?.stop();
    verifyBundleJobScheduler?.stop();
    if (numInflightMessages === 0 && runningConsumers === 0) {
      globalLogger.info(
        "Should Exit is true and there are no in flight messages or running consumers, exiting...",
        {
          numInflightMessages,
          runningConsumers,
        }
      );
      process.exit(0);
    }
  }
};

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
    logger.debug(`[SQS] Received message contents:`, message);
  });

  consumer.on("message_processed", (message: void | Message | Message[]) => {
    numInflightMessages -= 1;
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
}

function startQueueListeners(consumers: ConsumerQueue[]) {
  for (const consumerQueue of consumers) {
    const { logger, consumer } = consumerQueue;
    logger.debug("Registering queue...");
    registerEventHandlers(consumerQueue);
    logger.debug("Starting queue...");
    consumer.start();
  }
}

function stopQueueListeners(consumers: ConsumerQueue[]) {
  for (const consumerQueue of consumers) {
    const { logger, consumer } = consumerQueue;
    logger.debug("Stopping queue...");
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

// Set up queue handler configurations for jobs NOT based on a planId
type ConsumerProvisioningConfig = {
  envVarCountStr: string | undefined;
  defaultCount: number;
  createConsumerQueueFn: () => ConsumerQueue;
  friendlyQueueName: string;
};

const sqsClient = new SQSClient(defaultSQSOptions);

const consumerQueues: ConsumerProvisioningConfig[] = [
  {
    envVarCountStr: process.env.NUM_FINALIZE_UPLOAD_CONSUMERS,
    defaultCount: 2,
    createConsumerQueueFn: () =>
      createFinalizeUploadConsumerQueue({
        logger: globalLogger,
        database: uploadDatabase,
        objectStore,
        paymentService,
      }),
    friendlyQueueName: jobLabels.finalizeUpload,
  },
  {
    envVarCountStr: process.env.NUM_OPTICAL_CONSUMERS,
    defaultCount: 3,
    createConsumerQueueFn: () => createOpticalConsumerQueue(globalLogger),
    friendlyQueueName: jobLabels.opticalPost,
  },
  {
    envVarCountStr: process.env.NUM_NEW_DATA_ITEM_INSERT_CONSUMERS,
    defaultCount: 1,
    createConsumerQueueFn: () =>
      createNewDataItemBatchInsertQueue({
        database: uploadDatabase,
        logger: globalLogger,
        sqsClient,
      }),
    friendlyQueueName: jobLabels.newDataItem,
  },
  {
    envVarCountStr: process.env.NUM_UNBUNDLE_BDI_CONSUMERS,
    defaultCount: 1,
    createConsumerQueueFn: () => createUnbundleBDIQueueConsumer(globalLogger),
    friendlyQueueName: jobLabels.unbundleBdi,
  },
];

const consumersToStart: ConsumerQueue[] = consumerQueues
  .map((config) => {
    const count = +(config.envVarCountStr ?? config.defaultCount);
    globalLogger.info(
      `Starting up ${count} ${config.friendlyQueueName} consumers...`
    );
    return Array.from({ length: count }, () => config.createConsumerQueueFn());
  })
  .flat();

// start the listeners
consumers.push(...consumersToStart);

globalLogger.info("Starting fulfillment-pipeline service consumers...", {
  numConsumers: consumers.length,
});
startQueueListeners(consumers);

// Start up cron-like jobs
function setUpAndStartJobScheduler(jobScheduler: JobScheduler) {
  jobScheduler.on("job-start", () => numInflightMessages++);
  jobScheduler.on("job-complete", () => {
    numInflightMessages--;
    maybeExit();
  });
  jobScheduler.on("job-error", () => {
    numInflightMessages--;
    maybeExit();
  });
  jobScheduler.on("job-overdue", (schedulerName) => {
    globalLogger.info("Job overdue!", { schedulerName });
  });
  jobScheduler.start();
}

if (process.env.PLAN_BUNDLE_ENABLED === "true") {
  planBundleJobScheduler = new PlanBundleJobScheduler({
    intervalMs: +(process.env.PLAN_BUNDLE_INTERVAL_MS ?? 60_000),
    logger: globalLogger,
    database: uploadDatabase,
  });
  setUpAndStartJobScheduler(planBundleJobScheduler);
}
if (process.env.VERIFY_BUNDLE_ENABLED === "true") {
  verifyBundleJobScheduler = new VerifyBundleJobScheduler({
    intervalMs: +(process.env.VERIFY_BUNDLE_INTERVAL_MS ?? 60_000),
    logger: globalLogger,
    database: uploadDatabase,
  });
  setUpAndStartJobScheduler(verifyBundleJobScheduler);
}

const app = new Koa();
const router = new Router();

const metricsRegistry = MetricRegistry.getInstance().getRegistry();
promClient.collectDefaultMetrics({ register: metricsRegistry });

// Prometheus
router.get(["/fulfillment_metrics", "metrics"], async (ctx, next) => {
  ctx.body = await metricsRegistry.metrics();
  return next();
});

// Health check
router.get(["/health", "/"], (ctx, next) => {
  ctx.body = "OK";
  return next();
});
const port = +(process.env.FULFILLMENT_PORT ?? process.env.PORT ?? 4000);
app.use(router.routes());
const server = app.listen(port);

globalLogger.info(
  `Fulfillment pipeline service started with node environment ${process.env.NODE_ENV} on port ${port}...`
);

server.on("error", (error) => {
  globalLogger.error("Server error", error);
});
