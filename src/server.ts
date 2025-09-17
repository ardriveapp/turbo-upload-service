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
import cors from "@koa/cors";
import Koa, { DefaultState, Next, ParameterizedContext } from "koa";

import { Architecture, defaultArchitecture } from "./arch/architecture";
import { OTELExporter } from "./arch/tracing";
import { port as defaultPort } from "./constants";
import globalLogger from "./logger";
import { MetricRegistry } from "./metricRegistry";
import {
  architectureMiddleware,
  loggerMiddleware,
  requestMiddleware,
} from "./middleware";
import router from "./router";
import { getErrorCodeFromErrorObject } from "./utils/common";
import { loadConfig } from "./utils/config";

type KoaState = DefaultState & Architecture;
export type KoaContext = ParameterizedContext<KoaState>;

globalLogger.info(
  `Starting server with node environment ${process.env.NODE_ENV}...`
);

// global error handler
process.on("uncaughtException", (error) => {
  // Determine error code for metrics
  const errorCode = getErrorCodeFromErrorObject(error);

  // Always increment the counter with appropriate error_code label
  MetricRegistry.uncaughtExceptionCounter.inc({ error_code: errorCode });

  globalLogger.error("Uncaught exception:", error);
});

export async function createServer(
  arch: Partial<Architecture>,
  port: number = defaultPort
) {
  // load ssm parameters
  await loadConfig();

  const app = new Koa();
  const uploadDatabase = arch.database ?? defaultArchitecture.database;
  const objectStore = arch.objectStore ?? defaultArchitecture.objectStore;
  const paymentService =
    arch.paymentService ?? defaultArchitecture.paymentService;
  const cacheService = arch.cacheService ?? defaultArchitecture.cacheService;

  const getArweaveWallet =
    arch.getArweaveWallet ?? defaultArchitecture.getArweaveWallet;
  const arweaveGateway =
    arch.arweaveGateway ?? defaultArchitecture.arweaveGateway;
  const tracer =
    arch.tracer ??
    new OTELExporter({
      apiKey: process.env.HONEYCOMB_API_KEY,
    }).getTracer("upload-service");

  // attach logger to context including trace id
  app.use(loggerMiddleware);
  // attaches listeners related to request streams for debugging
  app.use(requestMiddleware);
  app.use(cors({ credentials: true }));
  // attach our primary architecture
  app.use((ctx: KoaContext, next: Next) =>
    architectureMiddleware(ctx, next, {
      database: uploadDatabase,
      objectStore,
      cacheService,
      paymentService,
      arweaveGateway,
      getArweaveWallet,
      tracer,
    })
  );
  app.use(router.routes());
  const server = app.listen(port);
  server.keepAliveTimeout = 120_000; // intentionally larger than ALB idle timeout
  server.requestTimeout = 0; // disable request timeout

  globalLogger.info(`Listening on port ${port}...`);
  globalLogger.info(
    `Communicating with payment service at ${paymentService.paymentServiceURL}...`
  );
  globalLogger.info(`Keep alive is: ${server.keepAliveTimeout}`);
  globalLogger.info(`Request timeout is: ${server.requestTimeout}`);
  return server;
}
