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
import cors from "@koa/cors";
import Koa, { DefaultState, Next, ParameterizedContext } from "koa";

import { Architecture, defaultArchitecture } from "./arch/architecture";
import { port as defaultPort } from "./constants";
import globalLogger from "./logger";
import { MetricRegistry } from "./metricRegistry";
import {
  architectureMiddleware,
  loggerMiddleware,
  requestMiddleware,
} from "./middleware";
import router from "./router";

type KoaState = DefaultState & Architecture;
export type KoaContext = ParameterizedContext<KoaState>;

globalLogger.info(
  `Starting server with node environment ${process.env.NODE_ENV}...`
);

// global error handler
process.on("uncaughtException", (error) => {
  MetricRegistry.uncaughtExceptionCounter.inc();
  globalLogger.error("Uncaught exception:", error);
});

export function createServer(
  arch: Partial<Architecture>,
  port: number = defaultPort
) {
  const app = new Koa();

  const uploadDatabase = arch.database ?? defaultArchitecture.database;
  const objectStore = arch.objectStore ?? defaultArchitecture.objectStore;
  const paymentService =
    arch.paymentService ?? defaultArchitecture.paymentService;
  const logger = arch.logger ?? defaultArchitecture.logger;
  const getArweaveWallet =
    arch.getArweaveWallet ?? defaultArchitecture.getArweaveWallet;
  const arweaveGateway =
    arch.arweaveGateway ?? defaultArchitecture.arweaveGateway;

  // attach logger to context including trace id
  app.use(loggerMiddleware);
  // attaches listeners related to request streams for debugging
  app.use(requestMiddleware);
  app.use(cors({ allowMethods: "POST" }));
  // attach our primary architecture
  app.use((ctx: KoaContext, next: Next) =>
    architectureMiddleware(ctx, next, {
      database: uploadDatabase,
      logger,
      objectStore,
      paymentService,
      arweaveGateway,
      getArweaveWallet,
    })
  );
  app.use(router.routes());
  const server = app.listen(port);
  server.keepAliveTimeout = 120_000; // intentionally larger than ALB idle timeout
  server.requestTimeout = 0; // disable request timeout

  logger.info(`Listening on port ${port}...`);
  logger.info(`Keep alive is: ${server.keepAliveTimeout}`);
  logger.info(`Request timeout is: ${server.requestTimeout}`);
  return server;
}
