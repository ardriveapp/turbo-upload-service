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
import { Next } from "koa";
import Router from "koa-router";
import * as promClient from "prom-client";

import { receiptVersion } from "./constants";
import { MetricRegistry } from "./metricRegistry";
import { dataItemRoute } from "./routes/dataItemPost";
import { swaggerDocs, swaggerDocsJSON } from "./routes/swagger";
import { KoaContext } from "./server";
import { jwkToPublicArweaveAddress } from "./utils/base64";

const metricsRegistry = MetricRegistry.getInstance().getRegistry();
promClient.collectDefaultMetrics({ register: metricsRegistry });

const router = new Router();

// post routes
router.post("/v1/tx", dataItemRoute);
router.post("/tx", dataItemRoute);
router.post("/v1/tx/:currency", dataItemRoute);
router.post("/tx/:currency", dataItemRoute);

router.get("/health", (ctx: KoaContext, next: Next) => {
  ctx.body = "OK";
  return next();
});

router.get("/", async (ctx: KoaContext, next: Next) => {
  const signingWalletAddress = jwkToPublicArweaveAddress(
    await ctx.state.getArweaveWallet()
  );
  ctx.body = {
    version: receiptVersion,
    addresses: {
      arweave: signingWalletAddress,
    },
    gateway: ctx.state.arweaveGateway["endpoint"].hostname,
  };
  return next();
});

// Prometheus
router.get("/bundler_metrics", async (ctx: KoaContext, next: Next) => {
  ctx.body = await metricsRegistry.metrics();
  return next();
});

// Swagger
router.get("/openapi.json", swaggerDocsJSON);
router.get("/api-docs", swaggerDocs);

export default router;
