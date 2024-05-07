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
import { Next } from "koa";
import Router from "koa-router";
import * as promClient from "prom-client";

import { MetricRegistry } from "./metricRegistry";
import { dataItemRoute } from "./routes/dataItemPost";
import { rootResponse } from "./routes/info";
import {
  createMultiPartUpload,
  finalizeMultipartUploadWithHttpRequest,
  getMultipartUpload,
  getMultipartUploadStatus,
  postDataItemChunk,
} from "./routes/multiPartUploads";
import { statusHandler } from "./routes/status";
import { swaggerDocs, swaggerDocsJSON } from "./routes/swagger";
import { KoaContext } from "./server";

const metricsRegistry = MetricRegistry.getInstance().getRegistry();
promClient.collectDefaultMetrics({ register: metricsRegistry });

const router = new Router();

// post routes
router.post("/v1/tx", dataItemRoute);
router.post("/tx", dataItemRoute);
router.post("/v1/tx/:currency", dataItemRoute);
router.post("/tx/:currency", dataItemRoute);

/**
 * START TEMPORARY PATCH TO SUPPORT up.arweave.net
 */
router.get("/price/:foo/:bar", (ctx, next: Next) => {
  ctx.body = "0.0000000000000";
  return next();
});

router.get("/price/:bar", (ctx, next: Next) => {
  ctx.body = "0.0000000000000";
  return next();
});

router.get("/account/balance/:rest", (ctx: KoaContext, next: Next) => {
  ctx.body = "99999999999999999999999999999999999999";
  return next();
});
/**
 * END TEMPORARY PATCH TO SUPPORT up.arweave.net
 */

// publish at root for backwards compatibility (blunder)
router.get("/tx/:id/status", statusHandler);
// publish at v1 for forwards compatibility 🧠
router.get("/v1/tx/:id/status", statusHandler);

// Multi-part upload routes
router.get("/chunks/:token/-1/-1", createMultiPartUpload);
router.get("/chunks/:token/:uploadId/-1", getMultipartUpload);
router.get("/chunks/:token/:uploadId/status", getMultipartUploadStatus);
router.post(
  "/chunks/:token/:uploadId/-1",
  finalizeMultipartUploadWithHttpRequest
);
router.post("/chunks/:token/:uploadId/finalize", (ctx: KoaContext) => {
  ctx.state.asyncValidation = true;
  return finalizeMultipartUploadWithHttpRequest(ctx);
});
router.post("/chunks/:token/:uploadId/:chunkOffset", postDataItemChunk);

// info routes
router.get("/", rootResponse);
router.get("/info", rootResponse);

// Prometheus
router.get("/bundler_metrics", async (ctx: KoaContext, next: Next) => {
  ctx.body = await metricsRegistry.metrics();
  return next();
});

// healthcheck
router.get("/health", (ctx: KoaContext, next: Next) => {
  ctx.body = "OK";
  return next();
});

// Swagger
router.get("/openapi.json", swaggerDocsJSON);
router.get("/api-docs", swaggerDocs);

export default router;
