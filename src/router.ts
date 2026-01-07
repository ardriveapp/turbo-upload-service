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
import { offsetsHandler } from "./routes/offsets";
import { statusHandler } from "./routes/status";
import { swaggerDocs, swaggerDocsJSON } from "./routes/swagger";
import { x402SignedDataItemRoute } from "./routes/x402DataItemPost";
import { x402RawDataPostRoute } from "./routes/x402RawDataPost";
import { KoaContext } from "./server";

const metricsRegistry = MetricRegistry.getInstance().getRegistry();
promClient.collectDefaultMetrics({ register: metricsRegistry });

const router = new Router();

// Publish to both root and v1 for convenience, but root might be deprecated or iterate in the future
const serveRoutesAndV1 = (path: string[]) =>
  path.flatMap((p) => [p, `/v1${p}`]);

// Raw data post routes
router.post(serveRoutesAndV1(["/tx", "/tx/:token"]), dataItemRoute);
router.post(
  serveRoutesAndV1([
    "/x402/data-item/unsigned",
    "/x402/data-item/unsigned/:network",
  ]),
  x402RawDataPostRoute
);
router.post(
  serveRoutesAndV1([
    "/x402/data-item/signed",
    "/x402/data-item/signed/:network",
  ]),
  x402SignedDataItemRoute
);

/**
 * START TEMPORARY PATCH TO SUPPORT up.arweave.net
 */
router.get(["/price/:foo/:bar", "/price/:bar"], (ctx, next: Next) => {
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

// Status routes
router.get(serveRoutesAndV1(["/tx/:id/status"]), statusHandler);
router.get(serveRoutesAndV1(["/tx/:id/offsets"]), offsetsHandler);

// Multi-part upload routes
router.get(serveRoutesAndV1(["/chunks/:token/-1/-1"]), createMultiPartUpload);
router.get(
  serveRoutesAndV1(["/chunks/:token/:uploadId/-1"]),
  getMultipartUpload
);
router.get(
  serveRoutesAndV1(["/chunks/:token/:uploadId/status"]),
  getMultipartUploadStatus
);
router.post(
  serveRoutesAndV1(["/chunks/:token/:uploadId/-1"]),
  finalizeMultipartUploadWithHttpRequest
);
router.post(
  serveRoutesAndV1(["/chunks/:token/:uploadId/finalize"]),
  (ctx: KoaContext) => {
    ctx.state.asyncValidation = true;
    return finalizeMultipartUploadWithHttpRequest(ctx);
  }
);
router.post(
  serveRoutesAndV1(["/chunks/:token/:uploadId/:chunkOffset"]),
  postDataItemChunk
);

// info routes
router.get(serveRoutesAndV1(["/", "/info"]), rootResponse);

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
