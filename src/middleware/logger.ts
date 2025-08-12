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
import { randomUUID } from "crypto";
import { Next } from "koa";

import logger from "../logger";
import { KoaContext } from "../server";

export async function loggerMiddleware(ctx: KoaContext, next: Next) {
  const trace = randomUUID().substring(0, 6);

  let log = logger.child({
    trace,
    path: ctx.path,
    method: ctx.method,
    params: ctx.params,
  });

  log.debug("Request headers", { headers: ctx.headers });

  const headersToAddToLoggedMessages = {
    xForwardedFor: ctx.get("x-forwarded-for"),
    referer: ctx.get("referer"),
    userAgent: ctx.get("user-agent"),
    origin: ctx.get("origin"),
    xAmznTraceId: ctx.get("x-amzn-trace-id"),
    xAmzCloudfrontId: ctx.get("x-amz-cf-id"),
    xTurboSourceIdentifier: ctx.get("x-turbo-source-identifier"),
    xTurboSourceVersion: ctx.get("x-turbo-source-version"),
    contentLength: ctx.get("content-length"),
  };
  for (const [key, value] of Object.entries(headersToAddToLoggedMessages)) {
    if (value !== undefined && value !== "") {
      // Add the header to the log context if it exists
      log = log.child({ [key]: value });
    }
  }

  ctx.state.logger = log;
  ctx.state.trace = trace;

  // Skip logging for metrics and health checks
  if (ctx.path === "/bundler_metrics" || ctx.path === "/health") {
    return next();
  }

  const startTime = Date.now();
  log.debug("Received request.");
  await next();
  const duration = Date.now() - startTime;
  log.debug("Completed request.", {
    responseTime: `${duration}ms`,
  });
}
