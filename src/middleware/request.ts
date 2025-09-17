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

import { KoaContext } from "../server";
import { InsufficientBalance } from "../utils/errors";

export async function requestMiddleware(ctx: KoaContext, next: Next) {
  const { logger } = ctx.state;
  ctx.req.on("close", () => {
    logger.debug("Request closed");
  });
  ctx.req.on("end", () => {
    logger.debug("Request ended");
  });
  ctx.req.on("error", (error) => {
    const msg = `Request error: ${error.message}`;
    if (error instanceof InsufficientBalance) {
      logger.warn(msg);
    } else {
      logger.error(msg, error);
    }
  });

  // response ending
  ctx.res.on("close", () => {
    logger.debug("Response closed");
    // cleanup any open streams
    ctx.req.destroy();
  });
  return next();
}
