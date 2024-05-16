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

export async function statusHandler(ctx: KoaContext, next: Next) {
  const { logger, database } = ctx.state;
  try {
    const info = await database.getDataItemInfo(ctx.params.id);
    if (info === undefined) {
      ctx.status = 404;
      ctx.body = "TX doesn't exist";
      return next();
    }

    ctx.body = {
      status:
        info.status === "permanent"
          ? "FINALIZED"
          : info.status === "failed"
          ? "FAILED"
          : "CONFIRMED",
      bundleId: info.bundleId,
      info: info.status,
      winc: info.assessedWinstonPrice,
      reason: info.failedReason,
    };
  } catch (error) {
    logger.error(`Error getting data item status: ${error}`);
    ctx.status;
    ctx.throw(503, "Internal Server Error");
  }

  return next();
}
