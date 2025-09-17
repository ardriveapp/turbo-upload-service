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
import { getDynamoOffsetsInfo } from "../utils/dynamoDbUtils";

export async function offsetsHandler(ctx: KoaContext, next: Next) {
  const { logger } = ctx.state;

  try {
    const maybeOffsetsInfo = await getDynamoOffsetsInfo(ctx.params.id, logger);
    if (maybeOffsetsInfo === undefined) {
      ctx.status = 404;
      ctx.body = "TX doesn't exist";
      return next();
    }

    // TODO: Decide whether to use the database to help provide for longer cache durations (e.g. when data is permanent)
    const cacheControlAgeSeconds = 60;
    ctx.set("Cache-Control", `public, max-age=${cacheControlAgeSeconds}`);

    // Remove the dataItemId from the response
    const { dataItemId, ...offsetsInfo } = maybeOffsetsInfo;
    ctx.body = {
      ...offsetsInfo,
      payloadContentLength:
        offsetsInfo.rawContentLength - offsetsInfo.payloadDataStart,
    };
  } catch (error) {
    logger.error(`Error getting data item offsets: ${error}`);
    ctx.status = 503;
    ctx.body = "Internal Server Error";
  }

  return next();
}
