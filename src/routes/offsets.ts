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
import { TransactionId } from "../types/types";
import { setCacheControlHeadersForDataItemInfo } from "../utils/cacheControl";
import { getDynamoOffsetsInfo } from "../utils/dynamoDbUtils";
import { dataItemInfoCache } from "../utils/infoCache";

export async function offsetsHandler(ctx: KoaContext, next: Next) {
  if (!ctx.params.id) {
    ctx.status = 400;
    ctx.body = "Data item ID not specified";
    return next();
  }

  const dataItemId = ctx.params.id as TransactionId;
  const { logger, database } = ctx.state;

  try {
    // Fetch db info and offsets info concurrently
    const [maybeOffsetsInfo, maybeInfo] = await Promise.all([
      getDynamoOffsetsInfo(dataItemId, logger),
      dataItemInfoCache.get(dataItemId, { database, logger }),
    ]);

    if (maybeOffsetsInfo === undefined) {
      ctx.status = 404;
      ctx.body = "TX doesn't exist";
      return next();
    }

    setCacheControlHeadersForDataItemInfo(ctx, maybeInfo);

    logger.debug(
      `Status age: ${
        maybeInfo ? Date.now() - maybeInfo.uploadedTimestamp : "unknown"
      }`,
      {
        context: "offsets",
      }
    );

    // Remove the dataItemId from the response
    const { dataItemId: _, ...offsetsInfo } = maybeOffsetsInfo;
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
