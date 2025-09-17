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
import winston from "winston";

import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { EnqueuedNewDataItem } from "../arch/queues";
import { fromB64Url } from "../utils/base64";

export async function newDataItemBatchInsertHandler({
  dataItemBatch,
  logger,
  uploadDatabase = new PostgresDatabase(),
}: {
  logger: winston.Logger;
  dataItemBatch: EnqueuedNewDataItem[];
  uploadDatabase?: Database;
}): Promise<void> {
  logger.debug(`Inserting new data items.`, {
    dataItemBatchLength: dataItemBatch.length,
  });

  const batchWithSignatureBuffered = dataItemBatch.map((dataItem) => {
    return {
      ...dataItem,
      signature: fromB64Url(dataItem.signature),
    };
  });
  await uploadDatabase.insertNewDataItemBatch(batchWithSignatureBuffered);

  logger.debug(`Inserted new data items!`, {
    dataItemBatchLength: dataItemBatch.length,
  });
  logger.debug(`Batch Ids`, {
    dataItemBatch: dataItemBatch.map((dataItem) => dataItem.dataItemId),
  });
}
