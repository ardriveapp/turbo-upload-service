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
import { GetParameterCommand, PutParameterCommand } from "@aws-sdk/client-ssm";
import fs from "fs/promises";
import knex, { Knex } from "knex";
import pLimit from "p-limit";
import path from "path";
import { EventEmitter } from "stream";
import winston from "winston";

import { columnNames, tableNames } from "../arch/db/dbConstants";
import { getReaderConfig } from "../arch/db/knexConfig";
import { ssmClient } from "../arch/ssmClient";
import { jobLabels } from "../constants";
import defaultLogger from "../logger";
import { PermanentDataItemDBResult, Timestamp } from "../types/dbTypes";
import { TransactionId } from "../types/types";
import { Deferred } from "../utils/deferred";
import { UPLOAD_DATA_PATH } from "../utils/fileSystemUtils";

const QUERY_BATCH_SIZE = 500;
const DELETE_CONCURRENCY_LIMIT = 8;
const MAX_ERROR_COUNT = 10;
const SSM_CURSOR_NAME = "fs-cleanup-last-deleted-cursor";
const DEFAULT_START_DATE = "2025-03-17T00:00:00";
let heartbeatTimer: NodeJS.Timeout | null = null;
type PermanentDataItem = Pick<
  PermanentDataItemDBResult,
  "data_item_id" | "uploaded_date"
>;

interface Cursor {
  uploadedAt: Timestamp;
  dataItemId: TransactionId | undefined;
}

async function getLastCursor(): Promise<Cursor> {
  try {
    const result = await ssmClient.send(
      new GetParameterCommand({ Name: SSM_CURSOR_NAME })
    );
    return result.Parameter?.Value
      ? JSON.parse(result.Parameter.Value)
      : { uploadedAt: DEFAULT_START_DATE, dataItemId: undefined };
  } catch {
    return { uploadedAt: DEFAULT_START_DATE, dataItemId: undefined };
  }
}

async function saveCursor(cursor: Cursor) {
  return ssmClient.send(
    new PutParameterCommand({
      Name: SSM_CURSOR_NAME,
      Value: JSON.stringify(cursor),
      Overwrite: true,
    })
  );
}

async function getNextBatch(
  knexClient: Knex,
  cursor: Cursor,
  cutoffTime: Date
): Promise<PermanentDataItem[]> {
  return knexClient<PermanentDataItem>(tableNames.permanentDataItems)
    .select(columnNames.dataItemId, columnNames.uploadedDate)
    .where(columnNames.uploadedDate, ">=", cursor.uploadedAt)
    .andWhere(columnNames.uploadedDate, "<=", cutoffTime.toISOString())
    .orderBy(columnNames.uploadedDate)
    .orderBy(columnNames.dataItemId)
    .limit(QUERY_BATCH_SIZE);
}

async function cleanupFsHandler({
  logger = defaultLogger.child({ job: jobLabels.cleanupFs }),
  knexClient,
  teardownComplete,
}: {
  logger?: winston.Logger;
  knexClient: Knex;
  teardownComplete: Deferred<void>;
}) {
  const startTimestamp = new Date();
  let deletedCount = 0;
  let errorCount = 0;
  let cursor = await getLastCursor();
  const HEARTBEAT_INTERVAL_MS = 15_000;
  const MAX_BATCHES = 5;
  const batchQueue: PermanentDataItem[][] = [];
  const fetchCoordinator = new EventEmitter();
  const workCoordinator = new EventEmitter();
  let isOutOfWorkToDo = false;
  let fetching = false;
  const fileLimit = pLimit(DELETE_CONCURRENCY_LIMIT);
  let fetchedBatchesCount = 0;

  function logProgress() {
    const elapsedSecs = Math.max(
      parseFloat(((Date.now() - startTimestamp.getTime()) / 1000).toFixed(3)),
      0.001 // Prevent division by zero
    );
    logger.info("Progress:", {
      deletedCount,
      errorCount,
      cursor,
      bufferedBatchesCount: batchQueue.length,
      fetchedBatchesCount,
      idsFetchedCount: fetchedBatchesCount * QUERY_BATCH_SIZE,
      elapsedSecs,
      fetchedBatchesPerSec: fetchedBatchesCount / elapsedSecs,
      deletesPerSec: deletedCount / elapsedSecs,
    });
  }

  function startHeartbeatLogger() {
    heartbeatTimer = setInterval(logProgress, HEARTBEAT_INTERVAL_MS);
  }

  function stopHeartbeatLogger() {
    if (heartbeatTimer) clearInterval(heartbeatTimer);
  }

  function teardown() {
    stopHeartbeatLogger();
    fetchCoordinator.removeAllListeners();
    workCoordinator.removeAllListeners();
    teardownComplete.resolve();
  }

  function nextBatch() {
    const batch = batchQueue.shift();
    fetchCoordinator.emit("canFetch");
    return batch;
  }

  // eslint-disable-next-line @typescript-eslint/no-misused-promises
  fetchCoordinator.on("canFetch", async () => {
    if (
      isOutOfWorkToDo || // worker will tear down
      fetching // already fetching so nothing more to do
    ) {
      return;
    }
    fetching = true;
    const cutoffTime = new Date(Date.now() - 24 * 60 * 60 * 1000); // 24 hours ago
    while (batchQueue.length < MAX_BATCHES) {
      let batch = await getNextBatch(knexClient, cursor, cutoffTime);
      logger.debug(`Unfiltered batch:`, {
        batch,
        cursor,
      });

      const batchHasRecentEntry = batch.some(
        (item) => new Date(item.uploaded_date) >= cutoffTime
      );
      if (batchHasRecentEntry) {
        batch = batch.filter(
          (item) => new Date(item.uploaded_date) < cutoffTime
        );
        isOutOfWorkToDo = true;
      }

      // Remove all entries with the same uploaded_date as the cursor and that sorted before the cursor's data_item_id
      batch = batch.filter((item) => {
        const itemDate = new Date(item.uploaded_date).getTime();
        const cursorDate = new Date(cursor.uploadedAt).getTime();
        return (
          itemDate !== cursorDate || // newer than the cursor's uploaded_date
          item.data_item_id > (cursor.dataItemId ?? "-") // newer than the cursor's data_item_id
        );
      });

      logger.debug(`Filtered batch:`, {
        batch,
        cursor,
      });

      // If no rows returned, we're done
      if (!batch.length) {
        isOutOfWorkToDo = true;
        break;
      }
      batchQueue.push(batch);
      fetchedBatchesCount++;
      const lastRow = batch[batch.length - 1];
      cursor = {
        uploadedAt: lastRow.uploaded_date,
        dataItemId: lastRow.data_item_id,
      };
      workCoordinator.emit("workReady");
    }
    fetching = false;

    // Give one last nudge to the worker in case it was waiting for work
    // when we finished fetching.
    workCoordinator.emit("workReady");
  });

  fetchCoordinator.on("error", (err) => {
    // Allow the work coordinator to handle teardown
    workCoordinator.emit("error", err);
  });

  let working = false;
  // eslint-disable-next-line @typescript-eslint/no-misused-promises
  workCoordinator.on("workReady", async () => {
    if (working) {
      // Worker is already processing a batch, so wait for it to finish.
      return;
    }

    working = true;

    let batch = nextBatch();
    while (batch && batch.length > 0) {
      // Use the latest uploaded_date from the batch for the next cursor.
      // Set it to the oldest in the batch if we encounter an error to improve
      // the (slim) chances of retrying it on a successive run of the job
      let batchCursor = {
        uploadedAt: batch[batch.length - 1].uploaded_date,
        dataItemId: batch[batch.length - 1].data_item_id,
      };
      await Promise.all(
        batch.flatMap((row) => {
          const baseDir = path.join(
            UPLOAD_DATA_PATH,
            row.data_item_id.slice(0, 2),
            row.data_item_id.slice(2, 4)
          );

          return ["raw_", "metadata_"].map((prefix) =>
            fileLimit(async () => {
              const filePath = path.join(
                baseDir,
                `${prefix}${row.data_item_id}`
              );
              try {
                await fs.unlink(filePath);
                deletedCount++;
                logger.debug(`Deleted: ${filePath}`);
              } catch (error: any) {
                if (error.code === "ENOENT") {
                  logger.warn(`Gone`, { path: filePath });
                } else {
                  logger.error(`Failed to delete!`, {
                    path: filePath,
                    error,
                  });
                  errorCount++;
                  batchCursor = {
                    uploadedAt: batch![0].uploaded_date,
                    dataItemId: batch![0].data_item_id,
                  };
                }
              }
            })
          );
        })
      );

      if (errorCount > MAX_ERROR_COUNT) {
        throw new Error(
          `Too many deletion errors encountered. Aborting after ${errorCount} errors.`
        );
      }

      await saveCursor(batchCursor);
      batch = nextBatch();
    }

    working = false;
    if (isOutOfWorkToDo) {
      if (batchQueue.length === 0) {
        logger.info(`✅ Cleanup complete!`, {
          deletedCount,
          errorCount,
          cursor,
        });
        teardown();
      } else {
        // Something went wrong, we still have work to do
        logger.error("Work still in queue, but no more work to do!", {
          bufferedBatchesCount: batchQueue.length,
          cursor,
        });
        workCoordinator.emit("workReady");
      }
    }
    // Otherwise expect the fetch coordinator to emit "workReady" again
  });

  workCoordinator.on("error", (error) => {
    logger.error("Error during processing!", { error });
    teardown();
  });

  // Kick off the system
  try {
    startHeartbeatLogger();
    fetchCoordinator.emit("canFetch");
  } catch (error) {
    logger.error("Error during processing!", { error });
    teardown();
  }
}

export async function handler(eventPayload?: unknown) {
  const knexClient = knex(getReaderConfig());
  const teardownComplete = new Deferred<void>();

  defaultLogger.info(`Cleanup job triggered with event payload:`, eventPayload);

  try {
    await cleanupFsHandler({
      logger: defaultLogger.child({ job: jobLabels.cleanupFs }),
      knexClient,
      teardownComplete,
    });
    await teardownComplete.promise;
  } finally {
    await knexClient.destroy();
  }
}
