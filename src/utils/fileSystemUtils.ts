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
import { randomBytes } from "crypto";
import { createReadStream, createWriteStream } from "fs";
import fs from "fs/promises";
import CircuitBreaker from "opossum";
import path from "path";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
import winston from "winston";

import { ConfigKeys, getConfigValue } from "../arch/remoteConfig";
import { quarantinePrefix } from "../constants";
import globalLogger from "../logger";
import {
  MetricRegistry,
  setUpCircuitBreakerListenerMetrics,
} from "../metricRegistry";
import { PayloadInfo, TransactionId } from "../types/types";
import { deserializePayloadInfo, minifyNestedDataItemInfo } from "./common";
import { Deferred } from "./deferred";

type FileSystemTask<T> = () => Promise<T>;

const fsBreaker = {
  breaker: new CircuitBreaker<[FileSystemTask<unknown>], unknown>(
    async (...args: [FileSystemTask<unknown>]) => {
      const [task] = args;
      return task();
    },
    {
      timeout: process.env.NODE_ENV === "local" ? 9_000 : 3000,
      errorThresholdPercentage: 10,
      resetTimeout: 30000,
    }
  ),
  fire<T>(task: FileSystemTask<T>): Promise<T> {
    return this.breaker.fire(task) as Promise<T>;
  },
};

setUpCircuitBreakerListenerMetrics("fsBackup", fsBreaker.breaker, globalLogger);

export function backupFsAvailable(): boolean {
  return !fsBreaker.breaker.opened;
}

export const UPLOAD_DATA_PATH = path.join(
  process.env.EFS_MOUNT_POINT ?? ".",
  "upload-service-data"
);

export async function ensureDataItemsBackupDirExists(): Promise<void> {
  await fsBreaker.fire(async () => {
    if (await fs.mkdir(UPLOAD_DATA_PATH, { recursive: true })) {
      globalLogger.info(
        `Created upload data directory at ${UPLOAD_DATA_PATH}.`
      );
    }
  });
}

isBackupFSNeeded()
  .then((needed) =>
    needed
      ? ensureDataItemsBackupDirExists().catch((error) => {
          globalLogger.error(
            `Failed to create upload data directory at ${UPLOAD_DATA_PATH}!`,
            { error }
          );
          throw error;
        })
      : Promise.resolve()
  )
  .catch((error) => {
    globalLogger.error(`Failed to determine if backup filesystem is needed!`, {
      error,
    });
    throw error;
  });

export function backupDirForDataItem(dataItemId: TransactionId): string {
  return `${UPLOAD_DATA_PATH}/${dataItemId.substring(
    0,
    2
  )}/${dataItemId.substring(2, 4)}`;
}

export async function ensureBackupDirForDataItem(
  dataItemId: TransactionId
): Promise<string> {
  const backupDir = backupDirForDataItem(dataItemId);
  await fsBreaker.fire(async () => {
    await fs.mkdir(backupDir, { recursive: true });
  });
  return backupDir;
}

export function filenameForRawDataItem({
  dataItemId,
  quarantine = false,
  backupDir = backupDirForDataItem(dataItemId),
}: {
  dataItemId: TransactionId;
  quarantine?: boolean;
  backupDir?: string;
}): string {
  return `${backupDir}/${quarantine ? "quarantine_" : ""}raw_${dataItemId}`;
}

export function filenameForMetadata({
  dataItemId,
  quarantine = false,
  backupDir = backupDirForDataItem(dataItemId),
}: {
  dataItemId: TransactionId;
  quarantine?: boolean;
  backupDir?: string;
}): string {
  return `${backupDir}/${
    quarantine ? "quarantine_" : ""
  }metadata_${dataItemId}`;
}

export async function fsBackupHasRawDataItem(
  dataItemId: TransactionId,
  backupDir: string = backupDirForDataItem(dataItemId),
  quarantine = false
): Promise<boolean> {
  return fsBreaker
    .fire(async () => {
      try {
        await fs.stat(
          filenameForRawDataItem({ dataItemId, backupDir, quarantine })
        );
        return true;
      } catch (err: any) {
        if (err.code === "ENOENT") {
          return false;
        }
        throw err;
      }
    })
    .catch((error) => {
      globalLogger.error(
        `Error checking for raw data item file for data item ID ${dataItemId}!`,
        { error }
      );
      return false;
    });
}

export async function fsBackupHasMetadata(
  dataItemId: TransactionId,
  backupDir: string = backupDirForDataItem(dataItemId),
  quarantine = false
): Promise<boolean> {
  return fsBreaker
    .fire(async () => {
      try {
        await fs.stat(
          filenameForMetadata({ dataItemId, backupDir, quarantine })
        );
        return true;
      } catch (err: any) {
        if (err.code === "ENOENT") {
          return false;
        }
        throw err;
      }
    })
    .catch((error) => {
      globalLogger.error(
        `Error checking for metadata file for data item ID ${dataItemId}!`,
        { error }
      );
      return false;
    });
}

export async function fsBackupHasDataItem(
  dataItemId: TransactionId,
  logger: winston.Logger = globalLogger,
  quarantine = false
): Promise<boolean> {
  if (!backupFsAvailable()) {
    logger.debug(
      `Backup FS is not available, skipping check for data item ID ${dataItemId}.`
    );
    return false;
  }

  const backupDir = backupDirForDataItem(dataItemId);
  try {
    const hasDataItem =
      (await fsBackupHasMetadata(dataItemId, backupDir, quarantine)) &&
      (await fsBackupHasRawDataItem(dataItemId, backupDir, quarantine));
    if (!hasDataItem) {
      logger.debug(`Data item ID ${dataItemId} not found in FS backup`);
    }
    return hasDataItem;
  } catch (error) {
    logger.error(
      `Error checking for data item ID ${dataItemId} in FS backup!`,
      { error }
    );
    return false;
  }
}

export async function fsBackupDataItemMetadata(
  dataItemId: TransactionId
): Promise<PayloadInfo> {
  const backupDir = backupDirForDataItem(dataItemId);
  return fsBreaker
    .fire(async () => {
      const metadata = await fs.readFile(
        `${backupDir}/metadata_${dataItemId}`,
        "utf8"
      );
      return deserializePayloadInfo(metadata);
    })
    .catch((error) => {
      globalLogger.error(
        `Error reading metadata file for data item ID ${dataItemId}!`,
        { error }
      );
      throw error;
    });
}

export async function fsBackupRawDataItemReadable({
  dataItemId,
  startOffset,
  endOffsetInclusive,
  quarantine = false,
}: {
  dataItemId: TransactionId;
  startOffset?: number;
  endOffsetInclusive?: number;
  quarantine?: boolean;
}): Promise<{
  readable: Readable;
}> {
  const backupDir = backupDirForDataItem(dataItemId);
  return fsBreaker.fire(async () =>
    Promise.resolve({
      readable: createReadStream(
        `${backupDir}/${
          quarantine ? quarantinePrefix + "_" : ""
        }raw_${dataItemId}`,
        {
          start: startOffset,
          end: endOffsetInclusive,
        }
      ),
    })
  );
}

/***
 * Helper function for backing up incoming small data items to durable file system (e.g. local or EFS)
 * Goals:
 * - Do not overwrite existing good data with bad data
 * - Write in parallel where possible
 * - Write streams to temp location, await for validity verification, then swap to final location
 * - Clean up as much as possible when things go wrong
 */
export async function writeStreamAndMetadataToFiles({
  inputStream,
  payloadContentType,
  payloadDataStart,
  dataItemId,
  logger,
  deferredIsValid,
}: {
  inputStream: Readable;
  payloadContentType: string;
  payloadDataStart: number;
  dataItemId: TransactionId;
  logger: winston.Logger;
  deferredIsValid: Deferred<boolean>;
}): Promise<void> {
  return fsBreaker.fire(async () => {
    const backupDir = await ensureBackupDirForDataItem(dataItemId);

    const rawFile = path.join(backupDir, `raw_${dataItemId}`);
    const metaFile = path.join(backupDir, `metadata_${dataItemId}`);

    // Use a "write to temp and then move" strategy to ensure write atomicity
    const tempSuffix = `.tmp.${randomBytes(6).toString("hex")}`;
    const tempRaw = `${rawFile}${tempSuffix}`;
    const tempMeta = `${metaFile}${tempSuffix}`;

    const metaContents = `${payloadContentType};${payloadDataStart}`;

    // NB: We'll wrap the temp metadata and raw data item writes in promises so they can run concurrently

    const safelyCleanUpTempMetadata = async () => {
      logger.error(
        `Removing temp metadata for ${dataItemId} at ${tempMeta} from backup FS...`
      );
      try {
        await fs.rm(tempMeta, { force: true });
      } catch (error) {
        logger.error(
          `Failed to clean up temp metadata file ${tempMeta}`,
          error
        );
      }
    };

    const safelyCleanUpTempRawDataItem = async () => {
      logger.error(
        `Removing temp data item for ${dataItemId} at ${tempRaw} from backup FS...`
      );
      try {
        await fs.rm(tempRaw, { force: true });
      } catch (error) {
        logger.error(`Failed to clean up temp raw file ${tempRaw}`, error);
      }
    };

    // Write metadata to temp file
    const tempMetadataWritePromise = (async () => {
      try {
        await fs.writeFile(tempMeta, metaContents, "utf8");
      } catch (err) {
        await safelyCleanUpTempMetadata();
        throw err;
      }
    })();

    // Start raw stream write, but don't await it yet
    const tempRawWritePromise = (async () => {
      try {
        await pipeline(inputStream, createWriteStream(tempRaw));
      } catch (error) {
        await safelyCleanUpTempRawDataItem();
        throw error;
      }
    })();

    // Let thrown errors from either of these propagate up. They clean themselves up along the way.
    await tempMetadataWritePromise;
    await tempRawWritePromise;

    // Catch finalization errors in the next part of the workflow so we can clean up temp files before propagating the error
    try {
      // Only finalize the data item and its metadata if it's valid (computed elsewhere in a separate stream)
      const isValid = await deferredIsValid?.promise;
      if (isValid) {
        // Wait for metadata to be finalized before finalizing raw since we'll need it to make use of the raw data item
        await fs.rename(tempMeta, metaFile);

        // The returned promise will be for the final rename move
        return fs.rename(tempRaw, rawFile).catch(async (error) => {
          // Technically only need to clean up the raw file here since the metadata was already moved and was valid
          // and will be cleaned up by automated processes later
          await safelyCleanUpTempRawDataItem();
          throw error;
        });
      }
      logger.error(
        `Data item ${dataItemId} is not valid! Not finalizing in backup FS.`
      );

      // Don't throw since we've streamed cleanly and done everything we can to clean up
    } catch (error) {
      await safelyCleanUpTempRawDataItem();
      await safelyCleanUpTempMetadata();
      throw error;
    }
  });
}

export async function quarantineDataItemFromBackupFs({
  dataItemId,
  logger,
}: {
  dataItemId: TransactionId;
  logger: winston.Logger;
}) {
  await fsBreaker.fire(async () => {
    const backupDir = backupDirForDataItem(dataItemId);
    if (await fsBackupHasRawDataItem(dataItemId, backupDir)) {
      logger.info(`Quarantining data item ${dataItemId} in FS backup...`);
      try {
        await fs.rename(
          filenameForRawDataItem({ dataItemId, backupDir }),
          filenameForRawDataItem({ dataItemId, backupDir, quarantine: true })
        );
        MetricRegistry.fsBackupQuarantineSuccess.inc();
      } catch (error) {
        logger.error(
          `Error quarantining raw data item ${dataItemId} in FS backup
        `,
          { error }
        );
        MetricRegistry.fsBackupQuarantineFailure.inc();
      }
      if (await fsBackupHasMetadata(dataItemId, backupDir)) {
        try {
          await fs.rename(
            filenameForMetadata({ dataItemId, backupDir }),
            filenameForMetadata({ dataItemId, backupDir, quarantine: true })
          );
        } catch (error) {
          logger.error(
            `Error quarantining metadata ${dataItemId} in FS backup`,
            {
              error,
            }
          );
        }
        logger.info(
          `Quarantined (best effort) data item ${dataItemId} in FS backup.`
        );
      }
    } else {
      logger.info(
        `Data item ${dataItemId} not found in FS backup. Skipping quarantine.`
      );
    }
  });
}

export async function fsBackupNestedDataItemInfo({
  dataItemId,
  parentDataItemId,
  parentPayloadDataStart,
  startOffsetInRawParent,
  rawContentLength,
  payloadContentType,
  payloadDataStart,
  logger,
}: {
  dataItemId: TransactionId;
  parentDataItemId: TransactionId;
  parentPayloadDataStart: number;
  startOffsetInRawParent: number;
  rawContentLength: number;
  payloadContentType: string;
  payloadDataStart: number;
  logger: winston.Logger;
}) {
  const payloadBuffer = Buffer.from(
    JSON.stringify(
      minifyNestedDataItemInfo({
        parentDataItemId,
        parentPayloadDataStart,
        startOffsetInRawParent,
        rawContentLength,
        payloadContentType,
        payloadDataStart,
      })
    ),
    "utf8"
  );
  return fsBreaker.fire(async () => {
    // Use a "write to temp and then move" strategy to ensure write atomicity
    const backupDir = await ensureBackupDirForDataItem(dataItemId);
    const offsetsFile = path.join(backupDir, `offsets_${dataItemId}`);
    const tempSuffix = `.tmp.${randomBytes(6).toString("hex")}`;
    const tempOffsetsFile = `${offsetsFile}${tempSuffix}`;

    const safelyCleanUpTempOffsetsFile = async () => {
      logger.error(
        `Removing temp offsets file for ${dataItemId} at ${tempOffsetsFile} from backup FS...`
      );
      try {
        await fs.rm(tempOffsetsFile, { force: true });
      } catch (error) {
        logger.error(
          `Failed to clean up temp offsets file ${tempOffsetsFile}`,
          { error }
        );
      }
    };

    try {
      logger.debug(
        `Storing nested data item offsets for ${dataItemId} in backup FS...`
      );
      await fs.writeFile(tempOffsetsFile, payloadBuffer, "utf8");
      await fs.rename(tempOffsetsFile, offsetsFile);
    } catch (err) {
      await safelyCleanUpTempOffsetsFile();
      throw err;
    }
  });
}

export async function isBackupFSNeeded(): Promise<boolean> {
  return (
    (await getConfigValue(ConfigKeys.fsBackupWriteDataItemSamplingRate)) > 0 ||
    (await getConfigValue(ConfigKeys.fsBackupWriteNestedDataItemSamplingRate)) >
      0
  );
}
