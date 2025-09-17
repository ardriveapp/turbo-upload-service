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
import { byteArrayToLong } from "@dha-team/arbundles";
import { EventEmitter, PassThrough, Readable, once } from "stream";
import winston from "winston";

import { CacheService } from "../arch/cacheServiceTypes";
import { Database } from "../arch/db/database";
import { ObjectStore } from "../arch/objectStore";
import { ConfigKeys, getConfigValue } from "../arch/remoteConfig";
import { bundleHeaderInfoFromBuffer } from "../bundles/assembleBundleHeader";
import {
  DataItemInterface,
  StreamingDataItem,
} from "../bundles/streamingDataItem";
import { DataItemOffsets } from "../constants";
import globalLogger from "../logger";
import { MetricRegistry } from "../metricRegistry";
import { PayloadInfo, TransactionId } from "../types/types";
import {
  cacheHasDataItem,
  cacheNestedDataItemInfo,
  cacheServiceIsAvailable,
  cacheSmallDataItem,
  cachedDataItemMetadata,
  cachedDataItemReadableRange,
  cachedRawDataItem,
  quarantineCachedDataItem,
} from "./cacheServiceUtils";
import { shouldSampleIn, sleep, tapStream } from "./common";
import { Deferred } from "./deferred";
import {
  dynamoAvailable,
  dynamoHasDataItem,
  dynamoPayloadInfo,
  dynamoReadableRange,
  putDynamoDataItem,
  putDynamoOffsetsInfo,
} from "./dynamoDbUtils";
import {
  backupFsAvailable,
  fsBackupDataItemMetadata,
  fsBackupHasDataItem,
  fsBackupNestedDataItemInfo,
  fsBackupRawDataItemReadable,
  quarantineDataItemFromBackupFs,
  writeStreamAndMetadataToFiles,
} from "./fileSystemUtils";
import {
  dataItemPrefix,
  getDataItemObjectReadableRange,
  getDataItemPayloadInfo,
  getRawSignatureOfDataItemFromObjStore,
  getSignatureTypeOfDataItemFromObjStore,
  putDataItemRaw,
  rawDataItemObjectExists,
} from "./objectStoreUtils";
import { streamToBuffer } from "./streamToBuffer";
import { drainStream } from "./streamUtils";
import { TaskCounter } from "./taskCounter";

async function shouldUseCacheService(): Promise<boolean> {
  const cacheServiceSamplingRate = await getConfigValue(
    ConfigKeys.cacheReadDataItemSamplingRate
  );
  return (
    cacheServiceSamplingRate >= 1 ||
    (cacheServiceSamplingRate > 0.0 &&
      Math.random() <= cacheServiceSamplingRate)
  );
}

export async function getSignatureTypeOfDataItem(
  objectStore: ObjectStore,
  cacheService: CacheService,
  dataItemId: TransactionId,
  logger: winston.Logger = globalLogger
): Promise<number> {
  // Try cache service first
  if (
    (await shouldUseCacheService()) &&
    (await cacheHasDataItem({ cacheService, dataItemId, logger }))
  ) {
    try {
      const rawDataItem = await cachedRawDataItem(cacheService, dataItemId);
      return byteArrayToLong(
        rawDataItem.subarray(
          DataItemOffsets.signatureTypeStart,
          DataItemOffsets.signatureTypeEnd + 1 // subarray end is exclusive so add 1
        )
      );
    } catch (error) {
      // Gracefully handle cache service invariance
      logger.error(
        `Failed to get signature type for dataitem ID ${dataItemId} from cache service`,
        error
      );
    }
  }

  // Fall back to file system
  if (await fsBackupHasDataItem(dataItemId, logger)) {
    try {
      const fsSignatureTypeReadable = await fsBackupRawDataItemReadable({
        dataItemId,
        startOffset: DataItemOffsets.signatureTypeStart,
        endOffsetInclusive: DataItemOffsets.signatureTypeEnd,
      });
      const signatureType = byteArrayToLong(
        await streamToBuffer(fsSignatureTypeReadable.readable)
      );
      logger.debug(
        `Got signature type for dataitem ID ${dataItemId} from FS: ${signatureType}`
      );
      return signatureType;
    } catch (error) {
      logger.error(
        `Error reading signature type for data item ID ${dataItemId} from FS backup`,
        { error }
      );
    }
  } else {
    logger.error(
      `No signature type found for dataitem ID ${dataItemId} in FS backup`
    );
  }

  // Next try DynamoDB
  if (await dynamoHasDataItem(dataItemId, logger)) {
    try {
      const readable = await dynamoReadableRange({
        dataItemId,
        start: DataItemOffsets.signatureTypeStart,
        inclusiveEnd: DataItemOffsets.signatureTypeEnd,
        logger,
      });
      if (readable) {
        const buf = await streamToBuffer(readable);
        return byteArrayToLong(buf);
      }
    } catch (error) {
      logger.error(
        `Error reading signature type for data item ID ${dataItemId} from DynamoDB`,
        { error }
      );
    }
  }

  // Final fallback is object store
  return getSignatureTypeOfDataItemFromObjStore(objectStore, dataItemId);
}

export async function getRawSignatureOfDataItem(
  objectStore: ObjectStore,
  cacheService: CacheService,
  dataItemId: TransactionId,
  signatureType: number,
  logger: winston.Logger = globalLogger
): Promise<Readable> {
  // Try cache service first
  if (
    (await shouldUseCacheService()) &&
    (await cacheHasDataItem({ cacheService, dataItemId, logger }))
  ) {
    try {
      const rawDataItem = await cachedRawDataItem(cacheService, dataItemId);
      return Readable.from(
        rawDataItem.subarray(
          DataItemOffsets.signatureStart,
          DataItemOffsets.signatureEnd(signatureType)
        )
      );
    } catch (error) {
      // Gracefully handle cache service invariance
      logger.error(
        `Failed to get raw signature for dataitem ID ${dataItemId} from cache service`,
        error
      );
    }
  }

  // Fall back to file system
  if (await fsBackupHasDataItem(dataItemId, logger)) {
    try {
      const fsSignatureReadable = await fsBackupRawDataItemReadable({
        dataItemId,
        startOffset: DataItemOffsets.signatureStart,
        endOffsetInclusive: DataItemOffsets.signatureEnd(signatureType),
      });
      logger.debug(
        `Got raw signature for dataitem ID ${dataItemId} from FS with startOffset ${
          DataItemOffsets.signatureStart
        } end offset ${DataItemOffsets.signatureEnd(signatureType)}`
      );
      return fsSignatureReadable.readable;
    } catch (error) {
      logger.error(
        `Error reading raw signature for data item ID ${dataItemId} from FS backup`,
        { error }
      );
    }
  } else {
    logger.error(
      `No raw signature found for dataitem ID ${dataItemId} in FS backup`
    );
  }

  if (await dynamoHasDataItem(dataItemId, logger)) {
    try {
      const readable = await dynamoReadableRange({
        dataItemId,
        start: DataItemOffsets.signatureStart,
        inclusiveEnd: DataItemOffsets.signatureEnd(signatureType),
        logger,
      });
      if (readable) {
        return readable;
      }
    } catch (error) {
      logger.error(
        `Error reading raw signature for data item ID ${dataItemId} from DynamoDB`,
        { error }
      );
    }
  }

  // Final fallback is object store
  return getRawSignatureOfDataItemFromObjStore(
    objectStore,
    dataItemId,
    signatureType
  );
}

export async function getDataItemMetadata({
  dataItemId,
  cacheService,
  objectStore,
  logger = globalLogger,
}: {
  dataItemId: TransactionId;
  cacheService: CacheService;
  objectStore: ObjectStore;
  logger?: winston.Logger;
}): Promise<PayloadInfo> {
  // Try cache service first
  if (
    (await shouldUseCacheService()) &&
    (await cacheHasDataItem({ cacheService, dataItemId, logger }))
  ) {
    try {
      const cachedMetadata = await cachedDataItemMetadata(
        cacheService,
        dataItemId
      );
      if (cachedMetadata) {
        return cachedMetadata;
      }
    } catch (error) {
      // Gracefully handle cache service invariance
      logger.error(
        `Failed to get metadata for dataitem ID ${dataItemId} from cache service`,
        error
      );
    }
  }

  // Fall back to file system
  if (await fsBackupHasDataItem(dataItemId, logger)) {
    try {
      const fsMetadata = await fsBackupDataItemMetadata(dataItemId);
      if (fsMetadata) {
        logger.debug(
          `Got metadata for dataitem ID ${dataItemId} from FS: ${JSON.stringify(
            fsMetadata,
            null,
            2
          )}`
        );
        return fsMetadata;
      }
    } catch (error) {
      logger.error(
        `Error reading metadata for data item ID ${dataItemId} from FS backup`,
        { error }
      );
    }
  } else {
    logger.error(
      `No metadata found for dataitem ID ${dataItemId} in FS backup`
    );
  }

  if (await dynamoHasDataItem(dataItemId, logger)) {
    try {
      const info = await dynamoPayloadInfo({ dataItemId, logger });
      if (info) {
        logger.debug(
          `Got metadata for dataitem ID ${dataItemId} from DynamoDB: ${JSON.stringify(
            info
          )}`
        );
        return info;
      }
    } catch (error) {
      logger.error(
        `Error reading metadata for data item ID ${dataItemId} from DynamoDB`,
        { error }
      );
    }
  }

  // Final fallback is object store
  const payloadInfo = await getDataItemPayloadInfo(
    objectStore,
    dataItemId
  ).catch((error) => {
    logger.error(
      `Failed to get metadata for dataitem ID ${dataItemId} from object store`,
      error
    );
    throw error;
  });
  return payloadInfo;
}

export async function getPayloadOfDataItem({
  dataItemId,
  cacheService,
  objectStore,
  logger = globalLogger,
}: {
  dataItemId: TransactionId;
  cacheService: CacheService;
  objectStore: ObjectStore;
  logger?: winston.Logger;
}): Promise<Readable> {
  const { payloadDataStart } = await getDataItemMetadata({
    dataItemId,
    cacheService,
    objectStore,
    logger,
  });
  // Try cache service first
  if (
    (await shouldUseCacheService()) &&
    (await cacheHasDataItem({ cacheService, dataItemId, logger }))
  ) {
    try {
      const cachedReadable = await cachedDataItemReadableRange({
        cacheService,
        dataItemId,
        startOffset: payloadDataStart,
      });
      if (cachedReadable) {
        return cachedReadable.readable;
      }
    } catch (error) {
      // Gracefully handle cache service invariance
      logger.error(
        `Failed to get payload for dataitem ID ${dataItemId} from cache service`,
        error
      );
    }
  }
  // Fall back to file system
  if (await fsBackupHasDataItem(dataItemId, logger)) {
    try {
      const fsReadable = await fsBackupRawDataItemReadable({
        dataItemId,
        startOffset: payloadDataStart,
      });
      if (fsReadable) {
        logger.debug(
          `Got payload for dataitem ID ${dataItemId} from FS at payload offset ${payloadDataStart}`
        );
        return fsReadable.readable;
      }
    } catch (error) {
      logger.error(
        `Error reading payload for data item ID ${dataItemId} from FS backup`,
        { error }
      );
    }
  } else {
    logger.error(`No payload found for dataitem ID ${dataItemId} in FS backup`);
  }

  if (await dynamoHasDataItem(dataItemId, logger)) {
    try {
      const readable = await dynamoReadableRange({
        dataItemId,
        start: payloadDataStart,
        logger,
      });
      if (readable) {
        logger.debug(
          `Got payload for dataitem ID ${dataItemId} from DynamoDB at payload offset ${payloadDataStart}`
        );
        return readable;
      }
    } catch (error) {
      logger.error(
        `Error reading payload for data item ID ${dataItemId} from DynamoDB`,
        { error }
      );
    }
  }

  // Final fallback is object store
  return getDataItemObjectReadableRange({
    objectStore,
    dataItemId,
    startOffset: payloadDataStart,
  });
}

export async function getDataItemReadableRange({
  cacheService,
  objectStore,
  dataItemId,
  startOffset,
  endOffsetInclusive,
  logger = globalLogger,
}: {
  cacheService: CacheService;
  objectStore: ObjectStore;
  dataItemId: TransactionId;
  startOffset: number; // Relative to the root of the raw data item (i.e. the start of its headers)
  endOffsetInclusive: number; // Also relative to the root of the raw data item
  logger?: winston.Logger;
}): Promise<Readable> {
  if (
    (await shouldUseCacheService()) &&
    (await cacheHasDataItem({ cacheService, dataItemId, logger }))
  ) {
    try {
      const { readable } = await cachedDataItemReadableRange({
        cacheService,
        dataItemId,
        startOffset,
        endOffsetInclusive,
      });
      return readable;
    } catch (error) {
      // Gracefully handle cache service invariance
      logger.error(
        `Failed to get ranged readable for dataitem ID ${dataItemId} from cache service`,
        {
          startOffset,
          endOffsetInclusive,
          error,
        }
      );
    }
  }

  // Fall back to file system
  if (await shouldReadFromBackupFS()) {
    if (await fsBackupHasDataItem(dataItemId, logger)) {
      try {
        const { readable } = await fsBackupRawDataItemReadable({
          dataItemId,
          startOffset,
          endOffsetInclusive,
        });
        logger.debug(
          `Got readable for dataitem ID ${dataItemId} from FS with range from start offset ${startOffset} to end offset ${endOffsetInclusive}`
        );
        return readable;
      } catch (error) {
        logger.error(
          `Error reading ranged readable for data item ID ${dataItemId} from FS backup`,
          { startOffset, endOffsetInclusive, error }
        );
      }
    } else {
      logger.error(
        `No readable found for dataitem ID ${dataItemId} in FS backup`
      );
    }
  }

  if (await dynamoHasDataItem(dataItemId, logger)) {
    try {
      const readable = await dynamoReadableRange({
        dataItemId,
        start: startOffset,
        inclusiveEnd: endOffsetInclusive,
        logger,
      });
      if (readable) {
        return readable;
      }
    } catch (error) {
      logger.error(
        `Error reading ranged readable for data item ID ${dataItemId} from DynamoDB`,
        { startOffset, endOffsetInclusive, error }
      );
    }
  }

  // Final fallback is object store
  return getDataItemObjectReadableRange({
    objectStore,
    dataItemId,
    startOffset,
    endOffsetInclusive,
  });
}

export async function dataItemExists(
  dataItemId: TransactionId,
  cacheService: CacheService,
  objectStore: ObjectStore,
  logger: winston.Logger = globalLogger
): Promise<boolean> {
  return (
    // Check in most likely to least likely order
    // 99%+ of items are in cache
    ((await shouldUseCacheService()) &&
      (await cacheHasDataItem({ cacheService, dataItemId, logger }))) ||
    // Anything not in cache is more likely to be in S3 than dynamo (e.g. larger items and fallback scenarios)
    (await rawDataItemObjectExists(objectStore, dataItemId)) ||
    // DynamoDB is a last resort for sufficiently small items
    (await dynamoHasDataItem(dataItemId, logger)) ||
    // File system is an exotic case
    (await fsBackupHasDataItem(dataItemId, logger))
  );
}

export interface DataItemAttributes {
  dataItemId: TransactionId;
  rawDataItemSize: number;
  payloadDataStartOffset: number;
  payloadContentType: string;
  rawDataItemOffsetInBundle: number;
}

export interface BundlePayloadResult {
  payloadReadable: Readable;
  dataItemAttributesPromise: Promise<DataItemAttributes[]>;
}

export function assembleBundlePayload(
  objectStore: ObjectStore,
  cacheService: CacheService,
  bundleHeaderBuffer: Buffer,
  logger: winston.Logger
): BundlePayloadResult {
  const bundleHeaderInfo = bundleHeaderInfoFromBuffer(bundleHeaderBuffer);

  const activeStreamsMap = new Map<string, Readable>();
  function cleanupActiveDataItemStreams() {
    for (const [, stream] of activeStreamsMap.entries()) {
      stream.destroy();
    }
    activeStreamsMap.clear();
  }

  // Store data item attributes as they're extracted
  const dataItemAttributes: DataItemAttributes[] = [];
  const bundleHeaderLength = bundleHeaderBuffer.byteLength;

  // Pre-calculate bundle offsets for each data item
  const dataItemOffsetsInRawParent = new Map<TransactionId, number>();
  let currentBundleOffset = bundleHeaderLength;
  for (const dataItem of bundleHeaderInfo.dataItems) {
    dataItemOffsetsInRawParent.set(dataItem.id, currentBundleOffset);
    currentBundleOffset += dataItem.size;
  }

  // Events emitted by this algorithm and handled by the coordinator:
  // - canFetch: when we have capacity to prefetch more data items
  // - canPipe: when we potentially have capacity to pipe more data items to the output stream
  const coordinator = new EventEmitter();

  const offsetsTaskCounter = new TaskCounter(logger);

  const outputStream = new PassThrough();
  // Start a sentinel task spanning the lifetime of the streaming/piping pipeline
  offsetsTaskCounter.startTask();
  logger.info("[offsets] Offsets sentinel task started", {
    expectedAttributesCount: bundleHeaderInfo.dataItems.length,
  });
  // Centralized cleanup for both happy and error paths
  outputStream.on("end", () => {
    coordinator.removeAllListeners();
    cleanupActiveDataItemStreams();
    logger.info(
      "[offsets] Output stream ended; finishing offsets sentinel task..."
    );
    try {
      offsetsTaskCounter.finishTask();
    } catch (error) {
      logger.warn("[offsets] Sentinel finishTask ignored (already zero?)", {
        error,
      });
    }
  });
  outputStream.on("error", (error) => {
    logger.error("Error emitted on outputStream", error);
    coordinator.removeAllListeners();
    cleanupActiveDataItemStreams();
    logger.error(
      "[offsets] Output stream errored; finishing offsets sentinel task..."
    );
    try {
      offsetsTaskCounter.finishTask();
    } catch (error) {
      logger.warn("[offsets] Sentinel finishTask ignored (already zero?)", {
        error,
      });
    }
  });

  // Start piping unawaited so we can return the output stream immediately
  void (async () => {
    // Start by piping the header...
    logger.debug(`Piping header buffer for bundle...`);
    const headerStream = Readable.from(bundleHeaderBuffer);

    try {
      headerStream.pipe(outputStream, { end: false });
      await once(headerStream, "end");
    } catch (error) {
      logger.error(`Error streaming bundle header`, { error });
      headerStream.destroy();
      outputStream.emit("error", error);
      outputStream.destroy();
      return;
    }

    // Then pipe each of the data items, enqueuing a handful of streams at a time for output piping
    const inflightDataItemsSizeLimit = 100 * Math.pow(2, 20); // Limit to 100MiB of inflight streams
    const maxInflightRequests = 100; // Limit to 100 total inflight streams
    let inflightDataSize = 0; // Increments when fetching starts; decrements when piping ends
    let inflightRequestCount = 0; // Increments when fetching starts; decrements when piping ends
    let nextDataItemIndexToFetch = 0;
    let nextDataItemIndexToPipe = 0;

    let piping = false;
    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    coordinator.on("canPipe", async () => {
      if (piping) {
        // Only have one active piping loop at a time
        return;
      }

      let dataItemToPipe = bundleHeaderInfo.dataItems[nextDataItemIndexToPipe];
      let nextDataItemStream = activeStreamsMap.get(
        `${nextDataItemIndexToPipe}`
      );
      while (nextDataItemStream) {
        piping = true;
        activeStreamsMap.delete(`${nextDataItemIndexToPipe}`);

        try {
          nextDataItemStream.pipe(outputStream, { end: false });
          if (nextDataItemStream.isPaused()) {
            nextDataItemStream.resume();
          }
          await once(nextDataItemStream, "end");
          logger.debug(
            `Finished piping data item ${nextDataItemIndexToPipe + 1}/${
              bundleHeaderInfo.dataItems.length
            } for bundle!`,
            { inflightDataSize, inflightRequestCount }
          );

          // Release capacity for more prefetching
          inflightDataSize -= dataItemToPipe.size;
          inflightRequestCount--;
        } catch (error) {
          logger.error(
            `Error streaming data item ${nextDataItemIndexToPipe + 1}/${
              bundleHeaderInfo.dataItems.length
            }!`,
            {
              error,
              dataItemInfo: dataItemToPipe,
            }
          );
          outputStream.emit("error", error);
          outputStream.destroy();
          return;
        }

        // Piping completed successfully. Move on to the next data item.
        piping = false;
        nextDataItemIndexToPipe++;
        nextDataItemStream = activeStreamsMap.get(`${nextDataItemIndexToPipe}`);
        dataItemToPipe = bundleHeaderInfo.dataItems[nextDataItemIndexToPipe];

        // Finished piping something so we may now have capacity to fetch more
        if (
          inflightRequestCount === 0 ||
          (nextDataItemIndexToFetch < bundleHeaderInfo.dataItems.length &&
            bundleHeaderInfo.dataItems[nextDataItemIndexToFetch].size +
              inflightDataSize <=
              inflightDataItemsSizeLimit &&
            inflightRequestCount < maxInflightRequests)
        ) {
          coordinator.emit("canFetch");
        }
        if (nextDataItemIndexToPipe >= bundleHeaderInfo.dataItems.length) {
          logger.debug(`Finished piping all data items for bundle!`);
          outputStream.end();
          return;
        }
      }
      logger.debug(`Done piping available streams.`, {
        inflightDataSize,
        inflightRequestCount,
      });
    });

    // eslint-disable-next-line @typescript-eslint/no-misused-promises
    coordinator.on("canFetch", async () => {
      if (nextDataItemIndexToFetch >= bundleHeaderInfo.dataItems.length) {
        logger.debug(`No more data items to fetch.`);
        coordinator.removeAllListeners("canFetch");
        return;
      }

      const dataItemIndex = nextDataItemIndexToFetch;
      const dataItem = bundleHeaderInfo.dataItems[dataItemIndex];

      // Reserve capacity for prefetching
      inflightRequestCount++;
      inflightDataSize += dataItem.size;

      // Prepare for the subsequent fetch
      nextDataItemIndexToFetch++;

      // If we're not at the limit of inflight requests, fetch another
      if (
        inflightDataSize < inflightDataItemsSizeLimit &&
        inflightRequestCount < maxInflightRequests
      ) {
        coordinator.emit("canFetch");
      }

      async function safelyFetchDataItemStream(
        dataItemIndex: number,
        dataItemKey: string
      ): Promise<Readable | undefined> {
        const dataItem = bundleHeaderInfo.dataItems[dataItemIndex];

        const services = [
          {
            name: "elasticache",
            fetch: async () => {
              if (
                (await shouldUseCacheService()) &&
                (await cacheHasDataItem({
                  cacheService,
                  dataItemId: dataItem.id,
                  logger,
                }))
              ) {
                const cached = await cachedDataItemReadableRange({
                  cacheService,
                  dataItemId: dataItem.id,
                });
                return cached.readable;
              }
              return undefined;
            },
          },
          {
            name: "backupFs",
            fetch: async () => {
              if (await fsBackupHasDataItem(dataItem.id, logger)) {
                const { readable } = await fsBackupRawDataItemReadable({
                  dataItemId: dataItem.id,
                });
                return readable;
              }
              return undefined;
            },
          },
          {
            name: "dynamodb",
            fetch: async () => {
              if (await dynamoHasDataItem(dataItem.id, logger)) {
                const readable = await dynamoReadableRange({
                  dataItemId: dataItem.id,
                  start: 0,
                  logger,
                });
                return readable ?? undefined;
              }
              return undefined;
            },
          },
          {
            name: "objectStore",
            fetch: async () => {
              const objStoreStream = await objectStore.getObject(dataItemKey);
              return objStoreStream.readable;
            },
          },
        ];

        for (const service of services) {
          try {
            const stream = await service.fetch();
            if (stream) {
              // Tap the stream to extract item attributes passively while the data item is being fed into the bundle
              stream.pause(); // Make sure not to affect the stream while tapping
              const tappedStream = tapStream({
                readable: stream,
                logger: logger.child({
                  context: "dataItemAttributeExtraction",
                }),
              });

              offsetsTaskCounter.startTask();

              // Extract attributes asynchronously
              void (async () => {
                try {
                  const streamingDataItem = new StreamingDataItem(tappedStream);
                  // asynchronously drain the full stream so we can consume the data item attributes
                  void drainStream(tappedStream);
                  // FUTURE TODO: If tags indicate it's a BDI, recurse to extract offsets for all nested data items (i.e. full unbundling for offsets data)
                  const rawDataItemSize =
                    await streamingDataItem.getRawDataItemSize(true);
                  const headers = await streamingDataItem.getHeaders();
                  const tags = await streamingDataItem.getTags();

                  // Find content type from tags
                  const contentTypeTag = tags.find(
                    (tag) => tag.name.toLowerCase() === "content-type"
                  );
                  const payloadContentType =
                    contentTypeTag?.value || "application/octet-stream";

                  // Store attributes
                  dataItemAttributes.push({
                    dataItemId: dataItem.id,
                    rawDataItemSize,
                    payloadDataStartOffset: headers.dataOffset,
                    payloadContentType,
                    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
                    rawDataItemOffsetInBundle: dataItemOffsetsInRawParent.get(
                      dataItem.id
                    )!,
                  });

                  // Consume the tapped stream to prevent backpressure
                  tappedStream.on("data", () => {
                    // Intentionally empty - just consuming data to prevent backpressure
                  });
                  tappedStream.on("end", () => {
                    // Intentionally empty - stream completed normally
                  });
                  tappedStream.on("error", (err) => {
                    logger.debug(`Tapped stream error for ${dataItem.id}`, err);
                  });
                } catch (error) {
                  logger.error(
                    `Failed to extract attributes for data item ${dataItem.id}`,
                    {
                      error: error instanceof Error ? error.message : error,
                    }
                  );
                } finally {
                  offsetsTaskCounter.finishTask();
                }
              })();

              return stream;
            }
          } catch (err) {
            logger.error(
              `safelyFetchDataItemStream failed for dataItemIndex=${dataItemIndex}`,
              {
                error: err instanceof Error ? err.message : err,
                dataItemKey,
                service: service.name,
              }
            );
          }
        }

        return undefined;
      }

      // Start fetching
      const dataItemKey = `${dataItemPrefix}/${dataItem.id}`;
      const dataItemStream = await safelyFetchDataItemStream(
        dataItemIndex,
        dataItemKey
      );

      if (!dataItemStream) {
        // TODO: Could consider providing for retrying here
        // Fetch failed, cleanup and exit
        outputStream.emit(
          "error",
          new Error(`Failed to fetch data item ${dataItem.id}`)
        );
        outputStream.destroy();
        return;
      }

      dataItemStream.on("error", (error) => {
        logger.error(`Stream error for data item ${dataItem.id}`, error);
        outputStream.emit("error", error);
        outputStream.destroy();
      });

      activeStreamsMap.set(`${dataItemIndex}`, dataItemStream);

      // We have data we can attempt to pipe now
      coordinator.emit("canPipe");
    });

    // Kick off the first fetch
    logger.debug(`Piping data items for bundle...`);
    coordinator.emit("canFetch");
  })();

  // Create a promise that resolves when all attributes are collected
  const dataItemAttributesPromise = new Promise<DataItemAttributes[]>(
    (resolve) => {
      logger.info(
        `[offsets] Waiting for ${offsetsTaskCounter.activeTaskCount()} tasks to finish...`
      );
      offsetsTaskCounter
        .waitForZero(60_000) // Wait for up to 60 seconds for all offsets tasks to finish
        .then(() => {
          logger.info("[offsets] All offsets tasks finished successfully.", {
            dataItemAttrsCount: dataItemAttributes.length,
          });
          resolve([...dataItemAttributes]);
        })
        .catch((error) => {
          logger.error(
            `[offsets] Error waiting for offsets task counter to finish: ${error}`,
            {
              dataItemAttrsCount: dataItemAttributes.length,
              activeTasks: offsetsTaskCounter.activeTaskCount(),
            }
          );
          resolve([...dataItemAttributes]); // Return what we have even on error
        });
      outputStream.on("error", () => {
        logger.error(
          "[offsets] Output stream errored; Returning collected attributes...",
          {
            dataItemAttrsCount: dataItemAttributes.length,
            activeTasks: offsetsTaskCounter.activeTaskCount(),
          }
        );
        resolve([...dataItemAttributes]); // Return what we have even on error
      });
    }
  );

  // At this point there's data flowing into the stream...
  return {
    payloadReadable: outputStream,
    dataItemAttributesPromise,
  };
}

export async function quarantineDataItem({
  dataItemId,
  objectStore,
  cacheService,
  database,
  logger,
  contentLength,
  contentType,
  payloadInfo,
}: {
  dataItemId: TransactionId;
  objectStore: ObjectStore;
  cacheService: CacheService;
  database: Database;
  logger: winston.Logger;
  contentLength?: number;
  contentType?: string;
  payloadInfo?: PayloadInfo;
}): Promise<void> {
  // Sleep for 100ms to allow the database to catch up from any replication lag
  // TODO: Consider valkey or filesystem-based alternatives to avoid the sleep
  await sleep(100);
  const dataItemExistsInDb = await database.getDataItemInfo(dataItemId);
  if (dataItemExistsInDb !== undefined) {
    logger.warn(
      `Data item ${dataItemId} is still referenced in the database. Skipping quarantine from data stores.`
    );
    MetricRegistry.dataItemRemoveCanceledWhenFoundInDb.inc();
    return;
  }

  // First quarantine the data item from the cache service
  try {
    await quarantineCachedDataItem(cacheService, dataItemId, logger);
  } catch (error) {
    logger.error(
      `Error quarantining data item ${dataItemId} in cache service
    `,
      { error }
    );
  }

  // Then quarantine the data item from the file system backup
  try {
    await quarantineDataItemFromBackupFs({
      dataItemId,
      logger,
    });
  } catch (error) {
    logger.error(
      `Error quarantining data item ${dataItemId} in backup file system`,
      { error }
    );
  }

  // Finally quarantine the data item from the object store
  if (await rawDataItemObjectExists(objectStore, dataItemId)) {
    try {
      logger.info(`Quarantining data item ${dataItemId} in object store...`);
      const sourceKey = `${dataItemPrefix}/${dataItemId}`;
      await objectStore.moveObject({
        sourceKey,
        destinationKey: `quarantine/${sourceKey}`,
        Options: {
          contentLength,
          contentType,
          payloadInfo,
        },
      });
      MetricRegistry.objectStoreQuarantineSuccess.inc();
    } catch (error) {
      logger.error(
        `Error quarantining data item ${dataItemId} in object store`,
        {
          error,
        }
      );
      MetricRegistry.objectStoreQuarantineFailure.inc();
    }
  } else {
    logger.info(
      `Data item ${dataItemId} not found in object store. Skipping quarantine.`
    );
  }
}

export async function streamsForDataItemStorage({
  inputStream,
  contentLength,
  cacheService,
  logger,
}: {
  inputStream: Readable;
  contentLength?: number;
  cacheService: CacheService;
  logger: winston.Logger;
}): Promise<{
  cacheServiceStream?: Readable;
  fsBackupStream?: Readable;
  objStoreStream?: Readable;
  dynamoStream?: Readable;
}> {
  // For sufficiently small data items, stream them to a cache service
  const isSmallDataItem =
    contentLength &&
    +contentLength <=
      (await getConfigValue(ConfigKeys.cacheDataItemBytesThreshold));
  const shouldSampleInSmallDataItem = shouldSampleIn(
    await getConfigValue(ConfigKeys.cacheWriteDataItemSamplingRate)
  );
  const cacheServiceStream =
    isSmallDataItem &&
    cacheServiceIsAvailable(cacheService) && // Don't bother creating this stream if the circuit breaker is open
    shouldSampleInSmallDataItem
      ? tapStream({
          readable: inputStream,
          logger: logger.child({ context: "smallDataItemStream" }),
        })
      : undefined;

  // If streaming to a cache service, also back up to a durable file system
  const shouldSampleInFSBackup =
    cacheServiceStream &&
    shouldSampleIn(
      await getConfigValue(ConfigKeys.fsBackupWriteDataItemSamplingRate)
    );
  const fsBackupStream =
    shouldSampleInFSBackup && backupFsAvailable() // Don't bother creating this stream if the circuit breaker is open
      ? tapStream({
          readable: inputStream,
          logger: logger.child({ context: "fsBackupStream" }),
        })
      : undefined;

  // DynamoDB can serve as the durable data store for sufficiently small data items
  const shouldSampleToDynamo = await shouldCacheDataItemToDynamoDB();
  const useDynamo =
    shouldSampleToDynamo &&
    contentLength &&
    +contentLength <=
      (await getConfigValue(ConfigKeys.dynamoDataItemBytesThreshold)) &&
    dynamoAvailable();
  const dynamoStream = useDynamo
    ? tapStream({
        readable: inputStream,
        logger: logger.child({ context: "dynamoStream" }),
      })
    : undefined;

  // If not streaming to a cache service, or if another layer of backup is desirable, send a stream to an object store
  const shouldSampleInS3Backup = shouldSampleIn(
    await getConfigValue(ConfigKeys.objStoreDataItemSamplingRate)
  );

  const haveNonS3DurableBackup = (dynamoStream || fsBackupStream) != undefined;
  const shouldBackUpToS3 =
    !isSmallDataItem || // everything over 256KiB should be backed up to S3
    !haveNonS3DurableBackup || // S3 should be durable backup of last resort...
    shouldSampleInS3Backup; // ... but sometimes we want to sample in S3 backups no matter what

  const objStoreStream = shouldBackUpToS3
    ? tapStream({
        readable: inputStream,
        logger: logger.child({ context: "s3BackupStream" }),
      })
    : undefined;

  return {
    cacheServiceStream,
    fsBackupStream,
    objStoreStream,
    dynamoStream,
  };
}

export type ValidDataItemStore = "cache" | "fs_backup" | "object_store" | "ddb";

// NB: There is delicate code depending on this list to be in this order and this size
export const allValidDataItemStores: ValidDataItemStore[] = [
  "cache",
  "fs_backup",
  "object_store",
  "ddb",
];

export async function cacheDataItem({
  streamingDataItem,
  rawContentLength,
  payloadContentType,
  payloadDataStart,
  cacheServiceStream,
  fsBackupStream,
  dynamoStream,
  objStoreStream,
  cacheService,
  objectStore,
  logger,
  durations,
}: {
  streamingDataItem: DataItemInterface;
  rawContentLength?: number;
  payloadContentType: string;
  payloadDataStart: number;
  cacheService: CacheService;
  objectStore: ObjectStore;
  cacheServiceStream?: Readable;
  fsBackupStream?: Readable;
  objStoreStream?: Readable;
  dynamoStream?: Readable;
  logger: winston.Logger;
  durations?: {
    cacheDuration: number;
  };
}): Promise<ValidDataItemStore[]> {
  const dataItemId = await streamingDataItem.getDataItemId();

  // Track exactly which stores we are using
  const plannedStores = allValidDataItemStores.filter(
    (_, i) =>
      [cacheServiceStream, fsBackupStream, objStoreStream, dynamoStream][i]
  );
  const actualStores: ValidDataItemStore[] = [];

  // Use a deferred promise to control whether stores perform final commits of data based on data item validity
  const deferredIsValid = new Deferred<boolean>();

  // Cache the raw and extracted data item streams
  const objectStoreCacheStart = Date.now();
  await Promise.allSettled([
    cacheServiceStream
      ? cacheSmallDataItem({
          cacheService,
          smallDataItemStream: cacheServiceStream,
          dataItemId,
          // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          rawContentLength: rawContentLength!, // We only have cacheServiceStream with specific contentLength range
          payloadContentType,
          payloadDataStart,
          logger,
          deferredIsValid,
        })
          .then(() => {
            actualStores.push("cache");
          })
          // Treat Elasticache streaming failures as soft errors
          .catch((error) => {
            logger.error(
              `Error while attempting to cache small data item!`,
              error
            );
          })
          .finally(() => {
            cacheServiceStream.destroy();
          })
      : Promise.resolve(),
    fsBackupStream
      ? writeStreamAndMetadataToFiles({
          inputStream: fsBackupStream,
          payloadContentType,
          payloadDataStart,
          dataItemId,
          logger,
          deferredIsValid,
        })
          .then(() => {
            if (durations) {
              durations.cacheDuration = Date.now() - objectStoreCacheStart;

              logger.debug(
                `Cache backup full item duration: ${durations.cacheDuration}ms`
              );
            }
            actualStores.push("fs_backup");
          })
          .catch((error) => {
            logger.error(
              `Error while attempting to backup data item to FS!`,
              error
            );
          })
          .finally(() => {
            fsBackupStream.destroy();
          })
      : Promise.resolve(),
    dynamoStream
      ? (async () => {
          const buffer = await streamToBuffer(dynamoStream);
          await putDynamoDataItem({
            dataItemId,
            data: buffer,
            size: buffer.length,
            payloadStart: payloadDataStart,
            contentType: payloadContentType,
            logger,
          });
          actualStores.push("ddb");
        })()
          .then(() => {
            if (durations) {
              durations.cacheDuration = Date.now() - objectStoreCacheStart;

              logger.debug(
                `Cache backup full item duration: ${durations.cacheDuration}ms`
              );
            }
            actualStores.push("ddb");
          })
          .catch((error) => {
            logger.error(
              `Error while attempting to backup data item to DynamoDB!`,
              error
            );
          })
          .finally(() => {
            dynamoStream.destroy();
          })
      : Promise.resolve(),
    objStoreStream
      ? putDataItemRaw(
          objectStore,
          dataItemId,
          objStoreStream,
          payloadContentType,
          payloadDataStart
        )
          .then(() => {
            if (durations) {
              durations.cacheDuration = Date.now() - objectStoreCacheStart;
              logger.debug(
                `Cache full item duration: ${durations.cacheDuration}ms`
              );
            }
            actualStores.push("object_store");
          })
          .finally(() => {
            objStoreStream.destroy();
          })
      : Promise.resolve(),
    // Pump data through the system by awaiting a result that requires consuming the full stream
    (async () => {
      logger.debug(`Consuming payload stream...`);
      try {
        const isValid = await streamingDataItem.isValid();
        deferredIsValid.resolve(isValid);
      } catch (error) {
        deferredIsValid.reject(error);
        throw error;
      }
      logger.debug(`Payload stream consumed.`);
    })(),
  ]).then(() => {
    // Ensure at least one durable store successfully cached the data item
    if (
      !["fs_backup", "object_store", "ddb"].some((store) =>
        actualStores.includes(store as ValidDataItemStore)
      )
    ) {
      const errMsg = "No durable store successfully cached the data item!";
      logger.error(errMsg, { dataItemId });
      throw new Error(errMsg);
    }
    logger.debug(`Finished uploading raw data item to planned stores!`, {
      plannedStores,
      actualStores,
    });
  });

  return actualStores;
}

export async function shouldCacheNestedDataItemToObjStore(): Promise<boolean> {
  const objStoreNestedDataItemSamplingRate = await getConfigValue(
    ConfigKeys.objStoreNestedDataItemSamplingRate
  );
  return shouldSampleIn(objStoreNestedDataItemSamplingRate);
}

async function shouldCachedNestedDataItemToCacheService(): Promise<boolean> {
  const cacheServiceNestedDataItemSamplingRate = await getConfigValue(
    ConfigKeys.cacheWriteNestedDataItemSamplingRate
  );
  return shouldSampleIn(cacheServiceNestedDataItemSamplingRate);
}

async function shouldCacheNestedDataItemToFsBackup(): Promise<boolean> {
  const fsBackupNestedDataItemSamplingRate = await getConfigValue(
    ConfigKeys.fsBackupWriteNestedDataItemSamplingRate
  );
  return shouldSampleIn(fsBackupNestedDataItemSamplingRate);
}

async function shouldCacheDataItemToDynamoDB(): Promise<boolean> {
  const dynamoDataItemSamplingRate = await getConfigValue(
    ConfigKeys.dynamoWriteDataItemSamplingRate
  );
  return shouldSampleIn(dynamoDataItemSamplingRate);
}

async function shouldCacheNestedDataItemToDynamoDB(): Promise<boolean> {
  const dynamoNestedDataItemSamplingRate = await getConfigValue(
    ConfigKeys.dynamoWriteNestedDataItemSamplingRate
  );
  return shouldSampleIn(dynamoNestedDataItemSamplingRate);
}

export async function cacheNestedDataItem({
  parentDataItemId,
  streamingDataItem,
  parentPayloadDataStart,
  startOffsetInRawParent,
  rawContentLength,
  payloadContentType,
  payloadDataStart,
  objStoreStream,
  cacheService,
  objectStore,
  logger,
  durations,
}: {
  parentDataItemId: TransactionId;
  streamingDataItem: StreamingDataItem;
  parentPayloadDataStart: number;
  startOffsetInRawParent: number;
  rawContentLength: number;
  payloadContentType: string;
  payloadDataStart: number;
  cacheService: CacheService;
  objectStore: ObjectStore;
  objStoreStream?: Readable;
  logger: winston.Logger;
  durations?: {
    cacheDuration: number;
  };
}): Promise<ValidDataItemStore[]> {
  const dataItemId = await streamingDataItem.getDataItemId();
  const shouldCacheToCacheService =
    await shouldCachedNestedDataItemToCacheService();

  let shouldCacheToFsBackup = await shouldCacheNestedDataItemToFsBackup();
  const fsAvailable = backupFsAvailable();
  logger.debug("Cache nested item to fs backup?", {
    shouldCacheToFsBackup,
    fsAvailable,
  });
  shouldCacheToFsBackup = shouldCacheToFsBackup && fsAvailable;

  let shouldCacheToDynamoDB = await shouldCacheNestedDataItemToDynamoDB();
  const dynamoIsAvailable = dynamoAvailable();
  const itemFitsInDynamo =
    rawContentLength <=
    (await getConfigValue(ConfigKeys.dynamoDataItemBytesThreshold));
  shouldCacheToDynamoDB =
    shouldCacheToDynamoDB && dynamoIsAvailable && itemFitsInDynamo;

  // Throw if no backup store is available
  if (!shouldCacheToFsBackup && !shouldCacheToDynamoDB && !objStoreStream) {
    throw new Error(
      `No backup store available for nested data item ${dataItemId} with parent ${parentDataItemId}`
    );
  }

  // Track exactly which stores we are using
  const plannedStores = ["cache", "fs_backup", "object_store", "ddb"].filter(
    (_, i) =>
      [
        shouldCacheToCacheService,
        shouldCacheToFsBackup,
        objStoreStream,
        shouldCacheToDynamoDB,
      ][i]
  ) as ValidDataItemStore[];
  const actualStores: ValidDataItemStore[] = [];

  // Cache the offsets and the raw data item stream
  const objectStoreCacheStart = Date.now();
  logger.debug(
    `Starting streaming of nested data item with ID ${dataItemId}...`,
    {
      plannedStores,
      shouldCacheToCacheService,
      shouldCacheToFsBackup,
      shouldCacheToDynamoDB,
    }
  );
  await Promise.allSettled([
    shouldCacheToCacheService
      ? (async () => {
          try {
            logger.debug(
              `Checking validity of ${dataItemId} before caching to cache service...`
            );
            const isValid = await streamingDataItem.isValid();
            logger.debug(
              `${dataItemId} is ${isValid ? "VALID" : "INVALID"}.${
                isValid ? "" : " NOT"
              } caching to cache service...`
            );
            if (!isValid) {
              throw new Error(
                `Data item ${dataItemId} is not valid. Not caching to cache service.`
              );
            }
            // Write offsets information to cache service
            await cacheNestedDataItemInfo({
              dataItemId,
              parentDataItemId,
              parentPayloadDataStart,
              startOffsetInRawParent,
              rawContentLength,
              payloadContentType,
              payloadDataStart,
              cacheService,
              logger,
            });
            actualStores.push("cache");
          } catch (error) {
            logger.error(
              `Error while attempting to cache nested data item offsets!`,
              error
            );
            // Don't treat as fatal yet if also backing up to fs or obj store
            if (!(shouldCacheToFsBackup || objStoreStream)) {
              throw error;
            }
          }
        })()
      : Promise.resolve(),
    shouldCacheToFsBackup
      ? (async () => {
          try {
            logger.debug(
              `Checking validity of ${dataItemId} before caching to FS backup...`
            );
            const isValid = await streamingDataItem.isValid();
            logger.debug(
              `${dataItemId} is ${isValid ? "VALID" : "INVALID"}.${
                isValid ? "" : " NOT"
              } caching to FS backup...`
            );
            if (!isValid) {
              throw new Error(
                `Data item ${dataItemId} is not valid. Not caching to fsBackup.`
              );
            }
            await fsBackupNestedDataItemInfo({
              dataItemId,
              parentDataItemId,
              parentPayloadDataStart,
              startOffsetInRawParent,
              rawContentLength,
              payloadContentType,
              payloadDataStart,
              logger,
            });
            actualStores.push("fs_backup");
            if (durations) {
              durations.cacheDuration = Date.now() - objectStoreCacheStart;
              logger.debug(
                `Cache nested item duration: ${durations.cacheDuration}ms`
              );
            }
          } catch (error) {
            logger.error(
              `Error while attempting to cache nested data item offsets in fsBackup!`,
              error
            );
            throw error;
          }
        })()
      : Promise.resolve(),
    shouldCacheToDynamoDB
      ? (async () => {
          try {
            logger.debug(
              `Checking validity of ${dataItemId} before caching to DynamoDB...`
            );
            const isValid = await streamingDataItem.isValid();
            logger.debug(
              `${dataItemId} is ${isValid ? "VALID" : "INVALID"}.${
                isValid ? "" : " NOT"
              } caching to DynamoDB...`
            );
            if (!isValid) {
              throw new Error(
                `Data item ${dataItemId} is not valid. Not caching to DynamoDB.`
              );
            }
            await putDynamoOffsetsInfo({
              dataItemId,
              parentDataItemId,
              startOffsetInParentDataItemPayload:
                startOffsetInRawParent - parentPayloadDataStart,
              rawContentLength,
              payloadContentType,
              payloadDataStart,
              logger,
            });
            actualStores.push("ddb");
          } catch (error) {
            logger.error(
              `Error while attempting to cache nested data item offsets to DynamoDB!`,
              error
            );
            // Don't treat as fatal yet if also backing up to fs or obj store
            if (!(shouldCacheToFsBackup || objStoreStream)) {
              throw error;
            }
          }
        })()
      : Promise.resolve(),
    objStoreStream
      ? putDataItemRaw(
          objectStore,
          dataItemId,
          objStoreStream,
          payloadContentType,
          payloadDataStart
        )
          .then(() => {
            actualStores.push("object_store");
            if (durations) {
              durations.cacheDuration = Date.now() - objectStoreCacheStart;
              logger.debug(
                `Cache nested item duration: ${durations.cacheDuration}ms`
              );
            }
          })
          .finally(() => {
            objStoreStream.destroy();
          })
      : Promise.resolve(),
  ]).then(() => {
    // Ensure that at least one of the durable backing stores cached the data
    if (
      !["fs_backup", "object_store", "ddb"].some((store) =>
        actualStores.includes(store as ValidDataItemStore)
      )
    ) {
      const errMsg = `No durable store successfully cached nested data item with ID ${dataItemId}!`;
      logger.error(errMsg);
      throw new Error(errMsg);
    }

    logger.debug(`Finished caching nested raw data item!`, {
      plannedStores,
      actualStores,
    });
  });

  return actualStores;
}

async function shouldReadFromBackupFS(): Promise<boolean> {
  return (
    (await getConfigValue(ConfigKeys.fsBackupWriteDataItemSamplingRate)) > 0 ||
    (await getConfigValue(ConfigKeys.fsBackupWriteNestedDataItemSamplingRate)) >
      0
  );
}
