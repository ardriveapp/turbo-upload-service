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
import { byteArrayToLong } from "arbundles";
import Transaction from "arweave/node/lib/transaction";
import MultiStream from "multistream";
import { EventEmitter, PassThrough, Readable, once, pipeline } from "stream";

import { Database } from "../arch/db/database";
import { ObjectStore } from "../arch/objectStore";
import { S3ObjectStore } from "../arch/s3ObjectStore";
import "../bundles/assembleBundleHeader";
import { bundleHeaderInfoFromBuffer } from "../bundles/assembleBundleHeader";
import { signatureTypeInfo } from "../bundles/verifyDataItem";
import { octetStreamContentType } from "../constants";
import logger from "../logger";
import { MetricRegistry } from "../metricRegistry";
import { PlanId } from "../types/dbTypes";
import { ByteCount, TransactionId, UploadId } from "../types/types";
import { sleep } from "./common";
import { streamToBuffer } from "./streamToBuffer";

const dataItemPrefix = "raw-data-item";
const multiPartPrefix = "multipart-uploads";
const bundlePayloadPrefix = "bundle-payload";
const bundleTxPrefix = "bundle";

let s3ObjectStore: S3ObjectStore | undefined;

export function getS3ObjectStore(): ObjectStore {
  const useMultiRegionAccessPoint =
    process.env.NODE_ENV === "dev" &&
    !!process.env.DATA_ITEM_MULTI_REGION_ENDPOINT;
  if (!s3ObjectStore) {
    s3ObjectStore = new S3ObjectStore({
      bucketName: useMultiRegionAccessPoint
        ? `${process.env.DATA_ITEM_MULTI_REGION_ENDPOINT}`
        : // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
          process.env.DATA_ITEM_BUCKET!, // Blow up if we can't fall back to this
      backupBucketName: process.env.BACKUP_DATA_ITEM_BUCKET,
    });
  }
  return s3ObjectStore;
}

export function putDataItemRaw(
  objectStore: ObjectStore,
  dataItemId: TransactionId,
  dataItem: Readable,
  payloadContentType: string,
  payloadDataStart: number
): Promise<void> {
  return objectStore.putObject(`${dataItemPrefix}/${dataItemId}`, dataItem, {
    payloadInfo: { payloadDataStart, payloadContentType },
  });
}

export async function getDataItemData(
  objectStore: ObjectStore,
  dataItemId: TransactionId,
  Range?: string
): Promise<Readable> {
  const key = `${dataItemPrefix}/${dataItemId}`;
  const { payloadDataStart } = await objectStore.getObjectPayloadInfo(key);

  if (Range) {
    const range = Range.split("=")[1].split("-");
    const rangeStart = range[0];
    const rangeEnd = range[1];

    Range = `bytes=${+rangeStart + payloadDataStart}-${
      rangeEnd ? +rangeEnd + payloadDataStart : ""
    }`;
  } else {
    Range = `bytes=${payloadDataStart}-`;
  }

  return objectStore.getObject(key, Range).then(({ readable }) => readable);
}

export async function getRawDataItem(
  objectStore: ObjectStore,
  dataItemId: TransactionId
): Promise<Readable> {
  const key = `${dataItemPrefix}/${dataItemId}`;
  return objectStore.getObject(key).then(({ readable }) => readable);
}

export async function getRawDataItemByteCount(
  objectStore: ObjectStore,
  dataItemId: TransactionId
): Promise<number> {
  return objectStore.getObjectByteCount(`${dataItemPrefix}/${dataItemId}`);
}

export async function rawDataItemExists(
  objectStore: ObjectStore,
  dataItemId: TransactionId
): Promise<boolean> {
  const key = `${dataItemPrefix}/${dataItemId}`;
  return objectStore
    .headObject(key)
    .then(() => true)
    .catch(() => false);
}

export function putBundlePayload(
  objectStore: ObjectStore,
  planId: PlanId,
  bundlePayload: Readable,
  payloadSize?: number
): Promise<void> {
  return objectStore.putObject(
    `${bundlePayloadPrefix}/${planId}`,
    bundlePayload,
    {
      contentType: octetStreamContentType,
      contentLength: payloadSize,
    }
  );
}

export function putBundleTx(
  objectStore: ObjectStore,
  bundleTxId: TransactionId,
  bundleTx: Readable
): Promise<void> {
  logger.debug(`Putting bundle tx with ID ${bundleTxId} into Object Store...`);
  return objectStore.putObject(`${bundleTxPrefix}/${bundleTxId}`, bundleTx);
}

export const byteCountRangeOfRawSignature = (sigType = 1) => {
  try {
    const sigLength = +signatureTypeInfo[sigType].signatureLength;
    return `bytes=2-${sigLength + 1}`;
  } catch (err) {
    const errMsg = `Unable to determine signature length for signature type ${sigType}`;
    logger.error(errMsg);
    throw new Error(errMsg);
  }
};

export function getSignatureTypeOfDataItem(
  objectStore: ObjectStore,
  dataItemId: TransactionId
): Promise<number> {
  return objectStore
    .getObject(`${dataItemPrefix}/${dataItemId}`, `bytes=0-1`)
    .then(({ readable: signatureTypeReadable }) => {
      return streamToBuffer(signatureTypeReadable, 2);
    })
    .then(byteArrayToLong);
}

export function getRawSignatureOfDataItem(
  objectStore: ObjectStore,
  dataItemId: TransactionId,
  signatureType: number
): Promise<Readable> {
  return objectStore
    .getObject(
      `${dataItemPrefix}/${dataItemId}`,
      byteCountRangeOfRawSignature(signatureType)
    )
    .then(({ readable: rawSignatureReadable }) => rawSignatureReadable);
}

export function assembleBundlePayload(
  objectStore: ObjectStore,
  bundleHeaderBuffer: Buffer
): Readable {
  const bundleHeaderInfo = bundleHeaderInfoFromBuffer(bundleHeaderBuffer);

  const outputStream = new PassThrough();

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
    const activeStreamsMap = new Map<string, Readable>();

    // Events emitted by this algorithm and handled by the coordinator:
    // - canFetch: when we have capacity to prefetch more data items
    // - canPipe: when we potentially have capacity to pipe more data items to the output stream
    const coordinator = new EventEmitter();

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
          nextDataItemStream.destroy();
          outputStream.emit("error", error);
          outputStream.destroy();
          return; // TODO: OVERALL FLOW CONTROL?
        }
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
          coordinator.removeAllListeners("canPipe");
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

      // Start fetching
      const dataItemKey = `${dataItemPrefix}/${dataItem.id}`;
      const fetchedObject = await objectStore
        .getObject(dataItemKey)
        .catch((err) => {
          logger.error(`Error fetching data item ${dataItemIndex + 1}`, {
            err: err instanceof Error ? err.message : err,
            dataItemKey,
          });
          activeStreamsMap.delete(`${dataItemIndex}`);
          outputStream.emit("error", err);
          return undefined;
        });

      const dataItemStream = fetchedObject?.readable;
      if (!dataItemStream) {
        return;
      }

      activeStreamsMap.set(`${dataItemIndex}`, dataItemStream);

      // We have data we can attempt to pipe now
      coordinator.emit("canPipe");
    });

    // Kick off the first fetch
    logger.debug(`Piping data items for bundle...`);
    coordinator.emit("canFetch");
  })();

  // At this point there's data flowing into the stream...
  return outputStream;
}

// TODO: Currently un-used. Test this is a new branch and ticket
// focused on improvements to the bundle payload creation
export async function assembleBundlePayloadWithMultiStream(
  objectStore: ObjectStore,
  bundleHeaderBuffer: Buffer
): Promise<Readable> {
  const bundleHeaderInfo = bundleHeaderInfoFromBuffer(bundleHeaderBuffer);

  const outputStream = new PassThrough();
  const dataItemCount = bundleHeaderInfo.numDataItems;
  let streamIndex = -1; // -1 will signify the bundle header

  const nextStream: MultiStream.FactoryStream = (cb) => {
    // Stash the current index and THEN iterate it
    const currentStreamIndex = streamIndex++;
    if (currentStreamIndex >= bundleHeaderInfo.numDataItems) {
      logger.debug(`No more data items to pipe.`);
      return cb(null, null);
    }

    // Stream the header for the -1 index
    if (currentStreamIndex === -1) {
      logger.debug(`Piping header buffer for bundle...`);
      const headerStream = Readable.from(bundleHeaderBuffer);
      headerStream.on("error", (err) => {
        logger.error(`Error streaming bundle header`, { err });
      });
      return cb(null, headerStream);
    }

    // Otherwise it's a data item
    logger.debug(
      `Piping data item ${currentStreamIndex + 1}/${dataItemCount} for bundle!`
    );
    const dataItemKey = `${dataItemPrefix}/${bundleHeaderInfo.dataItems[currentStreamIndex].id}`;
    objectStore.getObject(dataItemKey).then(
      // onfulfilled
      ({ readable: dataItemStream }) => {
        dataItemStream.on("end", () => {
          logger.debug(
            `Finished piping data item ${
              currentStreamIndex + 1
            }/${dataItemCount} for bundle!`
          );
        });
        dataItemStream.on("error", (err) => {
          logger.error(
            `Error streaming data item ${
              currentStreamIndex + 1
            }/${dataItemCount}!`,
            {
              err,
              dataItemKey,
            }
          );
        });
        cb(null, dataItemStream);
      },
      // onrejected
      (err) => {
        cb(err, null);
      }
    );
  };

  // Get piping!
  return pipeline(new MultiStream(nextStream), outputStream, (err) => {
    if (err) {
      logger.error(`Could not assemble buffer payload!`, err);
    } else {
      logger.debug(`Finished piping all data items for bundle!`);
    }
  });
}

export async function getBundleTx(
  objectStore: ObjectStore,
  bundleTxId: TransactionId,
  byteCount?: ByteCount
): Promise<Transaction> {
  const storeKey = `${bundleTxPrefix}/${bundleTxId}`;

  byteCount ??= await objectStore.getObjectByteCount(storeKey);

  const bundleTx = new Transaction(
    JSON.parse(
      (
        await streamToBuffer(
          (
            await objectStore.getObject(storeKey)
          ).readable,
          byteCount
        )
      ).toString()
    )
  );
  return bundleTx;
}

export async function getBundlePayload(
  objectStore: ObjectStore,
  planId: string
): Promise<Readable> {
  const storeKey = `${bundlePayloadPrefix}/${planId}`;

  return objectStore.getObject(storeKey).then(({ readable }) => readable);
}

export async function removeDataItem(
  objectStore: ObjectStore,
  dataItemTxId: string,
  database: Database
): Promise<void> {
  await sleep(100); // Sleep for 100ms to allow the database to catch up from any replication lag
  const dataItemExistsInDb = await database.getDataItemInfo(dataItemTxId);
  if (dataItemExistsInDb !== undefined) {
    logger.warn(
      `Data item ${dataItemTxId} is still referenced in the database. Skipping removal from object store.`
    );
    MetricRegistry.dataItemRemoveCanceledWhenFoundInDb.inc();
    return;
  }

  return objectStore.removeObject(`${dataItemPrefix}/${dataItemTxId}`);
}

export function removeBundleTx(
  objectStore: ObjectStore,
  bundleTxId: string
): Promise<void> {
  return objectStore.removeObject(`${bundleTxPrefix}/${bundleTxId}`);
}
export function createMultipartUpload(
  objectStore: ObjectStore,
  uploadKey: string
): Promise<string> {
  return objectStore.createMultipartUpload(`${multiPartPrefix}/${uploadKey}`);
}

export function uploadPart({
  objectStore,
  uploadKey,
  stream,
  uploadId,
  partNumber,
  sizeOfChunk,
}: {
  objectStore: ObjectStore;
  uploadKey: string;
  stream: Readable;
  uploadId: UploadId;
  partNumber: number;
  sizeOfChunk: number;
}): Promise<string> {
  return objectStore.uploadPart(
    `${multiPartPrefix}/${uploadKey}`,
    stream,
    uploadId,
    partNumber,
    sizeOfChunk
  );
}

export function getMultipartUploadObject(
  objectStore: ObjectStore,
  uploadKey: string
): Promise<{ readable: Readable; etag: string }> {
  return objectStore
    .getObject(`${multiPartPrefix}/${uploadKey}`)
    .then(({ readable, etag }) => {
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      return { readable, etag: etag! };
    });
}

export function getMultiPartUploadByteCount(
  objectStore: ObjectStore,
  uploadKey: string
): Promise<number> {
  return objectStore.getObjectByteCount(`${multiPartPrefix}/${uploadKey}`);
}

export function moveFinalizedMultipartObject(
  objectStore: ObjectStore,
  uploadKey: string,
  dataItemId: string,
  payloadContentType: string,
  payloadDataStart: number
): Promise<void> {
  return objectStore.moveObject({
    sourceKey: `${multiPartPrefix}/${uploadKey}`,
    destinationKey: `${dataItemPrefix}/${dataItemId}`,
    Options: { payloadInfo: { payloadContentType, payloadDataStart } },
  });
}

export async function removeMultiPartObject(
  objectStore: ObjectStore,
  uploadKey: string
): Promise<void> {
  return objectStore.removeObject(`${multiPartPrefix}/${uploadKey}`);
}

export async function completeMultipartUpload(
  objectStore: ObjectStore,
  uploadKey: string,
  uploadId: UploadId
): Promise<string> {
  return objectStore.completeMultipartUpload(
    `${multiPartPrefix}/${uploadKey}`,
    uploadId
  );
}

export async function getMultipartUploadParts(
  objectStore: ObjectStore,
  uploadKey: string,
  uploadId: UploadId
): Promise<
  {
    size: number;
    partNumber: number;
  }[]
> {
  return objectStore.getMultipartUploadParts(
    `${multiPartPrefix}/${uploadKey}`,
    uploadId
  );
}
