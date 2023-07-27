/**
 * Copyright (C) 2022-2023 Permanent Data Solutions, Inc. All Rights Reserved.
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
import { PassThrough, Readable, pipeline } from "stream";

import { ObjectStore } from "../arch/objectStore";
import { S3ObjectStore } from "../arch/s3ObjectStore";
import "../bundles/assembleBundleHeader";
import { bundleHeaderInfoFromBuffer } from "../bundles/assembleBundleHeader";
import { signatureTypeInfo } from "../bundles/verifyDataItem";
import { octetStreamContentType } from "../constants";
import logger from "../logger";
import { PlanId } from "../types/dbTypes";
import { ByteCount, TransactionId } from "../types/types";
import { streamToBuffer } from "./streamToBuffer";

const dataItemPrefix = "raw-data-item";
const fileDataPrefix = "data";
const bundleHeaderPrefix = "header";
const bundlePayloadPrefix = "bundle-payload";
const bundleTxPrefix = "bundle";

export function getS3ObjectStore(): ObjectStore {
  const useMultiRegionAccessPoint =
    process.env.NODE_ENV === "dev" &&
    !!process.env.DATA_ITEM_MULTI_REGION_ENDPOINT;
  return new S3ObjectStore({
    bucketName: useMultiRegionAccessPoint
      ? `${process.env.DATA_ITEM_MULTI_REGION_ENDPOINT}`
      : // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
        process.env.DATA_ITEM_BUCKET!, // Blow up if we can't fall back to this
  });
}

export function putDataItemRaw(
  objectStore: ObjectStore,
  dataItemId: TransactionId,
  dataItem: Readable
): Promise<void> {
  return objectStore.putObject(`${dataItemPrefix}/${dataItemId}`, dataItem);
}

export function putDataItemData(
  objectStore: ObjectStore,
  dataItemId: TransactionId,
  contentType: string,
  dataItem: Readable
): Promise<void> {
  return objectStore.putObject(`${fileDataPrefix}/${dataItemId}`, dataItem, {
    contentType,
  });
}

export function getDataItemData(
  objectStore: ObjectStore,
  dataItemId: TransactionId,
  Range?: string
): Promise<Readable> {
  return objectStore.getObject(`${fileDataPrefix}/${dataItemId}`, Range);
}

export function putBundleHeader(
  objectStore: ObjectStore,
  planId: PlanId,
  bundleHeader: Readable
): Promise<void> {
  return objectStore.putObject(`${bundleHeaderPrefix}/${planId}`, bundleHeader);
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
  logger.info(`Putting bundle tx with ID ${bundleTxId} into Object Store...`);
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
    .then((signatureTypeReadable) => {
      return streamToBuffer(signatureTypeReadable, 2);
    })
    .then(byteArrayToLong);
}

export function getRawSignatureOfDataItem(
  objectStore: ObjectStore,
  dataItemId: TransactionId,
  signatureType: number
): Promise<Readable> {
  return objectStore.getObject(
    `${dataItemPrefix}/${dataItemId}`,
    byteCountRangeOfRawSignature(signatureType)
  );
}

export async function getBundleHeader(
  objectStore: ObjectStore,
  planId: PlanId
): Promise<Readable> {
  const storeKey = `${bundleHeaderPrefix}/${planId}`;
  return objectStore.getObject(storeKey);
}

export async function assembleBundlePayload(
  objectStore: ObjectStore,
  bundleHeaderBuffer: Buffer
): Promise<Readable> {
  const bundleHeaderInfo = bundleHeaderInfoFromBuffer(bundleHeaderBuffer);

  const outputStream = new PassThrough();

  // Start by piping the header...
  logger.info(`Piping header buffer for bundle...`);
  const headerStream = Readable.from(bundleHeaderBuffer);
  headerStream.pipe(outputStream, { end: false });
  headerStream.on("error", (err) => {
    logger.error(`Error streaming bundle header`, { err });
    headerStream.destroy();
    outputStream.destroy();
    throw err;
  });

  // Then pipe each of the data items...
  headerStream.on("end", async () => {
    logger.info(`Piping data items for bundle...`);
    logger.info("bundleHeaderInfo.dataItems", { bundleHeaderInfo });
    const dataItemCount = bundleHeaderInfo.dataItems.length;
    let dataItemIndex = 0;

    // Use a recursive function to iterate through the data items
    async function streamNextDataItem() {
      if (dataItemIndex < dataItemCount) {
        logger.info(
          `Piping data item ${dataItemIndex + 1}/${dataItemCount} for bundle...`
        );
        const dataItemStream = await objectStore.getObject(
          `${dataItemPrefix}/${bundleHeaderInfo.dataItems[dataItemIndex].id}`
        );
        dataItemStream.pipe(outputStream, { end: false });
        dataItemStream.on("end", async () => {
          logger.info(
            `Finished piping data item ${
              dataItemIndex + 1
            }/${dataItemCount} for bundle!`
          );
          dataItemIndex++;
          await streamNextDataItem();
        });
        dataItemStream.on("error", (err) => {
          logger.error(
            `Error streaming data item ${dataItemIndex + 1}/${dataItemCount}!`,
            { err }
          );
          dataItemStream.destroy();
          outputStream.destroy();
          throw err;
        });
      } else {
        logger.info(`Finished piping all data items for bundle!`);
        outputStream.end();
      }
    }

    // Start recursing...
    await streamNextDataItem();
  });

  // At this point there's data flowing into the stream...
  return outputStream;
}

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
      logger.info(`No more data items to pipe.`);
      return cb(null, null);
    }

    // Stream the header for the -1 index
    if (currentStreamIndex === -1) {
      logger.info(`Piping header buffer for bundle...`);
      const headerStream = Readable.from(bundleHeaderBuffer);
      headerStream.on("error", (err) => {
        logger.error(`Error streaming bundle header`, { err });
      });
      return cb(null, headerStream);
    }

    // Otherwise it's a data item
    logger.info(
      `Piping data item ${currentStreamIndex + 1}/${dataItemCount} for bundle!`
    );
    objectStore
      .getObject(
        `${dataItemPrefix}/${bundleHeaderInfo.dataItems[currentStreamIndex].id}`
      )
      .then(
        // onfulfilled
        (dataItemStream) => {
          dataItemStream.on("end", () => {
            logger.info(
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
              { err }
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
      logger.info(`Finished piping all data items for bundle!`);
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
        await streamToBuffer(await objectStore.getObject(storeKey), byteCount)
      ).toString()
    )
  );
  return bundleTx;
}

export async function getBundlePayload(
  objectStore: ObjectStore,
  planId: string,
  headerByteCount?: ByteCount,
  payloadByteCount?: ByteCount
): Promise<Readable> {
  const storeKey = `${bundlePayloadPrefix}/${planId}`;

  // Try S3 first
  try {
    return objectStore.getObject(storeKey);
  } catch (error) {
    logger.warn(
      `Unexpectedly failed to fetch bundle payload from object store!`,
      {
        planId,
        error,
        headerByteCount,
        payloadByteCount,
      }
    );
  }

  // Fall back to building the payload manually
  const headerStream = await getBundleHeader(objectStore, planId);
  const headerBuffer = await streamToBuffer(
    headerStream,
    headerByteCount ?? (await objectStore.getObjectByteCount(storeKey))
  );

  return assembleBundlePayload(objectStore, headerBuffer);
}

export async function removeDataItem(
  objectStore: ObjectStore,
  dataItemTxId: string
): Promise<void> {
  await Promise.all([
    objectStore.removeObject(`${dataItemPrefix}/${dataItemTxId}`),
    objectStore.removeObject(`${fileDataPrefix}/${dataItemTxId}`),
  ]);
  return;
}
export function removeBundleTx(
  objectStore: ObjectStore,
  bundleTxId: string
): Promise<void> {
  return objectStore.removeObject(`${bundleTxPrefix}/${bundleTxId}`);
}
export function removeBundleHeader(
  objectStore: ObjectStore,
  bundlePlanId: string
): Promise<void> {
  return objectStore.removeObject(`${bundleHeaderPrefix}/${bundlePlanId}`);
}
