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
import Transaction from "arweave/node/lib/transaction";
import MultiStream from "multistream";
import { PassThrough, Readable, pipeline } from "stream";

import { ObjectStore } from "../arch/objectStore";
import { S3ObjectStore } from "../arch/s3ObjectStore";
import "../bundles/assembleBundleHeader";
import {
  BundleHeaderInfo,
  bundleHeaderInfoFromBuffer,
} from "../bundles/assembleBundleHeader";
import { DataItemOffsets } from "../constants";
import { octetStreamContentType } from "../constants";
import logger from "../logger";
import { PlanId } from "../types/dbTypes";
import {
  ByteCount,
  PayloadInfo,
  TransactionId,
  UploadId,
} from "../types/types";
import { streamToBuffer } from "./streamToBuffer";

export const dataItemPrefix =
  process.env.DATA_ITEM_S3_PREFIX ?? "raw-data-item";
const multiPartPrefix = process.env.MULTIPART_S3_PREFIX ?? "multipart-uploads";
const bundlePayloadPrefix =
  process.env.BUNDLE_PAYLOAD_S3_PREFIX ?? "bundle-payload";
const bundleTxPrefix = process.env.BUNDLE_TX_S3_PREFIX ?? "bundle";

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

/** strip CR/LF and the rest of the C0 control block (plus DEL 0x7F) */
// eslint-disable-next-line no-control-regex
const controlRegexp = new RegExp("[\\x00-\\x1F\\x7F]", "g"); // not a regex *literal*

export function sanitizePayloadContentType(raw: string): string {
  const defaultOctetStream = "application/octet-stream";
  if (raw.length === 0) return defaultOctetStream;

  const out = raw
    .replace(/[\r\n]+/g, " ") // step 1 – flatten line breaks
    .replace(controlRegexp, "") // step 2 – remove other controls
    .replace(/\s{2,}/g, " ") // step 3 – collapse whitespace
    .trim();

  return out.length === 0 ? defaultOctetStream : out;
}

export function putDataItemRaw(
  objectStore: ObjectStore,
  dataItemId: TransactionId,
  dataItem: Readable,
  payloadContentType: string,
  payloadDataStart: number
): Promise<void> {
  return objectStore.putObject(`${dataItemPrefix}/${dataItemId}`, dataItem, {
    payloadInfo: {
      payloadDataStart,
      payloadContentType: sanitizePayloadContentType(payloadContentType), // Ensure we don't throw an error on user provided content types
    },
  });
}

export async function getDataItemPayloadInfo(
  objectStore: ObjectStore,
  dataItemId: TransactionId
): Promise<PayloadInfo> {
  const key = `${dataItemPrefix}/${dataItemId}`;
  return objectStore.getObjectPayloadInfo(key);
}

export async function getDataItemObjectReadableRange({
  objectStore,
  dataItemId,
  startOffset,
  endOffsetInclusive,
}: {
  objectStore: ObjectStore;
  dataItemId: TransactionId;
  startOffset?: number;
  endOffsetInclusive?: number;
}): Promise<Readable> {
  let Range = `bytes=${startOffset || 0}-`;
  if (endOffsetInclusive !== undefined) {
    Range += endOffsetInclusive;
  }

  return objectStore
    .getObject(`${dataItemPrefix}/${dataItemId}`, Range)
    .then(({ readable }) => readable);
}

export async function getDataItemRangeReadable({
  objectStore,
  dataItemId,
  startOffset,
  endOffsetInclusive,
}: {
  objectStore: ObjectStore;
  dataItemId: TransactionId;
  startOffset?: number;
  endOffsetInclusive?: number;
}): Promise<Readable> {
  let Range = `bytes=${startOffset || 0}-`;
  if (endOffsetInclusive !== undefined) {
    Range += endOffsetInclusive;
  }
  return objectStore
    .getObject(`${dataItemPrefix}/${dataItemId}`, Range)
    .then(({ readable }) => readable);
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

export async function rawDataItemObjectExists(
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

export const byteCountRangeOfRawSignature = (sigType: number) => {
  try {
    return `bytes=${
      DataItemOffsets.signatureStart
    }-${DataItemOffsets.signatureEnd(sigType)}`;
  } catch (err) {
    const errMsg = `Unable to determine signature length for signature type ${sigType}`;
    logger.error(errMsg);
    throw new Error(errMsg);
  }
};

export function getSignatureTypeOfDataItemFromObjStore(
  objectStore: ObjectStore,
  dataItemId: TransactionId
): Promise<number> {
  return objectStore
    .getObject(
      `${dataItemPrefix}/${dataItemId}`,
      `bytes=${DataItemOffsets.signatureTypeStart}-${DataItemOffsets.signatureTypeEnd}`
    )
    .then(({ readable: signatureTypeReadable }) => {
      return streamToBuffer(
        signatureTypeReadable,
        DataItemOffsets.signatureTypeEnd -
          DataItemOffsets.signatureTypeStart +
          1
      );
    })
    .then(byteArrayToLong);
}

export function getRawSignatureOfDataItemFromObjStore(
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
    Options: {
      payloadInfo: {
        payloadContentType: sanitizePayloadContentType(payloadContentType), // Ensure we don't throw an error on user provided content types
        payloadDataStart,
      },
    },
  });
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

export async function getBundleHeaderInfo({
  headerSize,
  objectStore,
  planId,
}: {
  objectStore: ObjectStore;
  planId: PlanId;
  headerSize: ByteCount;
}): Promise<BundleHeaderInfo> {
  return objectStore.getBundleHeaderInfo(
    `${bundlePayloadPrefix}/${planId}`,
    `bytes=0-${headerSize - 1}`
  );
}
