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
import { expect } from "chai";
import { writeFileSync } from "fs";
import { Readable } from "multistream";
import pLimit from "p-limit";

import { FileSystemObjectStore } from "../arch/fileSystemObjectStore";
import { ObjectStore } from "../arch/objectStore";
import {
  assembleBundleHeader,
  bundleHeaderInfoFromBuffer,
  totalBundleSizeFromHeaderInfo,
} from "../bundles/assembleBundleHeader";
import { rawIdFromRawSignature } from "../bundles/rawIdFromRawSignature";
import logger from "../logger";
import { PlanId } from "../types/dbTypes";
import { TransactionId } from "../types/types";
import {
  assembleBundlePayload,
  byteCountRangeOfRawSignature,
  getRawSignatureOfDataItem,
  getSignatureTypeOfDataItem,
} from "./objectStoreUtils";
import { streamIntoBufferAtOffset, streamToBuffer } from "./streamToBuffer";

const objectStore = new FileSystemObjectStore();

describe("Bundle buffer functions", () => {
  it("streamed buffer implementation gives the same buffer as existing implementations", async () => {
    // cspell:disable
    const dataItemId = "PPqimlPSz890fAufmEs7XnpReEq_o70FvJvz-Leiw1A"; // cspell:enable
    const headerBuffer = await streamToBuffer(
      await assembleBundleHeader([
        {
          dataItemRawId: await rawIdFromRawSignature(
            await getRawSignatureOfDataItem(
              objectStore,
              dataItemId,
              await getSignatureTypeOfDataItem(objectStore, dataItemId)
            )
          ),
          byteCount: 1464,
        },
      ])
    );

    const stream = await assembleBundlePayload(objectStore, headerBuffer);
    const bufferFromStream = await streamToBuffer(stream);
    const headerSize = totalBundleSizeFromHeaderInfo(
      bundleHeaderInfoFromBuffer(headerBuffer)
    );
    expect(headerSize).to.equal(bufferFromStream.byteLength);

    // write header to disk for pLimit implementation to consume
    writeFileSync("temp/header/test-header", headerBuffer);

    const bufferFromPLimit = await getBundlePLimitBuffer(
      objectStore, // cspell:disable
      ["PPqimlPSz890fAufmEs7XnpReEq_o70FvJvz-Leiw1A"], // cspell:enable
      "test-header"
    );

    expect(bufferFromStream.byteLength).to.equal(bufferFromPLimit.byteLength);
    expect(bufferFromStream).to.deep.equal(bufferFromPLimit);

    const bufferFromBufferAlloc = await getBundleBufferAlloc(
      objectStore,
      "test-header",
      headerBuffer
    );
    expect(bufferFromStream.byteLength).to.equal(
      bufferFromBufferAlloc.byteLength
    );
    expect(bufferFromStream).to.deep.equal(bufferFromBufferAlloc);
  });
});

describe("byteCountRangeOfRawSignature", () => {
  it("returns the expected values for valid inputs", () => {
    const inputsToExpectedOutputs = {
      1: "bytes=2-513",
      2: "bytes=2-65",
      3: "bytes=2-66",
      4: "bytes=2-65",
      5: "bytes=2-65",
      6: "bytes=2-2053",
    };

    for (const [input, expectedOutput] of Object.entries(
      inputsToExpectedOutputs
    )) {
      expect(byteCountRangeOfRawSignature(+input)).to.equal(expectedOutput);
    }
  });

  it("throws when given an invalid input", () => {
    for (const input of [7, "a", 0, "arweave", "solana"]) {
      expect(() => byteCountRangeOfRawSignature(+input)).to.throw;
    }
  });
});

const PARALLEL_LIMIT = 5;
const dataItemPrefix = "raw-data-item";

/** Old implementation preserved for testing deep equality */
async function getBundlePLimitBuffer(
  objectStore: ObjectStore,
  dataItemIds: TransactionId[],
  planId: PlanId
): Promise<Buffer> {
  const parallelLimit = pLimit(PARALLEL_LIMIT);
  const dataItemBuffers = await Promise.all(
    dataItemIds.map((dataItemId) => {
      return parallelLimit(async () => {
        const storeKey = `${dataItemPrefix}/${dataItemId}`;
        const rawDataItemSize = await objectStore.getObjectByteCount(storeKey);
        return streamToBuffer(
          await objectStore.getObject(storeKey),
          rawDataItemSize
        );
      });
    })
  );

  const headerStoreKey = `header/${planId}`;
  const bundleHeaderBuffer = await streamToBuffer(
    await objectStore.getObject(headerStoreKey),
    await objectStore.getObjectByteCount(headerStoreKey)
  );

  return Buffer.concat([bundleHeaderBuffer, ...dataItemBuffers]);
}

/** Old implementation preserved for testing deep equality */
async function getBundleBufferAlloc(
  objectStore: ObjectStore,
  planId: PlanId,
  bundleHeaderBuffer: Buffer
): Promise<Buffer> {
  logger.info(`Preparing bundle buffer for plan ID ${planId}...`);

  // Figure out how large a buffer we'll need by utilizing the bundle header info
  const bundleHeaderInfo = bundleHeaderInfoFromBuffer(bundleHeaderBuffer);
  const totalBundleSize = totalBundleSizeFromHeaderInfo(bundleHeaderInfo);

  logger.info(
    `Allocating bundle buffer of size ${totalBundleSize} bytes for plan ID ${planId}...`
  );
  const bundleBuffer = Buffer.alloc(totalBundleSize);
  bundleHeaderBuffer.copy(bundleBuffer);

  logger.info(
    `Copied bundle header of size ${bundleHeaderBuffer.byteLength} into bundle buffer of size ${bundleBuffer.byteLength} for plan ID ${planId}...`
  );

  const parallelLimit = pLimit(5);
  await Promise.all(
    bundleHeaderInfo.dataItems.map((dataItem) => {
      const { id: dataItemId } = dataItem;
      return parallelLimit(async () => {
        let objReadable: Readable;
        try {
          objReadable = await objectStore.getObject(
            `${dataItemPrefix}/${dataItemId}`
          );
        } catch (error) {
          logger.error(
            `Failed to get readable for '${dataItemPrefix}/${dataItemId}' from object store!`,
            error
          );
          throw error;
        }
        await streamIntoBufferAtOffset(
          objReadable,
          bundleBuffer,
          dataItem.dataOffset
        );
      });
    })
  );

  return bundleBuffer;
}
