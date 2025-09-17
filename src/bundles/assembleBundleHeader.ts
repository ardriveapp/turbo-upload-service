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
import { byteArrayToLong, longTo32ByteArray } from "@dha-team/arbundles";
import { Readable } from "stream";

import { ByteCount } from "../types/types";
import { toB64Url } from "../utils/base64";

export interface DataItemInfo {
  size: number;
  id: string;
  dataOffset: number;
}

export interface BundleHeaderInfo {
  numDataItems: number;
  dataItems: DataItemInfo[];
}

export async function assembleBundleHeader(
  dataItems: { dataItemRawId: Buffer; byteCount: ByteCount }[]
): Promise<Readable> {
  const headerBuffers: (Buffer | Uint8Array)[] = [];

  headerBuffers.push(longTo32ByteArray(dataItems.length));
  for (const { dataItemRawId, byteCount } of dataItems) {
    headerBuffers.push(
      Buffer.concat([longTo32ByteArray(byteCount), dataItemRawId])
    );
  }

  return Readable.from(Buffer.concat(headerBuffers));
}

export function bundleHeaderInfoFromBuffer(
  headerBuffer: Buffer
): BundleHeaderInfo {
  const numDataItems = byteArrayToLong(headerBuffer.subarray(0, 32));
  let dataOffset = 32 + 64 * numDataItems;
  const dataItems: DataItemInfo[] = [];
  for (let i = 0; i < numDataItems; i++) {
    const entryStart = 32 + i * 64;
    const dataItemSize = byteArrayToLong(
      headerBuffer.subarray(entryStart, entryStart + 32)
    );
    dataItems.push({
      size: dataItemSize,
      id: toB64Url(headerBuffer.subarray(entryStart + 32, entryStart + 64)),
      dataOffset,
    });
    dataOffset += dataItemSize;
  }
  return {
    numDataItems,
    dataItems,
  };
}

export function totalBundleSizeFromHeaderInfo(
  bundleHeaderInfo: BundleHeaderInfo
): number {
  // Based on ANS-104 spec
  const bundleHeaderSize = 32 + bundleHeaderInfo.dataItems.length * 64;

  return (
    bundleHeaderSize +
    bundleHeaderInfo.dataItems.reduce((acc, item) => acc + item.size, 0)
  );
}
