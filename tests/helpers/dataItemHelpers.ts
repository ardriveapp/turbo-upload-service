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
import { Tag, serializeTags } from "arbundles";
import {
  byteArrayToLong,
  deepHash,
  longTo8ByteArray,
  shortTo2ByteArray,
} from "arbundles";
import Arweave from "arweave";
import { JWKInterface } from "arweave/node/lib/wallet";

import { arweaveSignatureLength } from "../../src/bundles/dataItem";
import { fromB64Url } from "../../src/utils/base64";

export function toDataItem(
  data: string | Uint8Array,
  jwk: JWKInterface,
  tags: Tag[]
): Buffer {
  // We're not using the optional target and anchor fields, they will always be 1 byte
  const targetLength = 1;
  const anchorLength = 1;

  // Get byte length of tags after being serialized for avro schema
  const serializedTags = serializeTags(tags);
  const tagsLength = 16 + serializedTags.byteLength;

  const arweaveSignerLength = 512;
  const ownerLength = 512;

  const signatureTypeLength = 2;

  const dataAsBuffer = Buffer.from(data);
  const dataLength = dataAsBuffer.byteLength;

  // See [https://github.com/joshbenaron/arweave-standards/blob/ans104/ans/ANS-104.md#13-dataitem-format]
  const totalByteLength =
    arweaveSignerLength +
    ownerLength +
    signatureTypeLength +
    targetLength +
    anchorLength +
    tagsLength +
    dataLength;

  // Create array with set length
  const bytes = Buffer.alloc(totalByteLength);

  bytes.set(shortTo2ByteArray(1), 0);
  // Push bytes for `signature`
  bytes.set(new Uint8Array(arweaveSignatureLength).fill(0), 2);

  // Push bytes for `owner`
  const owner = fromB64Url(jwk.n);
  if (owner.byteLength !== arweaveSignatureLength) {
    throw new Error(
      `Arweave Owner must be ${arweaveSignatureLength} bytes, but was incorrectly ${owner.byteLength}`
    );
  }

  bytes.set(owner, 2 + arweaveSignatureLength);

  const position = 2 + arweaveSignatureLength + ownerLength;
  // Push `presence byte` and push `target` if present
  // 64 + OWNER_LENGTH
  bytes[position] = 0;

  // Push `presence byte` and push `anchor` if present
  // 64 + OWNER_LENGTH
  const anchorStart = position + targetLength;
  const tagsStart = anchorStart + 1;
  bytes[anchorStart] = 0;

  bytes.set(longTo8ByteArray(tags.length), tagsStart);
  const bytesCount = longTo8ByteArray(serializedTags.byteLength);
  bytes.set(bytesCount, tagsStart + 8);
  bytes.set(serializedTags, tagsStart + 16);

  const dataStart = tagsStart + tagsLength;

  bytes.set(dataAsBuffer, dataStart);

  return bytes;
}

export function generateJunkDataItem(
  dataSizeInKB: number,
  jwk: JWKInterface,
  tags: Tag[]
): Buffer {
  const dataAsBuffer = new Uint8Array(dataSizeInKB * 1024).map(() =>
    Math.floor(Math.random() * 256)
  );
  return toDataItem(dataAsBuffer, jwk, tags);
}

const arweaveOwnerLength = 512;
export async function signDataItem(
  dataItem: Buffer,
  jwk: JWKInterface
): Promise<Buffer> {
  const rawOwner = dataItem.subarray(
    2 + arweaveSignatureLength,
    2 + arweaveSignatureLength + arweaveOwnerLength
  );
  const rawTarget = Buffer.alloc(0);
  const rawAnchor = Buffer.alloc(0);

  const tagsStart = 2 + arweaveSignatureLength + arweaveOwnerLength + 1 + 1;
  const tagsSize = byteArrayToLong(
    dataItem.subarray(tagsStart + 8, tagsStart + 16)
  );
  const rawTags = dataItem.subarray(tagsStart + 16, tagsStart + 16 + tagsSize);

  const numberOfTagBytesArray = dataItem.subarray(
    tagsStart + 8,
    tagsStart + 16
  );
  const numberOfTagBytes = byteArrayToLong(numberOfTagBytesArray);
  const dataStart = tagsStart + 16 + numberOfTagBytes;
  const rawData = dataItem.subarray(dataStart, dataItem.length);

  const sigData = await deepHash([
    Buffer.from("dataitem"),
    Buffer.from("1"),
    Buffer.from("1"),
    rawOwner,
    rawTarget,
    rawAnchor,
    rawTags,
    rawData,
  ]);

  const sigBytes = await Arweave.crypto.sign(jwk, sigData);
  const rawSig = Buffer.from(sigBytes);

  dataItem.set(rawSig, 2);

  return dataItem;
}
