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
import { Tag, byteArrayToLong, deserializeTags } from "arbundles";
import Arweave from "arweave";
import * as fs from "fs";
import { PathLike } from "fs";
import { promisify } from "util";

import { DataItemId, PublicArweaveAddress } from "../types/types";
import { ownerToAddress, toB64Url } from "../utils/base64";
import { verifyDataItem } from "./verifyDataItem";

export const arweaveSignatureLength = 512;
export const dataItemTagsByteLimit = 4096;

const read = promisify(fs.read);
export const readFileMode = "r";

export default class FileDataItem {
  constructor(public readonly filename: PathLike) {}

  public isValid(): Promise<boolean> {
    return verifyDataItem(this);
  }

  public async size(): Promise<number> {
    return await fs.promises.stat(this.filename).then((r) => r.size);
  }

  async signatureType(): Promise<number> {
    const handle = await fs.promises.open(this.filename, readFileMode);
    const buffer = await read(handle.fd, Buffer.allocUnsafe(2), 0, 2, 0).then(
      (r) => r.buffer
    );
    await handle.close();
    return byteArrayToLong(buffer);
  }

  async rawSignature(): Promise<Buffer> {
    const handle = await fs.promises.open(this.filename, readFileMode);
    const length = arweaveSignatureLength;

    const buffer = await read(
      handle.fd,
      Buffer.alloc(length),
      0,
      length,
      2
    ).then((r) => r.buffer);
    await handle.close();
    return buffer;
  }

  async signature(): Promise<string> {
    return toB64Url(await this.rawSignature());
  }

  async rawOwner(): Promise<Buffer> {
    const handle = await fs.promises.open(this.filename, readFileMode);
    const length = arweaveSignatureLength;
    const buffer = await read(
      handle.fd,
      Buffer.allocUnsafe(length),
      0,
      length,
      2 + arweaveSignatureLength
    ).then((r) => r.buffer);
    await handle.close();
    return buffer;
  }

  async owner(): Promise<string> {
    return toB64Url(await this.rawOwner());
  }

  async ownerPublicAddress(): Promise<PublicArweaveAddress> {
    return ownerToAddress(await this.owner());
  }

  async rawTarget(): Promise<Buffer> {
    const handle = await fs.promises.open(this.filename, readFileMode);
    const targetStart = this.getTargetStart();
    const targetPresentBuffer = await read(
      handle.fd,
      Buffer.allocUnsafe(1),
      0,
      1,
      targetStart
    ).then((r) => r.buffer);
    const targetPresent = targetPresentBuffer[0] === 1;
    if (targetPresent) {
      const targetBuffer = await read(
        handle.fd,
        Buffer.allocUnsafe(32),
        0,
        32,
        targetStart + 1
      ).then((r) => r.buffer);
      await handle.close();
      return targetBuffer;
    }
    await handle.close();
    return Buffer.allocUnsafe(0);
  }

  async target(): Promise<string> {
    return toB64Url(await this.rawTarget());
  }

  getTargetStart(): number {
    return 2 + arweaveSignatureLength + arweaveSignatureLength;
  }

  async rawAnchor(): Promise<Buffer> {
    const { isAnchorPresent, anchorStart } = await this.anchorStart();

    if (isAnchorPresent) {
      const handle = await fs.promises.open(this.filename, readFileMode);
      const anchorBuffer = await read(
        handle.fd,
        Buffer.allocUnsafe(32),
        0,
        32,
        anchorStart + 1
      ).then((r) => r.buffer);
      await handle.close();
      return anchorBuffer;
    }
    return Buffer.allocUnsafe(0);
  }

  async anchor(): Promise<string> {
    return (await this.rawAnchor()).toString();
  }

  async rawTags(): Promise<Buffer> {
    const handle = await fs.promises.open(this.filename, readFileMode);
    const tagsStart = await this.getTagsStart();
    const numberOfTagsBuffer = await read(
      handle.fd,
      Buffer.allocUnsafe(8),
      0,
      8,
      tagsStart
    ).then((r) => r.buffer);
    const numberOfTags = byteArrayToLong(numberOfTagsBuffer);
    if (numberOfTags === 0) {
      await handle.close();
      return Buffer.allocUnsafe(0);
    }
    const numberOfTagsBytesBuffer = await read(
      handle.fd,
      Buffer.allocUnsafe(8),
      0,
      8,
      tagsStart + 8
    ).then((r) => r.buffer);
    const numberOfTagsBytes = byteArrayToLong(numberOfTagsBytesBuffer);
    if (numberOfTagsBytes > dataItemTagsByteLimit) {
      await handle.close();
      throw new Error("Tags too large");
    }
    const tagsBytes = await read(
      handle.fd,
      Buffer.allocUnsafe(numberOfTagsBytes),
      0,
      numberOfTagsBytes,
      tagsStart + 16
    ).then((r) => r.buffer);
    await handle.close();
    return tagsBytes;
  }

  async tags(): Promise<Tag[]> {
    const tagsBytes = await this.rawTags();
    if (tagsBytes.byteLength === 0) return [];
    return deserializeTags(tagsBytes);
  }

  async rawData(): Promise<Buffer> {
    const dataStart = await this.dataStart();
    const size = await this.size();
    const dataSize = size - dataStart;
    if (dataSize === 0) {
      return Buffer.allocUnsafe(0);
    }
    const handle = await fs.promises.open(this.filename, readFileMode);

    const dataBuffer = await read(
      handle.fd,
      Buffer.allocUnsafe(dataSize),
      0,
      dataSize,
      dataStart
    ).then((r) => r.buffer);
    await handle.close();
    return dataBuffer;
  }

  async data(): Promise<string> {
    return toB64Url(await this.rawData());
  }

  async getRawId(): Promise<Buffer> {
    return Buffer.from(await Arweave.crypto.hash(await this.rawSignature()));
  }

  async getTxId(): Promise<DataItemId> {
    return toB64Url(await this.getRawId());
  }

  public async getTagsStart(): Promise<number> {
    const { isAnchorPresent, anchorStart } = await this.anchorStart();
    let tagsStart = anchorStart;
    tagsStart += isAnchorPresent ? 33 : 1;
    return tagsStart;
  }

  public async dataStart(): Promise<number> {
    const handle = await fs.promises.open(this.filename, readFileMode);
    const tagsStart = await this.getTagsStart();
    const numberOfTagsBytesBuffer = await read(
      handle.fd,
      Buffer.allocUnsafe(8),
      0,
      8,
      tagsStart + 8
    ).then((r) => r.buffer);
    const numberOfTagsBytes = byteArrayToLong(numberOfTagsBytesBuffer);
    await handle.close();
    return tagsStart + 16 + numberOfTagsBytes;
  }

  private async anchorStart(): Promise<{
    isAnchorPresent: boolean;
    anchorStart: number;
  }> {
    const targetStart = this.getTargetStart();
    const handle = await fs.promises.open(this.filename, readFileMode);
    const targetPresentBuffer = await read(
      handle.fd,
      Buffer.allocUnsafe(1),
      0,
      1,
      targetStart
    ).then((r) => r.buffer);
    const targetPresent = targetPresentBuffer[0] === 1;
    const anchorStart = targetStart + (targetPresent ? 33 : 1);
    const anchorPresentBuffer = await read(
      handle.fd,
      Buffer.allocUnsafe(1),
      0,
      1,
      anchorStart
    ).then((r) => r.buffer);
    const isAnchorPresent = anchorPresentBuffer[0] === 1;
    await handle.close();
    return { isAnchorPresent, anchorStart };
  }
}
