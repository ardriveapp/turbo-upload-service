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
import { createHash } from "crypto";
import {
  ReadStream,
  createReadStream,
  existsSync,
  mkdirSync,
  statSync,
} from "fs";
import { writeFile } from "fs/promises";
import { Readable } from "stream";

import logger from "../logger";
import { TransactionId, UploadId } from "../types/types";
import { cleanUpTempFile } from "../utils/common";
import { MoveObjectParams, ObjectStore, PayloadInfo } from "./objectStore";

const localDirectories = [
  "temp",
  "temp/bundle",
  "temp/raw-data-item",
  "temp/header",
  "temp/bundle-payload",
  "temp/data",
  "temp/multipart-uploads",
];
export class FileSystemObjectStore implements ObjectStore {
  constructor() {
    // create the directories if they don't exist
    for (const dir of localDirectories) {
      if (!existsSync(dir)) {
        mkdirSync(dir);
      }
    }
  }

  public async putObject(Key: string, fileReadStream: Readable) {
    logger.debug(`Writing file to temp/${Key} `);

    if (fileReadStream.errored) {
      throw new Error("File read stream errored");
    }

    const controller = new AbortController();
    const signal = controller.signal;
    let abortError: Error | undefined;

    fileReadStream.on("error", (error) => {
      logger.error("Aborting file write due to read stream error", error);
      abortError = error;
      controller.abort();
    });

    try {
      await writeFile(`temp/${Key}`, fileReadStream, { signal });
      logger.debug("File written successfully");
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        logger.error("writeFile aborted due to stream error");
      } else {
        logger.error("writeFile error:", error);
      }
      throw abortError ?? error;
    }
  }

  public async getObject(
    Key: string,
    Range?: string
  ): Promise<{ readable: Readable; etag: string | undefined }> {
    const range = Range?.split("=")[1].split("-");
    const start = range?.[0] ?? undefined;
    const end = range?.[1] ?? undefined;

    const getFileReadStream = () => {
      return createReadStream(`temp/${Key}`, {
        start: start !== undefined ? +start : start,
        end: end !== undefined ? +end : end,
      });
    };

    // TODO: Just use MD5 of read stream for etag?
    let readable = getFileReadStream();
    readable.on("error", () => {
      readable.destroy();
    });

    const etag = await calculateMD5(readable);
    readable = getFileReadStream();

    return {
      readable,
      etag,
    };
  }

  public getObjectByteCount(Key: string): Promise<number> {
    return Promise.resolve(statSync(`temp/${Key}`).size);
  }

  public getObjectPayloadInfo(_Key: string): Promise<PayloadInfo> {
    throw new Error("Method not implemented.");
  }

  public async removeObject(dataItemTxId: TransactionId) {
    return cleanUpTempFile(`temp/${dataItemTxId}`);
  }

  /* eslint-disable @typescript-eslint/no-unused-vars */
  // multipart uploads
  public async createMultipartUpload(_Key: string): Promise<string> {
    throw new Error("Method not implemented.");
  }

  public async uploadPart(
    _Key: string,
    _Body: Readable,
    _uploadId: UploadId,
    _partNumber: number
  ): Promise<string> {
    throw new Error("Method not implemented.");
  }

  async moveObject({
    sourceKey: _sourceKey,
    destinationKey: _destinationKey,
  }: MoveObjectParams): Promise<void> {
    throw new Error("Method not implemented.");
  }

  async completeMultipartUpload(
    _Key: string,
    _uploadId: UploadId
  ): Promise<string> {
    throw new Error("Method not implemented.");
  }

  async copyPartialObject(
    _sourceKey: string,
    _destinationKey: string,
    _start: number,
    _end: number
  ): Promise<void> {
    throw new Error("Method not implemented.");
  }

  async getMultipartUploadParts(
    _Key: string,
    _uploadId: UploadId
  ): Promise<
    {
      size: number;
      partNumber: number;
    }[]
  > {
    throw new Error("Method not implemented.");
  }

  async headObject(Key: string): Promise<{
    etag: string | undefined;
    ContentLength: number;
    ContentType: string | undefined;
  }> {
    // TODO: Just use MD5 of read stream for etag?
    const readable = createReadStream(`temp/${Key}`);
    readable.on("error", () => {
      readable.destroy();
    });

    const etag = await calculateMD5(readable);
    return {
      etag,
      ContentLength: await this.getObjectByteCount(Key),
      ContentType: "application/octet-stream", // TODO: undefined better?
    };
  }
}

function calculateMD5(readStream: ReadStream): Promise<string> {
  return new Promise((resolve, reject) => {
    // Create a hash object
    const hash = createHash("md5");

    readStream.on("data", (data) => {
      // Update hash with data chunk
      hash.update(data);
    });

    readStream.on("end", () => {
      // Finalize the hash and resolve the promise
      const md5 = hash.digest("hex");
      resolve(md5);
    });

    readStream.on("error", (err) => {
      // Reject the promise on error
      reject(err);
    });
  });
}
