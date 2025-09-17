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
import { Readable } from "stream";

import { BundleHeaderInfo } from "../bundles/assembleBundleHeader";
import { PayloadInfo, UploadId } from "../types/types";

export interface ObjectStoreOptions {
  contentType?: string;
  contentLength?: number;
  payloadInfo?: PayloadInfo;
}

export interface MoveObjectParams {
  sourceKey: string;
  destinationKey: string;
  Options: ObjectStoreOptions;
}

export interface ObjectStore {
  putObject(
    Key: string,
    Body: Readable,
    Options?: ObjectStoreOptions
  ): Promise<void>;
  getObject(
    Key: string,
    Range?: string
  ): Promise<{ readable: Readable; etag: string | undefined }>;
  headObject(Key: string): Promise<{
    etag: string | undefined;
    ContentLength: number;
    ContentType: string | undefined;
  }>;
  getObjectByteCount(Key: string): Promise<number>;
  moveObject(params: {
    sourceKey: string;
    destinationKey: string;
    Options?: ObjectStoreOptions;
  }): Promise<void>;
  getObjectPayloadInfo(Key: string): Promise<PayloadInfo>;

  // multipart uploads
  createMultipartUpload(Key: string): Promise<string>;
  completeMultipartUpload(key: string, uploadId: UploadId): Promise<string>;
  uploadPart(
    Key: string,
    Body: Readable,
    uploadId: UploadId,
    partNumber: number,
    ContentLength: number
  ): Promise<string>;
  // NOTE: this may be better moved to database interface. We may not want to make object stores responsible for keep tracking of their parts.
  getMultipartUploadParts(
    Key: string,
    uploadId: UploadId
  ): Promise<
    {
      size: number;
      partNumber: number;
    }[]
  >;

  getBundleHeaderInfo(Key: string, range: string): Promise<BundleHeaderInfo>;
}
