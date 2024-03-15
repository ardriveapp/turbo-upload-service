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
import { chunkMaxSize, chunkMinSize } from "../routes/multiPartUploads";
import { UploadId } from "../types/types";

export class DataItemExistsWarning extends Error {
  constructor(dataItemId: string) {
    super(
      `Data item with ID ${dataItemId} has already been uploaded to this service!`
    );
    this.name = "DataItemExistsWarning";
  }
}

export class MultiPartUploadNotFound extends Error {
  constructor(uploadId: string) {
    super(`Multi-part upload with ID ${uploadId} not found!`);
    this.name = "MultiPartUploadNotFound";
  }
}

export class BlocklistedAddressError extends Error {
  constructor() {
    super(`Forbidden`);
    this.name = "BlocklistedAddressError";
  }
}

export class InvalidChunkSize extends Error {
  constructor() {
    super(
      `Chunk size must be between ${chunkMinSize} - ${chunkMaxSize} bytes.`
    );
    this.name = "InvalidChunkSize";
  }
}

export class InvalidChunk extends Error {
  constructor(message?: string) {
    super(`Invalid chunk. ${message ? message : ""}.`);
    this.name = "InvalidChunk";
  }
}

export class InvalidDataItem extends Error {
  constructor(message?: string) {
    super(`Invalid Data Item! ${message ? message : ""}`);
    this.name = "InvalidDataItem";
  }
}

export class DatabaseInsertError extends Error {
  constructor(dataItemId: string) {
    super(
      `Data Item: ${dataItemId}. Upload Service is Unavailable. Cloud Database is unreachable`
    );
    this.name = "DatabaseError";
  }
}

export class EnqueuedForValidationError extends Error {
  constructor(uploadId: UploadId) {
    super(`Upload with id ${uploadId} has been enqueued for validation.`);
    this.name = "EnqueuedForValidationError";
  }
}

export interface PostgresError {
  code: string;
  constraint: string;
  detail: string;
  file: string;
  length: number;
  line: string;
  name: string;
  routine: string;
  schema: string;
  severity: string;
  table: string;
}

export const postgresInsertFailedPrimaryKeyNotUniqueCode = "23505";
export const postgresTableRowsLockedUniqueCode = "55P03";

export class BundlePlanExistsInAnotherStateWarning extends Error {
  constructor(planId: string, bundleId: string) {
    super(
      `[DUPLICATE-MESSAGE] Plan id '${planId}' is already in another state! (bundleId: ${bundleId})`
    );
    this.name = "BundlePlanExistsInAnotherStateWarning";
  }
}

export class InsufficientBalance extends Error {
  constructor() {
    super("Insufficient balance");
    this.name = "InsufficientBalance";
  }
}

export class DataItemsStillPendingWarning extends Error {
  constructor() {
    super(
      `Some data items in batch do not yet return block_heights, delaying permanent bundle update`
    );
    this.name = "DataItemsStillPendingWarning";
  }
}
