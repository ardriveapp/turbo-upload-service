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
import { multipartChunkMaxSize, multipartChunkMinSize } from "../constants";
import { UploadId } from "../types/types";

export class BaseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
  }
}

export class DataItemExistsWarning extends BaseError {
  constructor(dataItemId: string) {
    super(
      `Data item with ID ${dataItemId} has already been uploaded to this service!`
    );
  }
}

export class MultiPartUploadNotFound extends BaseError {
  constructor(uploadId: string) {
    super(`Multi-part upload with ID ${uploadId} not found!`);
  }
}

export class BlocklistedAddressError extends BaseError {
  constructor() {
    super(`Forbidden`);
  }
}

export class InvalidChunkSize extends BaseError {
  constructor() {
    super(
      `Chunk size must be between ${multipartChunkMinSize} - ${multipartChunkMaxSize} bytes.`
    );
  }
}

export class InvalidChunk extends BaseError {
  constructor(message?: string) {
    super(`Invalid chunk. ${message ? message : ""}.`);
  }
}

export class InvalidDataItem extends BaseError {
  constructor(message?: string) {
    super(`Invalid Data Item! ${message ? message : ""}`);
  }
}

export class DatabaseInsertError extends BaseError {
  constructor(dataItemId: string) {
    super(
      `Data Item: ${dataItemId}. Upload Service is Unavailable. Cloud Database is unreachable`
    );
  }
}

export class EnqueuedForValidationError extends BaseError {
  constructor(uploadId: UploadId) {
    super(`Upload with id ${uploadId} has been enqueued for validation.`);
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

export class BundlePlanExistsInAnotherStateWarning extends BaseError {
  constructor(planId: string, bundleId?: string) {
    super(
      `[DUPLICATE-MESSAGE] Plan id '${planId}' is already in another state! ${
        bundleId !== undefined ? `(bundleId: ${bundleId})` : ""
      }`
    );
  }
}

export class InsufficientBalance extends BaseError {
  constructor() {
    super("Insufficient balance");
  }
}

export class DataItemsStillPendingWarning extends BaseError {
  constructor() {
    super(
      `Some data items in batch do not yet return block_heights, delaying permanent bundle update`
    );
  }
}

export class PaymentServiceReturnedError extends BaseError {
  constructor(message: string) {
    super(message);
  }
}

export class BadRequest extends BaseError {
  constructor(message = "Bad Request") {
    super(message);
  }
}
