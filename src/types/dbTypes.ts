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
import {
  ByteCount,
  PublicArweaveAddress,
  TransactionId,
  UploadId,
  Winston,
} from "./types";

// TODO: Timestamp type. Currently using Postgres Date type (ISO String without Timezone)
export type Timestamp = string;

// TODO: Plan ID type
export type PlanId = string;

export type SignatureType = number;
export type Signature = Buffer;
export type DataStart = number;
export type BlockHeight = number;

export type BundleFailedReason = "not_found" | "failed_to_post";
export type DataItemFailedReason =
  | "missing_from_object_store"
  | "too_many_failures";

export type ContentType = string;

export interface NewDataItem {
  dataItemId: TransactionId;
  ownerPublicAddress: PublicArweaveAddress;
  byteCount: ByteCount;
  assessedWinstonPrice: Winston;
  uploadedDate: Timestamp;
  premiumFeatureType: string;

  failedBundles: TransactionId[];
  signatureType?: SignatureType;
  payloadContentType?: ContentType;
  payloadDataStart?: DataStart;
  signature?: Signature;
  deadlineHeight?: BlockHeight;
}
export type PostedNewDataItem = Required<NewDataItem>;

export interface PlannedDataItem extends NewDataItem {
  planId: PlanId;
  plannedDate: Timestamp;
}

export interface PermanentDataItem extends Omit<PlannedDataItem, "signature"> {
  bundleId: TransactionId;
  permanentDate: Timestamp;
  blockHeight: BlockHeight;
}

export interface FailedDataItem extends PlannedDataItem {
  failedDate: Timestamp;
  failedReason: DataItemFailedReason;
}

export interface BundlePlan {
  planId: PlanId;
  plannedDate: Timestamp;
}

export interface NewBundle extends BundlePlan {
  bundleId: TransactionId;
  signedDate: Timestamp;
  reward: Winston;

  transactionByteCount?: ByteCount;
  headerByteCount?: ByteCount;
  payloadByteCount?: ByteCount;
}

export interface InsertNewBundleParams {
  planId: PlanId;
  bundleId: TransactionId;
  reward: Winston;
  transactionByteCount: ByteCount;
  headerByteCount: ByteCount;
  payloadByteCount: ByteCount;
}

export interface PostedBundle extends NewBundle {
  postedDate: Timestamp;
}

export interface SeededBundle extends PostedBundle {
  seededDate: Timestamp;
}

export interface FailedBundle extends SeededBundle {
  failedDate: Timestamp;
  failedReason?: BundleFailedReason;
}

export interface PermanentBundle extends SeededBundle {
  permanentDate: Timestamp;
}

export type KnexRawResult = {
  command: string; // "SELECT" |
  rowCount: number; // 1
  oid: unknown; // null
  rows: { table_name: string }[]; //  [ { table_name: 'new_data_item_1' } ]
  fields: {
    name: string; // "table_name"
    tableID: number; // 13276
    columnID: number; // 3
    dataTypeID: number; // 19
    dataTypeSize: number; // 64
    dataTypeModifier: number; // -1
    format: "text";
  }[];

  // ...
  // _parsers: [ [Function: noParse] ],
  // _types: TypeOverrides {
  //   _types: {
  //     getTypeParser: [Function: getTypeParser],
  //     setTypeParser: [Function: setTypeParser],
  //     arrayParser: [Object],
  //     builtins: [Object]
  //   },
  //   text: {},
  //   binary: {}
  // },
  // RowCtor: null,
  // rowAsArray: false
  // ...
};

interface NewDataItemDB {
  data_item_id: string;
  owner_public_address: string;
  byte_count: string;
  assessed_winston_price: string;
}

export interface NewDataItemDBInsert extends NewDataItemDB {
  uploaded_date?: string;
  signature_type: number;
  data_start: number;
  failed_bundles: string;
  content_type: string;
  premium_feature_type: string;
  signature: Buffer;
  deadline_height: string;
}

export type RePackDataItemDbInsert = NewDataItemDB & {
  signature_type: number | null;
  data_start: number | null;
  failed_bundles: string | null;
  content_type: string | null;
  premium_feature_type: string | null;
  signature: Buffer | null;
};

export interface NewDataItemDBResult extends NewDataItemDB {
  uploaded_date: string;

  signature_type: number | null;
  data_start: number | null;
  failed_bundles: string | null;
  content_type: string | null;
  premium_feature_type: string | null;
  signature: Buffer | null;
  deadline_height: string | null;
}

export interface PlannedDataItemDBInsert extends NewDataItemDBResult {
  plan_id: string;
  uploaded_date: string;
  planned_date: string;
}

export type PlannedDataItemDBResult = PlannedDataItemDBInsert;

export type PermanentDataItemDBInsert = {
  data_item_id: string;
  owner_public_address: string;
  byte_count: string;
  uploaded_date: string;
  assessed_winston_price: string;
  plan_id: string;
  planned_date: string;
  bundle_id: string;

  block_height: number;

  data_start: number | null;
  signature_type: number | null;
  failed_bundles: string | null;
  content_type: string | null;
  premium_feature_type: string | null;
  deadline_height: number | null;
};

export type PermanentDataItemDBResult = PermanentDataItemDBInsert & {
  permanent_date: string;
};

export interface FailedDataItemDBInsert extends PlannedDataItemDBResult {
  failed_reason: DataItemFailedReason;
}

export interface FailedDataItemDBResult extends FailedDataItemDBInsert {
  failed_date: string;
}

export interface BundlePlanDBInsert {
  plan_id: string;
}

export interface BundlePlanDBResult
  extends BundlePlanDBInsert,
    Record<string, unknown> {
  planned_date: string;
}

interface NewBundleDB extends BundlePlanDBResult {
  bundle_id: string;
  reward: string;
}

export interface NewBundleDBInsert extends NewBundleDB {
  transaction_byte_count: string;
  header_byte_count: string;
  payload_byte_count: string;
}

export interface NewBundleDBResult extends NewBundleDB {
  signed_date: string;

  transaction_byte_count: string | null;
  header_byte_count: string | null;
  payload_byte_count: string | null;
}

export interface PostedBundleDbInsert extends NewBundleDBResult {
  usd_to_ar_rate?: number; // optional, as we don't want to block data item posting if the rate is not available
}

export interface PostedBundleDBResult extends PostedBundleDbInsert {
  posted_date: string;
}

export type SeededBundleDbInsert = PostedBundleDBResult;

export interface SeededBundleDBResult extends SeededBundleDbInsert {
  seeded_date: string;
}

export interface PermanentBundleDbInsert extends SeededBundleDBResult {
  block_height: string;
  indexed_on_gql: boolean;
}

export interface PermanentBundleDBResult extends PermanentBundleDbInsert {
  permanent_date: string;
}

export interface FailedBundleDbInsert extends SeededBundleDBResult {
  failed_reason: string;
}

export interface FailedBundleDBResult extends SeededBundleDBResult {
  failed_date: string;
  failed_reason: string | null;
}

/** @deprecated */
export type SeedResultStates =
  | "posted"
  | "confirmed"
  | "well-seeded"
  | "permanent"
  | "dropped";

/** @deprecated */
export interface SeedResultDBInsert {
  bundle_id: string;
  seed_result_status: SeedResultStates;
  block_height?: string;
  plan_id: string;
}

/** @deprecated */
export type SeedResultDBResult = SeedResultDBInsert;

export interface InFlightMultiPartUploadDBInsert {
  upload_id: string;
  upload_key: string;
  created_at: string;
  expires_at: string;
  chunk_size?: string;
  failed_reason?: string;
}

// eslint-disable-next-line @typescript-eslint/no-empty-interface
export interface InFlightMultiPartUploadDBResult
  extends InFlightMultiPartUploadDBInsert {}

export interface FinishedMultiPartUploadDBInsert
  extends InFlightMultiPartUploadDBResult {
  finalized_at: string;
  data_item_id: TransactionId;
  etag: string;
}

export type FinishedMultiPartUploadDBResult = FinishedMultiPartUploadDBInsert;

export interface InFlightMultiPartUploadParams {
  uploadId: UploadId;
  uploadKey: string;
  chunkSize?: number;
}

export interface InFlightMultiPartUpload {
  uploadId: UploadId;
  uploadKey: string;
  createdAt: Timestamp;
  expiresAt: Timestamp;
  chunkSize?: number;
  failedReason?: MultipartUploadFailedReason;
}

export interface FinishedMultiPartUpload extends InFlightMultiPartUpload {
  finalizedAt: Timestamp;
  dataItemId: TransactionId;
  etag: string;
}

export type MultipartUploadFailedReason =
  | "INVALID"
  | "UNDERFUNDED"
  | "APPROVAL_FAILED"
  | "REVOKE_FAILED";

export type DataItemDbResults =
  | NewDataItemDBResult
  | PlannedDataItemDBResult
  | PermanentDataItemDBResult
  | FailedDataItemDBResult;
