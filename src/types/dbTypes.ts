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
import {
  ByteCount,
  PublicArweaveAddress,
  TransactionId,
  Winston,
} from "./types";

export type Timestamp = string;

export type PlanId = string;

export type SignatureType = number;
export type DataStart = number;
export type BlockHeight = number;

export type FailedReason = "not_found" | "failed_to_post";

export interface NewDataItem {
  dataItemId: TransactionId;
  ownerPublicAddress: PublicArweaveAddress;
  byteCount: ByteCount;
  assessedWinstonPrice: Winston;
  uploadedDate: Timestamp;

  failedBundles: TransactionId[];
  signatureType?: SignatureType;
  dataStart?: DataStart;
}
export type PostedNewDataItem = Omit<Required<NewDataItem>, "uploadedDate">;

export interface PlannedDataItem extends NewDataItem {
  planId: PlanId;
  plannedDate: Timestamp;
}

export interface PermanentDataItem extends PlannedDataItem {
  bundleId: TransactionId;
  permanentDate: Timestamp;
  blockHeight: BlockHeight;
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
  failedReason?: FailedReason;
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
  signature_type: number;
  data_start: number;
  failed_bundles: string;
}

export interface NewDataItemDBResult extends NewDataItemDB {
  uploaded_date: string;

  signature_type: number | null;
  data_start: number | null;
  failed_bundles: string | null;
}

export interface PlannedDataItemDBInsert extends NewDataItemDBInsert {
  plan_id: string;
  uploaded_date: string;
}

export interface PlannedDataItemDBResult extends PlannedDataItemDBInsert {
  planned_date: string;
}

export interface PermanentDataItemDBInsert extends PlannedDataItemDBResult {
  block_height: string;
  bundle_id: string;
}

export interface PermanentDataItemDBResult extends PermanentDataItemDBInsert {
  permanent_date: string;
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
  transaction_byte_count: number;
  header_byte_count: number;
  payload_byte_count: number;
}

export interface NewBundleDBResult extends NewBundleDB {
  signed_date: string;

  transaction_byte_count: number | null;
  header_byte_count: number | null;
  payload_byte_count: number | null;
}

export type PostedBundleDbInsert = NewBundleDBResult;

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
