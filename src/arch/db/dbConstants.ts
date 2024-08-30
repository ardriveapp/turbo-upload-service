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
export const tableNames = {
  bundlePlan: "bundle_plan",
  failedBundle: "failed_bundle",
  newBundle: "new_bundle",
  newDataItem: "new_data_item",
  permanentBundle: "permanent_bundle",
  /** @deprecated in favor of partitioned table `permanent_data_items`  */
  permanentDataItem: "permanent_data_item",
  permanentDataItems: "permanent_data_items",
  plannedDataItem: "planned_data_item",
  failedDataItem: "failed_data_item",
  postedBundle: "posted_bundle",
  seededBundle: "seeded_bundle",
  /** @deprecated */
  seedResult: "seed_result",

  // multipart
  inFlightMultiPartUpload: "in_flight_multi_part_upload",
  finishedMultiPartUpload: "finished_multi_part_upload",
} as const;

export const columnNames = {
  blockHeight: "block_height",
  bundleId: "bundle_id",
  byteCount: "byte_count",
  contentType: "content_type",
  dataItemId: "data_item_id",
  dataStart: "data_start",
  deadlineHeight: "deadline_height",
  failedBundles: "failed_bundles",
  failedDate: "failed_date",
  failedReason: "failed_reason",
  headerByteCount: "header_byte_count",
  indexedOnGQL: "indexed_on_gql",
  owner: "owner_public_address",
  payloadByteCount: "payload_byte_count",
  permanentDate: "permanent_date",
  planId: "plan_id",
  plannedDate: "planned_date",
  postedDate: "posted_date",
  reward: "reward",
  seededDate: "seeded_date",
  signature: "signature",
  signatureType: "signature_type",
  signedDate: "signed_date",
  transactionByteCount: "transaction_byte_count",
  winstonPrice: "assessed_winston_price",
  uploadedDate: "uploaded_date",
  usdToArRate: "usd_to_ar_rate",

  // multipart
  uploadId: "upload_id",
  uploadKey: "upload_key",
  createdAt: "created_at",
  expiresAt: "expires_at",
  finalizedAt: "finalized_at",
  etag: "etag",
  chunkSize: "chunk_size",

  premiumFeatureType: "premium_feature_type",

  /** @deprecated */
  seedResultStatus: "seed_result_status",
} as const;
