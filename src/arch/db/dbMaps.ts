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
import { defaultPremiumFeatureType } from "../../constants";
import {
  DataItemFailedReason,
  FailedBundle,
  FailedBundleDBResult,
  FailedDataItem,
  FailedDataItemDBResult,
  NewBundle,
  NewBundleDBResult,
  NewDataItem,
  NewDataItemDBResult,
  PermanentBundle,
  PermanentBundleDBResult,
  PermanentDataItem,
  PermanentDataItemDBResult,
  PlannedDataItem,
  PlannedDataItemDBResult,
  PostedBundle,
  PostedBundleDBResult,
  SeededBundle,
  SeededBundleDBResult,
} from "../../types/dbTypes";
import { W } from "../../types/winston";

export function newBundleDbResultToNewBundleMap({
  bundle_id,
  plan_id,
  planned_date,
  reward,
  signed_date,
  header_byte_count,
  payload_byte_count,
  transaction_byte_count,
}: NewBundleDBResult): NewBundle {
  return {
    bundleId: bundle_id,
    planId: plan_id,
    plannedDate: planned_date,
    reward: W(reward),
    signedDate: signed_date,
    // bigInteger types come back as strings, so we convert them to numbers here
    headerByteCount: header_byte_count ? +header_byte_count : undefined,
    payloadByteCount: payload_byte_count ? +payload_byte_count : undefined,
    transactionByteCount: transaction_byte_count
      ? +transaction_byte_count
      : undefined,
  };
}

export function postedBundleDbResultToPostedBundleMap(
  dbResult: PostedBundleDBResult
): PostedBundle {
  return {
    ...newBundleDbResultToNewBundleMap(dbResult),
    postedDate: dbResult.posted_date,
  };
}

export function seededBundleDbResultToSeededBundleMap(
  dbResult: SeededBundleDBResult
): SeededBundle {
  return {
    ...postedBundleDbResultToPostedBundleMap(dbResult),
    seededDate: dbResult.seeded_date,
  };
}

export function permanentBundleDbResultToPermanentBundleMap(
  dbResult: PermanentBundleDBResult
): PermanentBundle {
  return {
    ...seededBundleDbResultToSeededBundleMap(dbResult),
    permanentDate: dbResult.permanent_date,
  };
}

export function failedBundleDbResultToFailedBundleMap(
  dbResult: FailedBundleDBResult
): FailedBundle {
  return {
    ...seededBundleDbResultToSeededBundleMap(dbResult),
    failedDate: dbResult.failed_date,
  };
}

export function newDataItemDbResultToNewDataItemMap({
  assessed_winston_price,
  byte_count,
  data_item_id,
  owner_public_address,
  uploaded_date,
  data_start,
  failed_bundles,
  signature_type,
  content_type,
  premium_feature_type,
  signature,
  deadline_height,
}: NewDataItemDBResult): NewDataItem {
  return {
    assessedWinstonPrice: W(assessed_winston_price),
    dataItemId: data_item_id,
    ownerPublicAddress: owner_public_address,
    byteCount: +byte_count,
    uploadedDate: uploaded_date,
    premiumFeatureType: premium_feature_type ?? defaultPremiumFeatureType,
    failedBundles: failed_bundles ? failed_bundles.split(",") : [],
    signatureType: signature_type ?? undefined,
    payloadDataStart: data_start ?? undefined,
    payloadContentType: content_type ?? undefined,
    signature: signature ?? undefined,
    deadlineHeight: deadline_height ? +deadline_height : undefined,
  };
}

export function plannedDataItemDbResultToPlannedDataItemMap(
  dbResult: PlannedDataItemDBResult
): PlannedDataItem {
  return {
    ...newDataItemDbResultToNewDataItemMap(dbResult),
    planId: dbResult.plan_id,
    plannedDate: dbResult.planned_date,
  };
}

export function permanentDataItemDbResultToPermanentDataItemMap({
  assessed_winston_price,
  byte_count,
  data_item_id,
  owner_public_address,
  uploaded_date,
  data_start,
  failed_bundles,
  signature_type,
  content_type,
  premium_feature_type,
  plan_id,
  planned_date,
  bundle_id,
  permanent_date,
  block_height,
  deadline_height,
}: PermanentDataItemDBResult): PermanentDataItem {
  return {
    assessedWinstonPrice: W(assessed_winston_price),
    dataItemId: data_item_id,
    ownerPublicAddress: owner_public_address,
    byteCount: +byte_count,
    uploadedDate: uploaded_date,
    premiumFeatureType: premium_feature_type ?? defaultPremiumFeatureType,
    failedBundles: failed_bundles ? failed_bundles.split(",") : [],
    signatureType: signature_type ?? undefined,
    payloadDataStart: data_start ?? undefined,
    payloadContentType: content_type ?? undefined,
    planId: plan_id,
    plannedDate: planned_date,
    bundleId: bundle_id,
    permanentDate: permanent_date,
    blockHeight: block_height,
    deadlineHeight: deadline_height ? deadline_height : undefined,
  };
}

export function failedDataItemDbResultToFailedDataItemMap({
  assessed_winston_price,
  byte_count,
  data_item_id,
  owner_public_address,
  uploaded_date,
  data_start,
  failed_bundles,
  signature_type,
  content_type,
  premium_feature_type,
  plan_id,
  planned_date,
  deadline_height,
  failed_date,
  failed_reason,
  signature,
}: FailedDataItemDBResult): FailedDataItem {
  return {
    assessedWinstonPrice: W(assessed_winston_price),
    dataItemId: data_item_id,
    ownerPublicAddress: owner_public_address,
    byteCount: +byte_count,
    uploadedDate: uploaded_date,
    premiumFeatureType: premium_feature_type ?? defaultPremiumFeatureType,
    failedBundles: failed_bundles ? failed_bundles.split(",") : [],
    signatureType: signature_type ?? undefined,
    payloadDataStart: data_start ?? undefined,
    payloadContentType: content_type ?? undefined,
    signature: signature ?? undefined,
    planId: plan_id,
    plannedDate: planned_date,
    deadlineHeight: deadline_height ? +deadline_height : undefined,
    failedDate: failed_date,
    failedReason: failed_reason as DataItemFailedReason,
  };
}
