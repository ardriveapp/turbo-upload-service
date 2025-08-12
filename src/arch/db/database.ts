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
  DataItemFailedReason,
  FinishedMultiPartUpload,
  InFlightMultiPartUpload,
  InsertNewBundleParams,
  MultipartUploadFailedReason,
  NewBundle,
  NewDataItem,
  PlanId,
  PlannedDataItem,
  PostedBundle,
  PostedNewDataItem,
  SeededBundle,
} from "../../types/dbTypes";
import {
  DataItemId,
  TransactionId,
  UploadId,
  Winston,
} from "../../types/types";

// TODO: this could be an interface since no functions have a default implementation
export interface Database {
  /** Store a new data item that has been posted to the service */
  insertNewDataItem(dataItem: PostedNewDataItem): Promise<void>;

  /** Stores a batch of new data items that have been enqueued for insert */
  insertNewDataItemBatch(dataItemBatch: PostedNewDataItem[]): Promise<void>;

  /**  Gets MAX_DATA_ITEM_LIMIT * 5 (75,000 as of this commit) new data items in the database sorted by uploadedDate */
  getNewDataItems(): Promise<NewDataItem[]>;

  /**
   * Creates a bundle plan transaction
   *
   * - Inserts new BundlePlan
   * - For each dataItemId:
   *   - Deletes NewDataItem
   *   - Adds PlannedDataItem
   */
  insertBundlePlan(planId: PlanId, dataItemIds: TransactionId[]): Promise<void>;

  getPlannedDataItemsForPlanId(planId: PlanId): Promise<PlannedDataItem[]>;

  getPlannedDataItemsForVerification(
    planId: PlanId
  ): Promise<PlannedDataItem[]>;

  /**
   * Creates a new bundle transaction
   *
   * - Deletes BundlePlan
   * - Inserts NewBundle
   */
  insertNewBundle({
    planId,
    bundleId,
    reward,
  }: InsertNewBundleParams): Promise<void>;

  getNextBundleToPostByPlanId(planId: PlanId): Promise<NewBundle>;

  /**
   * Creates posted bundle transaction
   *
   * - Delete NewBundle
   * - Insert PostedBundle
   */
  insertPostedBundle({
    bundleId,
    usdToArRate,
  }: {
    bundleId: TransactionId;
    usdToArRate?: number;
  }): Promise<void>;

  getNextBundleAndDataItemsToSeedByPlanId(planId: PlanId): Promise<{
    bundleToSeed: PostedBundle;
    dataItemsToSeed: PlannedDataItem[];
  }>;

  /**
   * Creates seeded bundle transaction
   *
   * - Delete PostedBundle
   * - Insert SeededBundle
   */
  insertSeededBundle(bundleId: TransactionId): Promise<void>;

  getSeededBundles(limit?: number): Promise<SeededBundle[]>;

  updateBundleAsPermanent(
    planId: PlanId,
    blockHeight: number,
    indexedOnGQL: boolean
  ): Promise<void>;

  updateDataItemsAsPermanent(
    params: UpdateDataItemsToPermanentParams
  ): Promise<void>;
  updateDataItemsToBeRePacked(
    dataItemIds: TransactionId[],
    failedBundleId: TransactionId
  ): Promise<void>;

  updateSeededBundleToDropped(
    planId: PlanId,
    bundleId: TransactionId
  ): Promise<void>;
  updateNewBundleToFailedToPost(
    planId: PlanId,
    bundleId: TransactionId
  ): Promise<void>;

  /** Gets latest status of a data item from the database */
  getDataItemInfo(dataItemId: TransactionId): Promise<
    | {
        status: "new" | "pending" | "permanent" | "failed";
        assessedWinstonPrice: Winston;
        bundleId?: TransactionId;
        uploadedTimestamp: number;
        deadlineHeight?: number;
        failedReason?: DataItemFailedReason;
      }
    | undefined
  >;

  getLastDataItemInBundle(planId: PlanId): Promise<PlannedDataItem>;

  /**
   * Multipart uploads
   */
  insertInFlightMultiPartUpload({
    uploadId,
    uploadKey,
  }: {
    uploadId: UploadId;
    uploadKey: string;
  }): Promise<InFlightMultiPartUpload>;
  finalizeMultiPartUpload(params: {
    uploadId: UploadId;
    etag: string;
    dataItemId: string;
  }): Promise<void>;
  getInflightMultiPartUpload(
    uploadId: UploadId
  ): Promise<InFlightMultiPartUpload>;
  failInflightMultiPartUpload({
    uploadId,
    failedReason,
  }: {
    uploadId: UploadId;
    failedReason: MultipartUploadFailedReason;
  }): Promise<InFlightMultiPartUpload>;
  failFinishedMultiPartUpload({
    uploadId,
    failedReason,
  }: {
    uploadId: UploadId;
    failedReason: MultipartUploadFailedReason;
  }): Promise<FinishedMultiPartUpload>;
  getFinalizedMultiPartUpload(
    uploadId: UploadId
  ): Promise<FinishedMultiPartUpload>;
  updateMultipartChunkSize(
    chunkSize: number,
    upload: InFlightMultiPartUpload
  ): Promise<number>;

  updatePlannedDataItemAsFailed(params: {
    dataItemId: DataItemId;
    failedReason: DataItemFailedReason;
  }): Promise<void>;
}

export type UpdateDataItemsToPermanentParams = {
  dataItemIds: string[];
  blockHeight: number;
  bundleId: string;
};
