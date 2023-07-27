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
  InsertNewBundleParams,
  NewBundle,
  NewDataItem,
  PlanId,
  PlannedDataItem,
  PostedBundle,
  PostedNewDataItem,
  SeededBundle,
} from "../../types/dbTypes";
import { TransactionId, Winston } from "../../types/types";

export interface Database {
  /** Store a new data item that has been posted to the data item route */
  insertNewDataItem(dataItem: PostedNewDataItem): Promise<void>;
  /**  Get all new data items in the database sorted by uploadedDate */
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
  insertPostedBundle(bundleId: TransactionId): Promise<void>;

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
  updateBundleAsDropped(planId: PlanId): Promise<void>;

  /** Gets latest status of a data item from the database */
  getDataItemInfo(dataItemId: TransactionId): Promise<
    | {
        status: "new" | "pending" | "permanent";
        assessedWinstonPrice: Winston;
        bundleId?: TransactionId;
      }
    | undefined
  >;

  insertFailedToPostBundle(bundleId: TransactionId): Promise<void>;

  getLastDataItemInBundle(planId: PlanId): Promise<PlannedDataItem>;
}
