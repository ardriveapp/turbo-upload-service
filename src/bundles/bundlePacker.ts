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
  defaultOverdueThresholdMs,
  maxBundleDataItemsByteCount as maxBundleSizeConstant,
  maxDataItemsPerBundle as maxDataItemLimitConstant,
  maxSingleDataItemByteCount as maxDataItemSizeConstant,
} from "../constants";
import logger from "../logger";
import { Timestamp } from "../types/dbTypes";
import { ByteCount, TransactionId } from "../types/types";
import { dataItemIsOverdue } from "../utils/planningUtils";

export interface PackerBundlePlan {
  dataItemIds: TransactionId[];
  totalByteCount: ByteCount;
  containsOverdueDataItems: boolean;
  dataItemSizes: Record<TransactionId, ByteCount>;
}
[];

interface PackerDataItem {
  dataItemId: TransactionId;
  byteCount: ByteCount;
  uploadedDate: Timestamp;
}

interface BundlePackerParams {
  maxTotalDataItemsByteCount?: ByteCount;
  maxSingleDataItemByteCount?: ByteCount;
  maxDataItemsCount?: number;
  overdueDataItemThresholdMs?: number;
}

export class BundlePacker {
  readonly maxTotalDataItemsByteCount: ByteCount;
  readonly maxSingleDataItemByteCount: ByteCount;
  readonly maxDataItemsCount: number;
  readonly overdueDataItemThresholdMs: number;

  constructor({
    maxTotalDataItemsByteCount = maxBundleSizeConstant,
    maxSingleDataItemByteCount = maxDataItemSizeConstant,
    maxDataItemsCount = maxDataItemLimitConstant,
    overdueDataItemThresholdMs = defaultOverdueThresholdMs,
  }: BundlePackerParams) {
    this.maxTotalDataItemsByteCount = maxTotalDataItemsByteCount;
    this.maxSingleDataItemByteCount = maxSingleDataItemByteCount;
    this.maxDataItemsCount = maxDataItemsCount;
    this.overdueDataItemThresholdMs = overdueDataItemThresholdMs;
  }

  public planHasCapacity(plan: PackerBundlePlan): boolean {
    return (
      plan.dataItemIds.length < this.maxDataItemsCount &&
      plan.totalByteCount < this.maxTotalDataItemsByteCount
    );
  }

  public packDataItemsIntoBundlePlans(
    dataItems: PackerDataItem[]
  ): PackerBundlePlan[] {
    let bundlePlans: PackerBundlePlan[] = [];

    for (const dataItem of dataItems) {
      const { dataItemId, byteCount } = dataItem;

      if (byteCount > this.maxSingleDataItemByteCount) {
        // This error case should already be sanitized on data item post route
        // Gracefully skip with logging
        logger.error(
          `Data item id ${dataItemId} from database had a byte count of ${byteCount} which exceeds the maximum dataItem size of ${this.maxSingleDataItemByteCount}!`
        );
        continue;
      }

      if (byteCount > this.maxTotalDataItemsByteCount) {
        logger.debug(
          "Data item bigger than max bundle size, putting this into its own bundle...",
          { dataItemId, byteCount }
        );
        bundlePlans.push({
          dataItemIds: [dataItemId],
          totalByteCount: byteCount,
          containsOverdueDataItems: dataItemIsOverdue(
            dataItem,
            this.overdueDataItemThresholdMs
          ),
          dataItemSizes: { [dataItemId]: byteCount },
        });
        continue;
      }

      bundlePlans = this.packDataItem(dataItem, bundlePlans);
    }

    // sort all data items from smallest to largest in each bundle
    bundlePlans.forEach((bundlePlan) => {
      bundlePlan.dataItemIds.sort(
        (a, b) => bundlePlan.dataItemSizes[a] - bundlePlan.dataItemSizes[b]
      );
    });

    return bundlePlans;
  }

  private packDataItem(
    dataItem: PackerDataItem,
    bundlePlans: PackerBundlePlan[]
  ): PackerBundlePlan[] {
    const { byteCount, dataItemId } = dataItem;
    for (let index = 0; index < bundlePlans.length; index++) {
      const bundlePlan = bundlePlans[index];

      if (this.fitsIntoBundle(byteCount, bundlePlan)) {
        // Put data item into bundle plan
        bundlePlan.dataItemIds.push(dataItemId);
        bundlePlan.totalByteCount = bundlePlan.totalByteCount + byteCount;
        bundlePlan.dataItemSizes[dataItemId] = byteCount;

        bundlePlan.containsOverdueDataItems =
          bundlePlan.containsOverdueDataItems ||
          dataItemIsOverdue(dataItem, this.overdueDataItemThresholdMs);

        return bundlePlans;
      }
    }

    // Else, create a new bundle plan with data item
    bundlePlans.push({
      dataItemIds: [dataItemId],
      totalByteCount: byteCount,
      dataItemSizes: { [dataItemId]: byteCount },
      containsOverdueDataItems: dataItemIsOverdue(
        dataItem,
        this.overdueDataItemThresholdMs
      ),
    });
    return bundlePlans;
  }

  private fitsIntoBundle(
    dataItemByteCount: ByteCount,
    { totalByteCount, dataItemIds }: PackerBundlePlan
  ) {
    const fitsInBundleTotalByteCount =
      dataItemByteCount <= this.maxTotalDataItemsByteCount - totalByteCount;
    const fitsInBundleDataItemLimit =
      dataItemIds.length + 1 <= this.maxDataItemsCount;

    return fitsInBundleTotalByteCount && fitsInBundleDataItemLimit;
  }
}
