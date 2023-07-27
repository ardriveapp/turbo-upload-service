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
  maxBundleSize as maxBundleSizeConstant,
  maxDataItemLimit as maxDataItemLimitConstant,
  maxDataItemSize as maxDataItemSizeConstant,
} from "../constants";
import logger from "../logger";
import { ByteCount, TransactionId } from "../types/types";

interface PackerBundlePlan {
  dataItemIds: TransactionId[];
  totalByteCount: ByteCount;
}
[];

interface PackerDataItem {
  dataItemId: TransactionId;
  byteCount: ByteCount;
}

interface BundlePackerParams {
  maxBundleSize?: ByteCount;
  maxDataItemSize?: ByteCount;
  maxDataItemLimit?: number;
}

export class BundlePacker {
  private readonly maxBundleSize: ByteCount;
  private readonly maxDataItemSize: ByteCount;
  private readonly maxDataItemLimit: number;

  constructor({
    maxBundleSize = maxBundleSizeConstant,
    maxDataItemSize = maxDataItemSizeConstant,
    maxDataItemLimit = maxDataItemLimitConstant,
  }: BundlePackerParams) {
    this.maxBundleSize = maxBundleSize;
    this.maxDataItemSize = maxDataItemSize;
    this.maxDataItemLimit = maxDataItemLimit;
  }

  public packDataItemsIntoBundlePlans(
    dataItems: PackerDataItem[]
  ): PackerBundlePlan[] {
    let bundlePlans: PackerBundlePlan[] = [];

    for (const dataItem of dataItems) {
      const { dataItemId, byteCount } = dataItem;

      if (byteCount > this.maxDataItemSize) {
        // This error case should already be sanitized on data item post route
        // Gracefully skip with logging
        logger.error(
          `Data item id ${dataItemId} from database had a byte count of ${byteCount} which exceeds the maximum dataItem size of ${this.maxDataItemSize}!`
        );
        continue;
      }

      if (byteCount > this.maxBundleSize) {
        logger.info(
          "Data item bigger than max bundle size, putting this into its own bundle...",
          { dataItemId, byteCount }
        );
        bundlePlans.push({
          dataItemIds: [dataItemId],
          totalByteCount: byteCount,
        });
        continue;
      }

      bundlePlans = this.packDataItem(dataItem, bundlePlans);
    }

    return bundlePlans;
  }

  private packDataItem(
    { byteCount, dataItemId }: PackerDataItem,
    bundlePlans: PackerBundlePlan[]
  ): PackerBundlePlan[] {
    for (let index = 0; index < bundlePlans.length; index++) {
      const bundlePlan = bundlePlans[index];

      if (this.fitsIntoBundle(byteCount, bundlePlan)) {
        // Put data item into bundle plan
        bundlePlans[index].dataItemIds.push(dataItemId);
        bundlePlans[index].totalByteCount =
          bundlePlan.totalByteCount + byteCount;

        return bundlePlans;
      }
    }

    // Else, create a new bundle plan with data item
    bundlePlans.push({
      dataItemIds: [dataItemId],
      totalByteCount: byteCount,
    });
    return bundlePlans;
  }

  private fitsIntoBundle(
    dataItemByteCount: ByteCount,
    { totalByteCount, dataItemIds }: PackerBundlePlan
  ) {
    const fitsInBundleTotalByteCount =
      dataItemByteCount <= this.maxBundleSize - totalByteCount;
    const fitsInBundleDataItemLimit =
      dataItemIds.length + 1 <= this.maxDataItemLimit;

    return fitsInBundleTotalByteCount && fitsInBundleDataItemLimit;
  }
}
