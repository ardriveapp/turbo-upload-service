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
import { BundlePacker, PackerBundlePlan } from "../bundles/bundlePacker";
import { Timestamp } from "../types/dbTypes";

export function factorBundlesByTargetSize(
  plans: PackerBundlePlan[],
  bundlePacker: BundlePacker
): {
  underweightBundlePlans: PackerBundlePlan[];
  bundlePlans: PackerBundlePlan[];
} {
  return plans.reduce(
    (acc, plan) => {
      if (bundlePacker.planHasCapacity(plan)) {
        acc.underweightBundlePlans.push(plan);
      } else {
        acc.bundlePlans.push(plan);
      }
      return acc;
    },
    {
      underweightBundlePlans: new Array<PackerBundlePlan>(),
      bundlePlans: new Array<PackerBundlePlan>(),
    }
  );
}

export function dataItemIsOverdue(
  dataItem: { uploadedDate: Timestamp },
  overdueThresholdMs: number
): boolean {
  const msSinceDataItemUploaded =
    new Date().getTime() - new Date(dataItem.uploadedDate).getTime();
  return msSinceDataItemUploaded >= overdueThresholdMs;
}

export function dataItemIsOverdueFilter(
  overdueThresholdMs: number
): (dataItem: { uploadedDate: Timestamp }) => boolean {
  return (dataItem: { uploadedDate: Timestamp }) =>
    dataItemIsOverdue(dataItem, overdueThresholdMs);
}
