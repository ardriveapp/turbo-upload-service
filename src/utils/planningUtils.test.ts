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
import { expect } from "chai";

import { BundlePacker } from "../bundles/bundlePacker";
import { dataItemIsOverdue, factorBundlesByTargetSize } from "./planningUtils";

describe("factorBundlesByTargetSize function", () => {
  const testBundlePacker = new BundlePacker({});

  it("returns empty arrays when provided no bundle plans", () => {
    const { underweightBundlePlans, bundlePlans } = factorBundlesByTargetSize(
      [],
      testBundlePacker
    );

    expect(underweightBundlePlans).to.deep.equal([]);
    expect(bundlePlans).to.deep.equal([]);
  });

  it("returns properly factored arrays of plans", () => {
    const underweightPlan = {
      dataItemIds: new Array(testBundlePacker.maxDataItemsCount - 1).fill(
        "stub"
      ),
      totalByteCount: testBundlePacker.maxTotalDataItemsByteCount - 1,
      containsOverdueDataItems: false,
      dataItemSizes: {},
    };
    const maxedOutOnDataItemsPlan = {
      dataItemIds: new Array(testBundlePacker.maxDataItemsCount).fill("stub"),
      totalByteCount: testBundlePacker.maxTotalDataItemsByteCount - 1,
      containsOverdueDataItems: false,
      dataItemSizes: {},
    };
    const maxedOutOnTotalByteCountPlan = {
      dataItemIds: new Array(testBundlePacker.maxDataItemsCount - 1).fill(
        "stub"
      ),
      totalByteCount: testBundlePacker.maxTotalDataItemsByteCount,
      containsOverdueDataItems: false,
      dataItemSizes: {},
    };
    const justRightPlan = {
      dataItemIds: new Array(testBundlePacker.maxDataItemsCount).fill("stub"),
      totalByteCount: testBundlePacker.maxTotalDataItemsByteCount,
      containsOverdueDataItems: false,
      dataItemSizes: {},
    };

    const overPackedPlan = {
      dataItemIds: new Array(testBundlePacker.maxDataItemsCount + 1).fill(
        "stub"
      ),
      totalByteCount: testBundlePacker.maxTotalDataItemsByteCount,
      containsOverdueDataItems: false,
      dataItemSizes: {},
    };
    const oversizedPlan = {
      dataItemIds: new Array(testBundlePacker.maxDataItemsCount).fill("stub"),
      totalByteCount: testBundlePacker.maxTotalDataItemsByteCount + 1,
      containsOverdueDataItems: false,
      dataItemSizes: {},
    };
    const { underweightBundlePlans, bundlePlans } = factorBundlesByTargetSize(
      [
        underweightPlan,
        maxedOutOnDataItemsPlan,
        maxedOutOnTotalByteCountPlan,
        justRightPlan,
        overPackedPlan,
        oversizedPlan,
      ],
      testBundlePacker
    );

    expect(underweightBundlePlans).to.deep.equal([underweightPlan]);
    expect(bundlePlans).to.deep.equal([
      maxedOutOnDataItemsPlan,
      maxedOutOnTotalByteCountPlan,
      justRightPlan,
      overPackedPlan,
      oversizedPlan,
    ]);
  });
});

describe("dataItemIsOverdue function", () => {
  it("returns false when data item is not overdue", () => {
    const dataItem = {
      uploadedDate: new Date().toISOString(),
    };
    expect(dataItemIsOverdue(dataItem, 30_000)).to.equal(false);
  });

  it("returns true when data item is overdue", () => {
    const dataItem = {
      uploadedDate: new Date(new Date().getTime() - 30_000).toISOString(),
    };
    expect(dataItemIsOverdue(dataItem, 30_000)).to.equal(true);
  });
});
