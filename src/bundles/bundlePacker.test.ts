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
import { expect } from "chai";

import {
  stubTxId1,
  stubTxId2,
  stubTxId3,
  stubTxId4,
  stubTxId5,
  stubTxId6,
  stubTxId7,
  stubTxId8,
  stubTxId9,
  stubTxId10,
} from "../../tests/stubs";
import { BundlePacker } from "./bundlePacker";

describe("BundlePacker class", () => {
  const bundlePacker = new BundlePacker({
    maxBundleSize: 100,
    maxDataItemSize: 100,
    maxDataItemLimit: 3,
  });

  describe("packDataItemsIntoBundlePlans method returns the expected BundlePlans", () => {
    it("when provided a single data item within the bundle max byte count limit", () => {
      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans([
        { byteCount: 10, dataItemId: stubTxId1 },
      ]);

      expect(bundlePlans).to.deep.equal([
        { dataItemIds: [stubTxId1], totalByteCount: 10 },
      ]);
    });

    it("when provided a single data item that exceeds the bundle max byte count limit", () => {
      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans([
        { byteCount: 1000, dataItemId: stubTxId1 },
      ]);

      // We gracefully ignore this oversized dataitem with std.err logging
      expect(bundlePlans).to.deep.equal([]);
    });

    it("when provided multiple data items within the bundle max byte count limit and data item limit", () => {
      const dataItemIds = [stubTxId1, stubTxId2, stubTxId3];

      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans(
        dataItemIds.map((dataItemId) => {
          return { byteCount: 10, dataItemId };
        })
      );

      expect(bundlePlans).to.deep.equal([{ dataItemIds, totalByteCount: 30 }]);
    });

    it("when provided multiple data items within that when combined will exceed the bundle max byte count limit", () => {
      const dataItemIds = [stubTxId1, stubTxId2, stubTxId3];

      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans(
        dataItemIds.map((dataItemId) => {
          return { byteCount: 51, dataItemId };
        })
      );

      expect(bundlePlans).to.deep.equal([
        { dataItemIds: [stubTxId1], totalByteCount: 51 },
        { dataItemIds: [stubTxId2], totalByteCount: 51 },
        { dataItemIds: [stubTxId3], totalByteCount: 51 },
      ]);
    });

    it("when provided multiple data items, and those data items are always packed into the lowest index bundle", () => {
      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans([
        { dataItemId: stubTxId1, byteCount: 90 },
        { dataItemId: stubTxId2, byteCount: 90 },
        { dataItemId: stubTxId3, byteCount: 10 },
      ]);

      expect(bundlePlans).to.deep.equal([
        // Tx id 3 is packed into lowest index because it fits within the 100 byte max
        { dataItemIds: [stubTxId1, stubTxId3], totalByteCount: 100 },
        { dataItemIds: [stubTxId2], totalByteCount: 90 },
      ]);
    });

    it("when provided multiple data items within the bundle max byte count limit but they exceed the data item limit", () => {
      const dataItemIds = [stubTxId1, stubTxId2, stubTxId3, stubTxId4];

      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans(
        dataItemIds.map((dataItemId) => {
          return { byteCount: 10, dataItemId };
        })
      );

      expect(bundlePlans).to.deep.equal([
        { dataItemIds: [stubTxId1, stubTxId2, stubTxId3], totalByteCount: 30 },
        { dataItemIds: [stubTxId4], totalByteCount: 10 },
      ]);
    });

    it("when provided many data items within the bundle max byte count limit but they exceed the data item limit", () => {
      const dataItemIds = [
        stubTxId1,
        stubTxId2,
        stubTxId3,
        stubTxId4,
        stubTxId5,
        stubTxId6,
        stubTxId7,
        stubTxId8,
        stubTxId9,
        stubTxId10,
      ];

      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans(
        dataItemIds.map((dataItemId) => {
          return { byteCount: 10, dataItemId };
        })
      );

      expect(bundlePlans).to.deep.equal([
        { dataItemIds: [stubTxId1, stubTxId2, stubTxId3], totalByteCount: 30 },
        { dataItemIds: [stubTxId4, stubTxId5, stubTxId6], totalByteCount: 30 },
        { dataItemIds: [stubTxId7, stubTxId8, stubTxId9], totalByteCount: 30 },
        { dataItemIds: [stubTxId10], totalByteCount: 10 },
      ]);
    });
  });
});
