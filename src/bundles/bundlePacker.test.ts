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
import { defaultOverdueThresholdMs } from "../constants";
import { BundlePacker, PackerBundlePlan } from "./bundlePacker";

describe("BundlePacker class", () => {
  const maxTotalDataItemsByteCount = 100;
  const maxSingleDataItemByteCount = 100;
  const maxDataItemsCount = 3;
  const bundlePacker = new BundlePacker({
    maxTotalDataItemsByteCount,
    maxSingleDataItemByteCount,
    maxDataItemsCount,
  });
  const overdueDataItemDateISOStr = () =>
    new Date(new Date().getTime() - defaultOverdueThresholdMs).toISOString();

  describe("packDataItemsIntoBundlePlans method returns the expected BundlePlans", () => {
    it("when provided a single data item within the bundle max byte count limit", () => {
      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans([
        {
          byteCount: 10,
          dataItemId: stubTxId1,
          uploadedDate: new Date().toISOString(),
        },
      ]);

      expect(bundlePlans).to.deep.equal([
        {
          dataItemIds: [stubTxId1],
          totalByteCount: 10,
          containsOverdueDataItems: false,
          dataItemSizes: {
            [stubTxId1]: 10,
          },
        },
      ]);
    });

    it("when provided a single data item that exceeds the bundle max byte count limit", () => {
      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans([
        {
          byteCount: 1000,
          dataItemId: stubTxId1,
          uploadedDate: overdueDataItemDateISOStr(),
        },
      ]);

      // We gracefully ignore this oversized dataitem with std.err logging
      expect(bundlePlans).to.deep.equal([]);
    });

    it("when provided multiple data items within the bundle max byte count limit and data item limit", () => {
      const dataItemIds = [stubTxId1, stubTxId2, stubTxId3];

      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans(
        dataItemIds.map((dataItemId) => {
          return {
            byteCount: 10,
            dataItemId,
            uploadedDate: overdueDataItemDateISOStr(),
          };
        })
      );

      expect(bundlePlans).to.deep.equal([
        {
          dataItemIds,
          totalByteCount: 30,
          containsOverdueDataItems: true,
          dataItemSizes: {
            [stubTxId1]: 10,
            [stubTxId2]: 10,
            [stubTxId3]: 10,
          },
        },
      ]);
    });

    it("when provided multiple data items within that when combined will exceed the bundle max byte count limit", () => {
      const dataItemIds = [stubTxId1, stubTxId2, stubTxId3];

      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans(
        dataItemIds.map((dataItemId) => {
          return {
            byteCount: 51,
            dataItemId,
            uploadedDate: overdueDataItemDateISOStr(),
          };
        })
      );

      expect(bundlePlans).to.deep.equal([
        {
          dataItemIds: [stubTxId1],
          totalByteCount: 51,
          containsOverdueDataItems: true,
          dataItemSizes: {
            [stubTxId1]: 51,
          },
        },
        {
          dataItemIds: [stubTxId2],
          totalByteCount: 51,
          containsOverdueDataItems: true,
          dataItemSizes: {
            [stubTxId2]: 51,
          },
        },
        {
          dataItemIds: [stubTxId3],
          totalByteCount: 51,
          containsOverdueDataItems: true,
          dataItemSizes: {
            [stubTxId3]: 51,
          },
        },
      ]);
    });

    it("when provided multiple data items, and those data items are always packed into the lowest index bundle", () => {
      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans([
        {
          dataItemId: stubTxId1,
          byteCount: 90,
          uploadedDate: overdueDataItemDateISOStr(),
        },
        {
          dataItemId: stubTxId2,
          byteCount: 90,
          uploadedDate: new Date().toISOString(),
        },
        {
          dataItemId: stubTxId3,
          byteCount: 10,
          uploadedDate: new Date().toISOString(),
        },
      ]);

      expect(bundlePlans).to.deep.equal([
        // Tx id 3 is packed into lowest index because it fits within the 100 byte max
        {
          dataItemIds: [stubTxId3, stubTxId1],
          totalByteCount: 100,
          containsOverdueDataItems: true,
          dataItemSizes: {
            [stubTxId3]: 10,
            [stubTxId1]: 90,
          },
        },
        {
          dataItemIds: [stubTxId2],
          totalByteCount: 90,
          containsOverdueDataItems: false,
          dataItemSizes: {
            [stubTxId2]: 90,
          },
        },
      ]);
    });

    it("will pack data items from smallest byte count to largest byte count", () => {
      const bundlePacker = new BundlePacker({
        maxTotalDataItemsByteCount: 1000,
        maxSingleDataItemByteCount: 1000,
        maxDataItemsCount: 5,
      });

      const dataItems = [
        {
          dataItemId: stubTxId1,
          byteCount: 5,
          uploadedDate: new Date().toISOString(),
        },
        {
          dataItemId: stubTxId2,
          byteCount: 25,
          uploadedDate: new Date().toISOString(),
        },
        {
          dataItemId: stubTxId3,
          byteCount: 1,
          uploadedDate: new Date().toISOString(),
        },
        {
          dataItemId: stubTxId4,
          byteCount: 10,
          uploadedDate: new Date().toISOString(),
        },
      ];

      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans(dataItems);

      expect(bundlePlans).to.deep.equal([
        {
          dataItemIds: [stubTxId3, stubTxId1, stubTxId4, stubTxId2],
          containsOverdueDataItems: false,
          totalByteCount: 41,
          dataItemSizes: {
            [stubTxId3]: 1,
            [stubTxId1]: 5,
            [stubTxId4]: 10,
            [stubTxId2]: 25,
          },
        },
      ]);
    });

    it("when provided multiple data items within the bundle max byte count limit but they exceed the data item limit", () => {
      const dataItemIds = [stubTxId1, stubTxId2, stubTxId3, stubTxId4];

      const bundlePlans = bundlePacker.packDataItemsIntoBundlePlans(
        dataItemIds.map((dataItemId) => {
          return {
            byteCount: 10,
            dataItemId,
            uploadedDate: new Date().toISOString(),
          };
        })
      );

      expect(bundlePlans).to.deep.equal([
        {
          dataItemIds: [stubTxId1, stubTxId2, stubTxId3],
          totalByteCount: 30,
          containsOverdueDataItems: false,
          dataItemSizes: {
            [stubTxId1]: 10,
            [stubTxId2]: 10,
            [stubTxId3]: 10,
          },
        },
        {
          dataItemIds: [stubTxId4],
          totalByteCount: 10,
          containsOverdueDataItems: false,
          dataItemSizes: {
            [stubTxId4]: 10,
          },
        },
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
          return {
            byteCount: 10,
            dataItemId,
            uploadedDate: new Date().toISOString(),
          };
        })
      );

      expect(bundlePlans).to.deep.equal([
        {
          dataItemIds: [stubTxId1, stubTxId2, stubTxId3],
          totalByteCount: 30,
          containsOverdueDataItems: false,
          dataItemSizes: {
            [stubTxId1]: 10,
            [stubTxId2]: 10,
            [stubTxId3]: 10,
          },
        },
        {
          dataItemIds: [stubTxId4, stubTxId5, stubTxId6],
          totalByteCount: 30,
          containsOverdueDataItems: false,
          dataItemSizes: {
            [stubTxId4]: 10,
            [stubTxId5]: 10,
            [stubTxId6]: 10,
          },
        },
        {
          dataItemIds: [stubTxId7, stubTxId8, stubTxId9],
          totalByteCount: 30,
          containsOverdueDataItems: false,
          dataItemSizes: {
            [stubTxId7]: 10,
            [stubTxId8]: 10,
            [stubTxId9]: 10,
          },
        },
        {
          dataItemIds: [stubTxId10],
          totalByteCount: 10,
          containsOverdueDataItems: false,
          dataItemSizes: {
            [stubTxId10]: 10,
          },
        },
      ]);
    });
  });

  describe("planHasCapacity function", () => {
    [
      [0, 0, true],
      [0, maxTotalDataItemsByteCount - 1, true], // invariant
      [0, maxTotalDataItemsByteCount, false], // invariant
      [1, maxTotalDataItemsByteCount - 1, true],
      [1, maxTotalDataItemsByteCount, false],
      [maxDataItemsCount - 1, 0, true], // invariant
      [maxDataItemsCount, 0, false], // invariant
      [maxDataItemsCount - 1, 1, true],
      [maxDataItemsCount, 1, false],
      [maxDataItemsCount, maxTotalDataItemsByteCount, false],
    ].forEach(([dataItemsCount, totalByteCount, expected]) => {
      it(`returns ${expected} when provided a plan with dataItemsCount ${dataItemsCount} and totalByteCount ${totalByteCount}`, () => {
        const bundlePlan: PackerBundlePlan = {
          dataItemIds: new Array(dataItemsCount as number).fill(""),
          totalByteCount: totalByteCount as number,
          dataItemSizes: {},
          containsOverdueDataItems: false,
        };
        expect(bundlePacker.planHasCapacity(bundlePlan)).to.equal(expected);
      });
    });
  });
});
