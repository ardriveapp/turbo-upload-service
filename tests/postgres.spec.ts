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

import { tableNames } from "../src/arch/db/dbConstants";
import {
  failedBundleDbResultToFailedBundleMap,
  newDataItemDbResultToNewDataItemMap,
  permanentBundleDbResultToPermanentBundleMap,
  permanentDataItemDbResultToPermanentDataItemMap,
  plannedDataItemDbResultToPlannedDataItemMap,
} from "../src/arch/db/dbMaps";
import { PostgresDatabase } from "../src/arch/db/postgres";
import {
  BundlePlanDBResult,
  FailedBundleDBResult,
  NewBundleDBResult,
  NewDataItemDBResult,
  PermanentBundleDBResult,
  PermanentDataItemDBResult,
  PlannedDataItemDBResult,
  PostedBundleDBResult,
  SeededBundleDBResult,
} from "../src/types/dbTypes";
import { DbTestHelper } from "./helpers/dbTestHelpers";
import {
  failedBundleExpectations,
  newBundleDbResultExpectations,
  newDataItemExpectations,
  permanentBundleExpectations,
  permanentDataItemExpectations,
  plannedDataItemExpectations,
  postedBundleDbResultExpectations,
} from "./helpers/expectations";
import {
  stubBlockHeight,
  stubByteCount,
  stubDates,
  stubOwnerAddress,
  stubPlanId,
  stubPlanId2,
  stubPlanId3,
  stubTxId1,
  stubTxId2,
  stubTxId3,
  stubTxId4,
  stubTxId5,
  stubTxId6,
  stubTxId7,
  stubTxId8,
  stubTxId9,
  stubTxId13,
  stubTxId14,
  stubTxId15,
  stubTxId16,
  stubWinstonPrice,
} from "./stubs";

describe("PostgresDatabase class", () => {
  const db = new PostgresDatabase();
  const dbTestHelper = new DbTestHelper(db);

  it("insertNewDataItem method adds a new_data_item to the database", async () => {
    await db.insertNewDataItem({
      dataItemId: stubTxId13,
      ownerPublicAddress: stubOwnerAddress,
      byteCount: stubByteCount,
      assessedWinstonPrice: stubWinstonPrice,
      dataStart: 1500,
      failedBundles: [],
      signatureType: 1,
    });

    const newDataItems = await db["writer"]<NewDataItemDBResult>(
      "new_data_item"
    ).where({ data_item_id: stubTxId13 });
    expect(newDataItems.length).to.equal(1);

    const {
      assessed_winston_price,
      owner_public_address,
      byte_count,
      data_item_id,
      uploaded_date,
    } = newDataItems[0];

    expect(assessed_winston_price).to.equal(stubWinstonPrice.toString());
    expect(owner_public_address).to.equal(stubOwnerAddress);
    expect(byte_count).to.equal(stubByteCount.toString());
    expect(data_item_id).to.equal(stubTxId13);
    expect(uploaded_date).to.exist;

    await dbTestHelper.cleanUpEntityInDb(tableNames.newDataItem, stubTxId13);
  });

  it("getNewDataItems method gets all new_data_item in the database sorted by uploaded_date", async () => {
    await Promise.all([
      dbTestHelper.insertStubNewDataItem({
        dataItemId: stubTxId14,
        uploadedDate: stubDates.middleDate,
      }),
      dbTestHelper.insertStubNewDataItem({
        dataItemId: stubTxId15,
        uploadedDate: stubDates.latestDate,
      }),
      dbTestHelper.insertStubNewDataItem({
        dataItemId: stubTxId16,
        uploadedDate: stubDates.earliestDate,
      }),
    ]);

    const txIds = [stubTxId14, stubTxId15, stubTxId16];
    const newDataItems = await db.getNewDataItems();

    const [dataItem1, dataItem2, dataItem3] = newDataItems.filter((d) =>
      txIds.includes(d.dataItemId)
    );

    // We expect these items returns in this order, earliest to latest
    expect(dataItem1.dataItemId).to.equal(stubTxId16);
    expect(dataItem2.dataItemId).to.equal(stubTxId14);
    expect(dataItem3.dataItemId).to.equal(stubTxId15);

    // Cleanup newDataItems
    await Promise.all(
      txIds.map((id) =>
        dbTestHelper.cleanUpEntityInDb(tableNames.newDataItem, id)
      )
    );
  });

  it("insertBundlePlan method adds a bundle_plan, deletes specified new_data_items, and inserts planned_data_items ", async () => {
    await Promise.all([
      // Setup 2 NewDataItem that BundlePlan insert will depend on
      dbTestHelper.insertStubNewDataItem({ dataItemId: stubTxId4 }),
      dbTestHelper.insertStubNewDataItem({ dataItemId: stubTxId5 }),
    ]);

    await db.insertBundlePlan(stubPlanId, [stubTxId4, stubTxId5]);

    const { plan_id, planned_date } = (
      await db["writer"]<BundlePlanDBResult>(tableNames.bundlePlan)
    )[0];
    expect(plan_id).to.equal(stubPlanId);
    expect(planned_date).to.exist;

    // We expect the new data items to have been deleted
    expect(
      (
        await db["writer"]<NewDataItemDBResult>(tableNames.newDataItem).whereIn(
          "data_item_id",
          [stubTxId4, stubTxId5]
        )
      ).length
    ).to.equal(0);

    // We expect planned data items to have been inserted
    const plannedDataItems = await db["writer"]<PlannedDataItemDBResult>(
      tableNames.plannedDataItem
    ).whereIn("data_item_id", [stubTxId4, stubTxId5]);
    expect(plannedDataItems.length).to.equal(2);

    plannedDataItemExpectations(
      plannedDataItemDbResultToPlannedDataItemMap(plannedDataItems[0]),
      { expectedDataItemId: stubTxId4, expectedPlanId: stubPlanId }
    );
    plannedDataItemExpectations(
      plannedDataItemDbResultToPlannedDataItemMap(plannedDataItems[1]),
      { expectedDataItemId: stubTxId5, expectedPlanId: stubPlanId }
    );

    await dbTestHelper.cleanUpBundlePlanInDb({
      planId: stubPlanId,
      dataItemIds: [stubTxId4, stubTxId5],
    });
  });

  it("insertNewBundle method deletes existing bundle_plan and inserts new_bundle as expected", async () => {
    const bundleId = stubTxId13;
    const planId = stubPlanId;

    await dbTestHelper.insertStubBundlePlan({
      planId,
      dataItemIds: [],
    });

    await db.insertNewBundle({
      bundleId,
      planId,
      reward: stubWinstonPrice,
      headerByteCount: stubByteCount,
      payloadByteCount: stubByteCount,
      transactionByteCount: stubByteCount,
    });

    const newBundleDbResult = await db["writer"]<NewBundleDBResult>(
      tableNames.newBundle
    ).where({ bundle_id: bundleId });
    expect(newBundleDbResult.length).to.equal(1);
    newBundleDbResultExpectations(newBundleDbResult[0], {
      expectedBundleId: bundleId,
      expectedPlanId: planId,
    });

    await Promise.all([
      dbTestHelper.cleanUpBundlePlanInDb({ planId, dataItemIds: [] }),
      dbTestHelper.cleanUpEntityInDb(tableNames.newBundle, bundleId),
    ]);
  });

  it("insertPostedBundle method deletes existing new_bundle and inserts posted_bundle and seed_result as expected", async () => {
    const bundleId = stubTxId13;

    await dbTestHelper.insertStubNewBundle({ planId: stubPlanId, bundleId });

    await db.insertPostedBundle(bundleId);

    // New bundle is removed
    expect(
      (await db["writer"](tableNames.newBundle).where({ bundle_id: bundleId }))
        .length
    ).to.equal(0);

    // Posted bundle exists as expected
    const postedBundleDbResult = await db["writer"]<PostedBundleDBResult>(
      tableNames.postedBundle
    ).where({ bundle_id: bundleId });
    expect(postedBundleDbResult.length).to.equal(1);
    postedBundleDbResultExpectations(postedBundleDbResult[0], {
      expectedBundleId: bundleId,
      expectedPlanId: stubPlanId,
    });

    await dbTestHelper.cleanUpEntityInDb(tableNames.postedBundle, bundleId);
  });

  it("insertSeededBundle method deletes existing posted_bundle and inserts seeded_bundle", async () => {
    const bundleId = stubTxId13;
    const planId = stubPlanId;

    await dbTestHelper.insertStubPostedBundle({ planId, bundleId });

    await db.insertSeededBundle(bundleId);

    // New bundle is removed
    expect(
      (
        await db["writer"](tableNames.postedBundle).where({
          bundle_id: bundleId,
        })
      ).length
    ).to.equal(0);

    // Seeded bundle exists as expected
    const seededBundleDbResult = await db["writer"]<SeededBundleDBResult>(
      tableNames.seededBundle
    ).where({ bundle_id: bundleId });
    expect(seededBundleDbResult.length).to.equal(1);
    postedBundleDbResultExpectations(seededBundleDbResult[0], {
      expectedBundleId: bundleId,
      expectedPlanId: stubPlanId,
    });

    await dbTestHelper.cleanUpEntityInDb(tableNames.seededBundle, bundleId);
  });

  it("getSeededBundles method gets the expected seed_results", async () => {
    const stubSeededBundles = [
      { bundleId: stubTxId1, planId: stubPlanId },
      { bundleId: stubTxId2, planId: stubPlanId2 },
      { bundleId: stubTxId3, planId: stubPlanId3 },
    ];

    await Promise.all(
      stubSeededBundles.map((seededBundle) =>
        dbTestHelper.insertStubSeededBundle(seededBundle)
      )
    );

    const seededBundles = await db.getSeededBundles();
    expect(seededBundles.length).to.equal(3);
    seededBundles.forEach(({ bundleId }) =>
      expect(stubSeededBundles.map((s) => s.bundleId)).to.include(bundleId)
    );

    await Promise.all(
      stubSeededBundles.map(({ bundleId }) =>
        dbTestHelper.cleanUpSeededBundleInDb({
          bundleId,
          bundleTable: "seeded_bundle",
        })
      )
    );
  });

  it("updateBundleAsPermanent method  deletes existing seeded_bundle, inserts permanent_bundle, deletes each planned_data_item, and inserts them as permanent_data_items", async () => {
    const bundleId = stubTxId13;
    const planId = stubPlanId;
    const dataItemIds = [stubTxId1, stubTxId2, stubTxId3];
    const indexedOnGQL = true;
    const blockHeight = stubBlockHeight;

    await dbTestHelper.insertStubSeededBundle({
      planId,
      bundleId,
      dataItemIds,
    });
    await db.updateBundleAsPermanent(planId, blockHeight, indexedOnGQL);

    // Seeded bundle is removed
    expect(
      (
        await db["writer"](tableNames.seededBundle).where({
          bundle_id: bundleId,
        })
      ).length
    ).to.equal(0);

    // Permanent bundle exists as expected
    const permanentBundleDbResult = await db["writer"]<PermanentBundleDBResult>(
      tableNames.permanentBundle
    ).where({ bundle_id: bundleId });
    expect(permanentBundleDbResult.length).to.equal(1);
    permanentBundleExpectations(
      permanentBundleDbResultToPermanentBundleMap(permanentBundleDbResult[0]),
      {
        expectedBundleId: bundleId,
        expectedPlanId: stubPlanId,
      }
    );
    expect(permanentBundleDbResult[0].block_height).to.equal(
      blockHeight.toString()
    );

    // Planned data items are removed
    await Promise.all([
      dataItemIds.map(async (data_item_id) => {
        expect(
          (
            await db["writer"](tableNames.plannedDataItem).where({
              data_item_id,
            })
          ).length
        ).to.equal(0);
      }),
    ]);

    // Permanent data items are inserted as expected
    await Promise.all([
      dataItemIds.map(async (data_item_id) => {
        permanentDataItemExpectations(
          permanentDataItemDbResultToPermanentDataItemMap(
            (
              await db["writer"]<PermanentDataItemDBResult>(
                tableNames.permanentDataItem
              ).where({
                data_item_id,
              })
            )[0]
          ),
          { expectedDataItemId: data_item_id, expectedPlanId: planId }
        );
      }),
    ]);

    await dbTestHelper.cleanUpSeededBundleInDb({
      bundleId,
      dataItemIds,
      bundleTable: "permanent_bundle",
    });
  });

  it("updateBundleAsDropped method deletes existing seeded_bundle, inserts failed_bundle, deletes each planned_data_item, and inserts them as new_data_items", async () => {
    const bundleId = stubTxId4;
    const planId = stubPlanId2;
    const dataItemIds = [stubTxId5, stubTxId13, stubTxId4];

    await dbTestHelper.insertStubSeededBundle({
      planId,
      bundleId,
      dataItemIds,
    });
    await db.updateBundleAsDropped(planId);

    // Seeded bundle is removed
    expect(
      (
        await db["writer"](tableNames.seededBundle).where({
          bundle_id: bundleId,
        })
      ).length
    ).to.equal(0);

    // Failed bundle exists as expected
    const failedBundleDbResult = await db["writer"]<FailedBundleDBResult>(
      tableNames.failedBundle
    ).where({ bundle_id: bundleId });
    expect(failedBundleDbResult.length).to.equal(1);
    failedBundleExpectations(
      failedBundleDbResultToFailedBundleMap(failedBundleDbResult[0]),
      {
        expectedBundleId: bundleId,
        expectedPlanId: planId,
      }
    );

    // Planned data items are removed
    await Promise.all([
      dataItemIds.map(async (data_item_id) => {
        expect(
          (
            await db["writer"](tableNames.plannedDataItem).where({
              data_item_id,
            })
          ).length
        ).to.equal(0);
      }),
    ]);

    // New data items are inserted as expected
    await Promise.all([
      dataItemIds.map(async (data_item_id) => {
        newDataItemExpectations(
          newDataItemDbResultToNewDataItemMap(
            (
              await db["writer"]<NewDataItemDBResult>(
                tableNames.newDataItem
              ).where({
                data_item_id,
              })
            )[0]
          ),
          { expectedDataItemId: data_item_id }
        );
      }),
    ]);

    await dbTestHelper.cleanUpSeededBundleInDb({
      bundleId,
      dataItemIds,
      bundleTable: "failed_bundle",
    });
  });

  describe("getDataItemInfo method returns the expected info for a", () => {
    it("newDataItem", async () => {
      const dataItemId = stubTxId6;

      await dbTestHelper.insertStubNewDataItem({ dataItemId });

      const { status, assessedWinstonPrice, bundleId } =
        (await db.getDataItemInfo(dataItemId))!;

      expect(status).to.equal("new");
      expect(assessedWinstonPrice.toString()).to.equal(
        stubWinstonPrice.toString()
      );
      expect(bundleId).to.be.undefined;

      await dbTestHelper.cleanUpEntityInDb(tableNames.newDataItem, dataItemId);
    });

    it("plannedDataItem", async () => {
      const dataItemId = stubTxId8;
      const planId = stubPlanId;

      await dbTestHelper.insertStubPlannedDataItem({ dataItemId, planId });

      const { status, assessedWinstonPrice, bundleId } =
        (await db.getDataItemInfo(dataItemId))!;

      expect(status).to.equal("pending");
      expect(assessedWinstonPrice.toString()).to.equal(
        stubWinstonPrice.toString()
      );
      expect(bundleId).to.be.undefined;

      await dbTestHelper.cleanUpEntityInDb(
        tableNames.plannedDataItem,
        dataItemId
      );
    });

    it("permanentDataItem", async () => {
      const dataItemId = stubTxId7;
      const planId = stubPlanId2;
      const bundleId = stubTxId13;

      await dbTestHelper.insertStubPermanentDataItem({
        dataItemId,
        planId,
        bundleId,
      });

      const {
        status,
        assessedWinstonPrice,
        bundleId: bundleIdInDb,
      } = (await db.getDataItemInfo(dataItemId))!;

      expect(status).to.equal("permanent");
      expect(assessedWinstonPrice.toString()).to.equal(
        stubWinstonPrice.toString()
      );
      expect(bundleIdInDb).to.equal(bundleId);

      await dbTestHelper.cleanUpEntityInDb(
        tableNames.permanentDataItem,
        dataItemId
      );
    });

    it("non existent data item", async () => {
      expect(await db.getDataItemInfo(stubTxId9)).to.be.undefined;
    });
  });
});
