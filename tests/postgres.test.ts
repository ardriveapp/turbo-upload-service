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

import { tableNames } from "../src/arch/db/dbConstants";
import {
  failedBundleDbResultToFailedBundleMap,
  permanentBundleDbResultToPermanentBundleMap,
  plannedDataItemDbResultToPlannedDataItemMap,
} from "../src/arch/db/dbMaps";
import { PostgresDatabase } from "../src/arch/db/postgres";
import {
  BundlePlanDBResult,
  FailedBundleDBResult,
  NewBundleDBResult,
  NewDataItemDBResult,
  PermanentBundleDBResult,
  PlannedDataItemDBResult,
  PostedBundleDBResult,
  SeededBundleDBResult,
} from "../src/types/dbTypes";
import { DbTestHelper } from "./helpers/dbTestHelpers";
import {
  failedBundleExpectations,
  newBundleDbResultExpectations,
  permanentBundleExpectations,
  plannedDataItemExpectations,
  postedBundleDbResultExpectations,
} from "./helpers/expectations";
import {
  stubBlockHeight,
  stubByteCount,
  stubDataItemBufferSignature,
  stubDates,
  stubNewDataItem,
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
  stubUsdToArRate,
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
      payloadDataStart: 1500,
      failedBundles: [],
      signatureType: 1,
      uploadedDate: stubDates.earliestDate,
      payloadContentType: "application/json",
      premiumFeatureType: "default",
      signature: stubDataItemBufferSignature,
      deadlineHeight: 200,
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
      content_type,
    } = newDataItems[0];

    expect(assessed_winston_price).to.equal(stubWinstonPrice.toString());
    expect(owner_public_address).to.equal(stubOwnerAddress);
    expect(byte_count).to.equal(stubByteCount.toString());
    expect(data_item_id).to.equal(stubTxId13);
    expect(uploaded_date).to.exist;
    expect(content_type).to.equal("application/json");

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

    // Items come back in any order when we batch insert
    const plannedDataItem4 = plannedDataItems.filter(
      (p) => p.data_item_id === stubTxId4
    )[0];
    const plannedDataItem5 = plannedDataItems.filter(
      (p) => p.data_item_id === stubTxId5
    )[0];

    plannedDataItemExpectations(
      plannedDataItemDbResultToPlannedDataItemMap(plannedDataItem4),
      { expectedDataItemId: stubTxId4, expectedPlanId: stubPlanId }
    );
    plannedDataItemExpectations(
      plannedDataItemDbResultToPlannedDataItemMap(plannedDataItem5),
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

    await dbTestHelper.insertStubNewBundle({
      planId: stubPlanId,
      bundleId,
    });

    await db.insertPostedBundle({
      bundleId,
      usdToArRate: stubUsdToArRate,
    });

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
    const usdToArRate = stubUsdToArRate;

    await dbTestHelper.insertStubPostedBundle({
      planId,
      bundleId,
      usdToArRate,
    });

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
      { bundleId: stubTxId1, planId: stubPlanId, usdToArRate: stubUsdToArRate },
      {
        bundleId: stubTxId2,
        planId: stubPlanId2,
        usdToArRate: stubUsdToArRate,
      },
      {
        bundleId: stubTxId3,
        planId: stubPlanId3,
        usdToArRate: stubUsdToArRate,
      },
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
    const bundleId = "Unique updateBundleAsPermanent Bundle ID";
    const planId = "Unique updateBundleAsPermanent Plan ID";
    const indexedOnGQL = true;
    const blockHeight = stubBlockHeight;
    const usdToArRate = stubUsdToArRate;

    await dbTestHelper.insertStubSeededBundle({
      planId,
      bundleId,
      usdToArRate,
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
        expectedPlanId: planId,
      }
    );
    expect(permanentBundleDbResult[0].block_height).to.equal(
      blockHeight.toString()
    );
  });

  it("updateSeededBundleToDropped method deletes existing seeded_bundle, inserts failed_bundle, deletes each planned_data_item, and inserts them as new_data_items", async () => {
    const bundleId = "Stub bundle ID updateSeededBundleToDropped";
    const planId = "Stub plan ID updateSeededBundleToDropped";
    const dataItemIds = [stubTxId5, stubTxId13, stubTxId4];
    const usdToArRate = stubUsdToArRate;

    await dbTestHelper.insertStubSeededBundle({
      planId,
      bundleId,
      dataItemIds,
      usdToArRate,
      failedBundles: ["testOne", "testTwo"],
    });
    await db.updateSeededBundleToDropped(planId, bundleId);

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
    await Promise.all(
      dataItemIds.map(async (data_item_id) => {
        const dbResult = await db["writer"]<NewDataItemDBResult>(
          tableNames.newDataItem
        ).where({
          data_item_id,
        });
        expect(dbResult.length).to.equal(1);
        expect(dbResult[0].data_item_id).to.equal(data_item_id);
        expect(dbResult[0].failed_bundles).to.equal(
          `testOne,testTwo,${bundleId}`
        );
      })
    );

    await dbTestHelper.cleanUpSeededBundleInDb({
      bundleId,
      dataItemIds,
      bundleTable: "failed_bundle",
    });
  });

  describe("updateNewBundleToFailedToPost method", () => {
    it("updates the expected new bundle", async () => {
      const bundleId = "updateNewBundleToFailedToPost Bundle ID";
      const planId = "updateNewBundleToFailedToPost Plan ID";
      const dataItemIds = [
        "testOne  updateNewBundleToFailedToPost",
        "testTwo  updateNewBundleToFailedToPost",
        "testThree  updateNewBundleToFailedToPost",
      ];

      await dbTestHelper.insertStubNewBundle({
        planId,
        bundleId,
        dataItemIds,
      });

      await db.updateNewBundleToFailedToPost(planId, bundleId);

      // Get and cleanup data items immediately from new data item table to avoid test race conditions
      const dataItems = await db["writer"]<NewDataItemDBResult>(
        tableNames.newDataItem
      )
        .whereIn("data_item_id", dataItemIds)
        .del()
        .returning("*");
      expect(dataItems.length).to.equal(3);
      dataItems.forEach((dataItem) => {
        expect(dataItem.failed_bundles).to.equal(bundleId);
      });

      // New bundle is removed
      expect(
        (
          await db["writer"](tableNames.newBundle).where({
            bundle_id: bundleId,
          })
        ).length
      ).to.equal(0);

      // Failed bundle exists as expected
      const failedBundleDbResult = await db["writer"]<FailedBundleDBResult>(
        tableNames.failedBundle
      ).where({ bundle_id: bundleId });
      expect(failedBundleDbResult.length).to.equal(1);
      expect(failedBundleDbResult[0].bundle_id).to.equal(bundleId);
      expect(failedBundleDbResult[0].plan_id).to.equal(planId);
      expect(failedBundleDbResult[0].failed_date).to.exist;

      await dbTestHelper.cleanUpEntityInDb(tableNames.failedBundle, bundleId);
    });
  });

  describe("getDataItemInfo method returns the expected info for a", () => {
    it("newDataItem", async () => {
      const dataItemId = stubTxId6;

      await dbTestHelper.insertStubNewDataItem({ dataItemId });

      const { status, assessedWinstonPrice, bundleId } =
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
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

      await dbTestHelper.insertStubPlannedDataItem({
        dataItemId,
        planId,
        signature: stubDataItemBufferSignature, // may not work depending on invariants checked
      });

      const { status, assessedWinstonPrice, bundleId } =
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
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
        // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
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

  describe("updateDataItemBatchAsPermanent method", () => {
    it("updates the expected data items", async () => {
      const dataItemIds = [
        "permanent data item test 1",
        "permanent data item test 2",
        "permanent data item test 3",
      ];
      const blockHeight = stubBlockHeight;
      const bundleId = stubTxId13;

      await Promise.all(
        dataItemIds.map((dataItemId) =>
          dbTestHelper.insertStubPlannedDataItem({
            dataItemId,
            planId: "Unique plan ID",
          })
        )
      );

      await db.updateDataItemsAsPermanent({
        dataItemIds,
        blockHeight,
        bundleId,
      });

      // Planned data items are removed
      const plannedDbResult = await db["writer"](
        tableNames.plannedDataItem
      ).whereIn("data_item_id", dataItemIds);
      expect(plannedDbResult.length).to.equal(0);

      // Permanent data items are inserted as expected
      const permanentDbResult = await db["writer"](
        tableNames.permanentDataItem
      ).whereIn("data_item_id", dataItemIds);
      expect(permanentDbResult.length).to.equal(3);

      await Promise.all(
        dataItemIds.map((dataItemId) =>
          dbTestHelper.cleanUpEntityInDb(
            tableNames.permanentDataItem,
            dataItemId
          )
        )
      );
    });
  });

  describe("updateDataItemBatchToBeRePacked method", () => {
    it("updates the expected data items", async () => {
      const dataItemIds = [
        "re pack data item test 1",
        "re pack data item test 2",
        "re pack data item test 3",
      ];
      const bundleId = stubTxId13;

      const previouslyFailedBundle = "already has a failed bundle";
      await Promise.all(
        dataItemIds.map((dataItemId) =>
          dbTestHelper.insertStubPlannedDataItem({
            dataItemId,
            planId: "A great Unique plan ID",
            failedBundles: [previouslyFailedBundle],
          })
        )
      );

      await db.updateDataItemsToBeRePacked(dataItemIds, bundleId);

      // Planned data items are removed
      const plannedDbResult = await db["writer"](
        tableNames.plannedDataItem
      ).whereIn("data_item_id", dataItemIds);
      expect(plannedDbResult.length).to.equal(0);

      // New data items are inserted as expected
      const newDbResult = await db["writer"](tableNames.newDataItem).whereIn(
        "data_item_id",
        dataItemIds
      );
      expect(newDbResult.length).to.equal(3);
      expect(newDbResult[0].failed_bundles).to.equal(
        previouslyFailedBundle + "," + bundleId
      );

      await Promise.all(
        dataItemIds.map((dataItemId) =>
          dbTestHelper.cleanUpEntityInDb(tableNames.newDataItem, dataItemId)
        )
      );
    });
  });

  describe("insertNewDataItemBatch method", () => {
    it("inserts a batch of new data items", async () => {
      const testIds = ["unique id one", "unique id two", "unique id three"];
      const dataItemBatch = testIds.map((dataItemId) =>
        stubNewDataItem(dataItemId)
      );

      await db.insertNewDataItemBatch(dataItemBatch);

      const newDataItems =
        await dbTestHelper.getAndDeleteNewDataItemDbResultsByIds(testIds);
      expect(newDataItems.length).to.equal(3);

      newDataItems.forEach((newDataItem) => {
        expect(newDataItem.data_item_id).to.be.oneOf(testIds);
      });
    });

    it("gracefully skips inserting data items that already exist in the database", async () => {
      const testIds = [
        "unique skip insert id one",
        "unique skip insert id two",
      ];
      const dataItemBatch = testIds.map((dataItemId) =>
        stubNewDataItem(dataItemId)
      );

      // insert the first data item into the planned data item table
      await dbTestHelper.insertStubPlannedDataItem({
        dataItemId: testIds[0],
        planId: "unique stub for this test",
      });

      // Run batch insert with both data items
      await db.insertNewDataItemBatch(dataItemBatch);

      const newDataItems =
        await dbTestHelper.getAndDeleteNewDataItemDbResultsByIds(testIds);

      // Expect only the second data item to have been inserted to new data item table
      expect(newDataItems.length).to.equal(1);
      expect(newDataItems[0].data_item_id).to.equal(testIds[1]);
    });
  });
});
