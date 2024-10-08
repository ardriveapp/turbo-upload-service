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
import { retryLimitForFailedDataItems } from "../src/constants";
import {
  BundlePlanDBResult,
  FailedBundleDBResult,
  FailedDataItemDBResult,
  NewBundleDBResult,
  NewDataItemDBResult,
  PermanentBundleDBResult,
  PlannedDataItemDBResult,
  PostedBundleDBResult,
  SeededBundleDBResult,
} from "../src/types/dbTypes";
import { sleep } from "../src/utils/common";
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
  stubTxId14,
  stubTxId15,
  stubTxId16,
  stubUsdToArRate,
  stubWinstonPrice,
} from "./stubs";

describe("PostgresDatabase class", () => {
  const db = new PostgresDatabase();
  const dbTestHelper = new DbTestHelper(db);
  describe("insertNewDataItem method", () => {
    const uniqueDataItemId = "Unique data ID for the new data item tests.";

    it("adds a new_data_item to the database", async () => {
      await db.insertNewDataItem({
        dataItemId: uniqueDataItemId,
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
      )
        .where({ data_item_id: uniqueDataItemId })
        .del()
        .returning("*");
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
      expect(data_item_id).to.equal(uniqueDataItemId);
      expect(uploaded_date).to.exist;
      expect(content_type).to.equal("application/json");
    });

    it("deletes an existing failed_data_item if it exists in the database", async () => {
      await dbTestHelper.insertStubFailedDataItem({
        dataItemId: uniqueDataItemId,
        failedReason: "too_many_failures",
      });

      await db.insertNewDataItem({
        dataItemId: uniqueDataItemId,
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

      const newDataItems = await db["writer"]<FailedDataItemDBResult>(
        tableNames.newDataItem
      )
        .where({ data_item_id: uniqueDataItemId })
        .del()
        .returning("*");
      expect(newDataItems.length).to.equal(1);

      const failedDataItems = await db["writer"]<FailedDataItemDBResult>(
        tableNames.failedDataItem
      )
        .where({ data_item_id: uniqueDataItemId })
        .del()
        .returning("*");
      expect(failedDataItems.length).to.equal(0);
    });
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
    const bundleId = "unique bundle ID insertNewBundle";
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
    const bundleId = "Unique insertPostedBundle Bundle ID";

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
    const bundleId = "Unique insertSeededBundle Bundle ID";
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

  describe("updateSeededBundleToDropped method", () => {
    before(async () => {
      await sleep(333); // Sleep to avoid db race conditions
    });

    it("deletes existing seeded_bundle, inserts failed_bundle, deletes each planned_data_item, and inserts them as new_data_items", async () => {
      const bundleId = "Stub bundle ID updateSeededBundleToDropped";
      const planId = "Stub plan ID updateSeededBundleToDropped";
      const dataItemIds = [
        "testOne updateSeededBundleToDropped",
        "testTwo updateSeededBundleToDropped",
        "testThree updateSeededBundleToDropped",
      ];
      const usdToArRate = stubUsdToArRate;

      await dbTestHelper.insertStubSeededBundle({
        planId,
        bundleId,
        dataItemIds,
        usdToArRate,
        failedBundles: ["testOne", "testTwo"],
      });
      await db.updateSeededBundleToDropped(planId, bundleId);

      // New data items are inserted as expected
      const results = await dbTestHelper.getAndDeleteNewDataItemDbResultsByIds(
        dataItemIds
      );
      expect(results.length).to.equal(3);
      results.forEach((result) => {
        expect(result.failed_bundles).to.equal(`testOne,testTwo,${bundleId}`);
      });

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

      await dbTestHelper.cleanUpSeededBundleInDb({
        bundleId,
        dataItemIds,
        bundleTable: "failed_bundle",
      });
    });

    it("moves planned_data_item to failed_data_item table if they contain more than the retry limit of failed bundles", async () => {
      await sleep(200); // Sleep before this test to avoid race conditions with new_data_item table

      const dataItemId = "updateSeededBundleToDropped f ailed test";
      const planId = "updateSeededBundleToDrop ailed test";
      const bundleId = "updateSeededBundleToD TxID failed test";

      const failedBundles = Array.from(
        { length: retryLimitForFailedDataItems + 2 },
        (_, i) => `failed ${i}`
      );

      await dbTestHelper.insertStubSeededBundle({
        planId,
        bundleId,
        dataItemIds: [dataItemId],
        failedBundles,
        usdToArRate: stubUsdToArRate,
      });

      await db.updateSeededBundleToDropped(planId, bundleId);

      // No new data items are inserted as expected
      expect(
        (
          await db["writer"](tableNames.newDataItem).where({
            data_item_id: dataItemId,
          })
        ).length
      ).to.equal(0);

      // Failed data item exists as expected
      const failedDataItemDbResult = await db["writer"]<FailedDataItemDBResult>(
        tableNames.failedDataItem
      )
        .where({ data_item_id: dataItemId })
        .del()
        .returning("*");

      expect(failedDataItemDbResult.length).to.equal(1);
      expect(failedDataItemDbResult[0].data_item_id).to.equal(dataItemId);
      expect(failedDataItemDbResult[0].failed_reason).to.equal(
        "too_many_failures"
      );
      expect(failedDataItemDbResult[0].failed_bundles).to.equal(
        [...failedBundles, bundleId].join(",")
      );
    });
  });

  describe("updateNewBundleToFailedToPost method", () => {
    it("updates the expected new bundle", async () => {
      await sleep(100); // Sleep before this test to avoid race conditions with new_data_item table

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
      const bundleId = "Unique bundle ID permanentDataItem";

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
        tableNames.permanentDataItems,
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
      const bundleId = "unique bundle ID permanent data item";

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
        tableNames.permanentDataItems
      ).whereIn("data_item_id", dataItemIds);
      expect(permanentDbResult.length).to.equal(3);

      await Promise.all(
        dataItemIds.map((dataItemId) =>
          dbTestHelper.cleanUpEntityInDb(
            tableNames.permanentDataItems,
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
      const bundleId = "re pack data item test bundle id";

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
      const newDbResult =
        await dbTestHelper.getAndDeleteNewDataItemDbResultsByIds(dataItemIds);
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

    it("moves data items to failed_data_item if they are already tried beyond the limit", async () => {
      const dataItemIds = [
        "re pack data item test 1 unique failed",
        "re pack data item test 2 unique failed",
        "re pack data item test 3 unique failed",
      ];
      const bundleId = "re pack data item test bundle id to failed";

      const failedBundles = Array.from(
        { length: retryLimitForFailedDataItems + 1 },
        (_, i) => i.toString()
      );
      await Promise.all(
        dataItemIds.map((dataItemId) =>
          dbTestHelper.insertStubPlannedDataItem({
            dataItemId,
            planId: "A great Unique plan ID",
            failedBundles,
          })
        )
      );

      await db.updateDataItemsToBeRePacked(dataItemIds, bundleId);

      // Planned data items are removed
      const plannedDbResult = await db["writer"](
        tableNames.plannedDataItem
      ).whereIn("data_item_id", dataItemIds);
      expect(plannedDbResult.length).to.equal(0);

      // Failed data items are inserted as expected
      const failedDbResult = await db["writer"](tableNames.failedDataItem)
        .whereIn("data_item_id", dataItemIds)
        .del()
        .returning("*");
      expect(failedDbResult.length).to.equal(3);
      expect(failedDbResult[0].failed_bundles).to.equal(
        failedBundles.join(",") + "," + bundleId
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

    it("deletes failed data items if they exist in the database", async () => {
      const testIds = ["unique failed data item id one"];
      const dataItemBatch = testIds.map((dataItemId) =>
        stubNewDataItem(dataItemId)
      );

      // insert the first data item into the failed data item table
      await dbTestHelper.insertStubFailedDataItem({
        dataItemId: testIds[0],
        failedReason: "missing_from_object_store",
      });

      // Run batch insert with the data item
      await db.insertNewDataItemBatch(dataItemBatch);

      const newDataItems =
        await dbTestHelper.getAndDeleteNewDataItemDbResultsByIds(testIds);

      // Expect only the second data item to have been inserted to new data item table
      expect(newDataItems.length).to.equal(1);
      expect(newDataItems[0].data_item_id).to.equal(testIds[0]);

      // Expect the failed data item to have been removed
      const failedDataItems = await db["writer"]<FailedDataItemDBResult>(
        tableNames.failedDataItem
      ).where({
        data_item_id: testIds[0],
      });
      expect(failedDataItems.length).to.equal(0);
    });

    it("deduplicates data items within the batch", async () => {
      const testIds = [
        "unique deduplication id one",
        "unique deduplication id two",
      ];
      const dataItemBatch = testIds.map((dataItemId) =>
        stubNewDataItem(dataItemId)
      );

      // Run batch insert with the same data item twice
      await db.insertNewDataItemBatch([...dataItemBatch, ...dataItemBatch]);

      const newDataItems =
        await dbTestHelper.getAndDeleteNewDataItemDbResultsByIds(testIds);

      // Expect only one data item to have been inserted to new data item table
      expect(newDataItems.length).to.equal(2);
    });

    it("catches primary key constraint errors and gracefully continues the rest of the batch", async () => {
      const testIds = [
        "0000000000000000000000000000000000000000122",
        "0000000000000000000000000000000000000000222",
        "0000000000000000000000000000000000000000322",
      ];
      const dataItemBatch = testIds.map((dataItemId) =>
        stubNewDataItem(dataItemId)
      );

      // Run the batch insert with the first data item, with two data items, and with all three data items concurrently
      await Promise.all([
        db.insertNewDataItemBatch([dataItemBatch[0]]),
        db.insertNewDataItemBatch(dataItemBatch.slice(0, 2)),
        db.insertNewDataItemBatch(dataItemBatch),
      ]);

      const newDataItems =
        await dbTestHelper.getAndDeleteNewDataItemDbResultsByIds(testIds);

      // Expect all data items to have been inserted to new data item table
      expect(newDataItems.length).to.equal(3);
    });
  });

  describe("updatePlannedDataItemAsFailed method", () => {
    it("updates the expected data item", async () => {
      const dataItemId = "updatePlannedDataItemAsFailed test";

      await dbTestHelper.insertStubPlannedDataItem({
        dataItemId,
        planId: "Unique plan ID",
      });

      await db.updatePlannedDataItemAsFailed({
        dataItemId,
        failedReason: "missing_from_object_store",
      });

      const failedDataItemDbResult = await db["writer"]<FailedDataItemDBResult>(
        tableNames.failedDataItem
      ).where({ data_item_id: dataItemId });
      expect(failedDataItemDbResult.length).to.equal(1);
      expect(failedDataItemDbResult[0].plan_id).to.equal("Unique plan ID");
      expect(failedDataItemDbResult[0].failed_date).to.exist;
    });
  });
});
