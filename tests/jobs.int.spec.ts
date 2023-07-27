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
import axios from "axios";
import { expect } from "chai";
import { stub } from "sinon";

import { ArweaveGateway } from "../src/arch/arweaveGateway";
import { columnNames, tableNames } from "../src/arch/db/dbConstants";
import { PostgresDatabase } from "../src/arch/db/postgres";
import { FileSystemObjectStore } from "../src/arch/fileSystemObjectStore";
import { gatewayUrl } from "../src/constants";
import { planBundleHandler } from "../src/jobs/plan";
import { postBundleHandler } from "../src/jobs/post";
import { verifyBundleHandler } from "../src/jobs/verify";
import {
  BundlePlanDBResult,
  FailedBundleDBResult,
  NewBundleDBResult,
  NewDataItemDBResult,
  PermanentBundleDBResult,
  PlanId,
  PlannedDataItemDBResult,
  PostedBundleDBResult,
} from "../src/types/dbTypes";
import { Winston } from "../src/types/winston";
import { DbTestHelper } from "./helpers/dbTestHelpers";
import {
  bundleTxStubOwnerAddress,
  stubDates,
  stubPlanId,
  stubTxId10,
  stubTxId11,
  stubTxId12,
  stubTxId14,
  stubTxId15,
  stubTxId16,
} from "./stubs";
import {
  arweave,
  expectAsyncErrorThrow,
  fundArLocalWalletAddress,
  mineArLocalBlock,
} from "./test_helpers";

const db = new PostgresDatabase();
const dbTestHelper = new DbTestHelper(db);
const objectStore = new FileSystemObjectStore();
const gateway = new ArweaveGateway();
describe("Plan bundle job handler function integrated with PostgresDatabase class", () => {
  const dataItemIds = [stubTxId10, stubTxId11, stubTxId12];

  beforeEach(async () => {
    await Promise.all(
      dataItemIds.map((dataItemId) =>
        dbTestHelper.insertStubNewDataItem({ dataItemId })
      )
    );
  });

  let planId: PlanId;

  afterEach(async () => {
    if (planId) {
      await dbTestHelper.cleanUpBundlePlanInDb({
        planId,
        dataItemIds,
      });
    }
  });

  it("inserts bundle_plan, removes new_data_items, and inserts them as planned_data_items as expected", async () => {
    // Run handler as AWS would
    await planBundleHandler();

    expect(
      (
        await db["writer"](tableNames.newDataItem).whereIn(
          columnNames.dataItemId,
          dataItemIds
        )
      ).length
    ).to.equal(0);

    const plannedDataItemDbResults = await db[
      "writer"
    ]<PlannedDataItemDBResult>(tableNames.plannedDataItem).whereIn(
      columnNames.dataItemId,
      dataItemIds
    );

    expect(plannedDataItemDbResults.length).to.equal(3);
    plannedDataItemDbResults.forEach(({ data_item_id }) =>
      expect(dataItemIds).to.include(data_item_id)
    );

    planId = plannedDataItemDbResults[0].plan_id;

    const bundlePlanDbResult = await db["writer"]<BundlePlanDBResult>(
      tableNames.bundlePlan
    ).where(columnNames.planId, planId);

    expect(bundlePlanDbResult.length).to.equal(1);
    expect(bundlePlanDbResult[0].plan_id).to.equal(planId);
    expect(bundlePlanDbResult[0].planned_date).to.exist;
  });

  /**
   * Simulate 2 concurrent executions, which should cause locking errors to occur
   *
   * Note: this is a brittle test, as it's not always guaranteed to produce locking
   * errors, BUT, it should ALWAYS pass.
   * */
  it("only creates one bundle ID when invoked with 2 concurrent executions", async () => {
    await Promise.all([planBundleHandler(), planBundleHandler()]);

    const plannedDataItemDbResults = await db[
      "writer"
    ]<PlannedDataItemDBResult>(tableNames.plannedDataItem).whereIn(
      columnNames.dataItemId,
      dataItemIds
    );

    // should still only produce 3 data items in planned table
    expect(plannedDataItemDbResults.length).to.equal(3);
    plannedDataItemDbResults.forEach(({ data_item_id }) =>
      expect(dataItemIds).to.include(data_item_id)
    );

    planId = plannedDataItemDbResults[0].plan_id;

    // every data item should have the same bundle id
    expect(plannedDataItemDbResults.every((d) => d.plan_id === planId)).to.be
      .true;

    // don't filter, there should only be one
    const bundlePlanDbResult = await db["writer"]<BundlePlanDBResult>(
      tableNames.bundlePlan
    );

    // only one bundle plan should be created
    expect(bundlePlanDbResult.length).to.equal(1);
    expect(bundlePlanDbResult[0].plan_id).to.equal(planId);
    expect(bundlePlanDbResult[0].planned_date).to.exist;
  });
});

describe("Post bundle job handler function integrated with PostgresDatabase class", () => {
  // cspell:disable
  const bundleId = "uMguurlEh9a7MKYiauKGlbxG6OjP2xaGmWa1-vrHVh8"; // cspell:enable
  const planId = stubPlanId;
  const signedDate = stubDates.earliestDate;
  const dataItemIds = [stubTxId10, stubTxId11, stubTxId12];

  beforeEach(async () => {
    await dbTestHelper.insertStubNewBundle({
      bundleId,
      planId,
      signedDate,
      dataItemIds,
    });

    await fundArLocalWalletAddress(arweave, bundleTxStubOwnerAddress);
  });

  afterEach(async () => {
    await Promise.all([
      dbTestHelper.cleanUpEntityInDb(tableNames.newBundle, bundleId),
      dbTestHelper.cleanUpEntityInDb(tableNames.postedBundle, bundleId),
      dbTestHelper.cleanUpEntityInDb(tableNames.failedBundle, bundleId),
      ...dataItemIds.map((d) => [
        dbTestHelper.cleanUpEntityInDb(tableNames.newDataItem, d),
        dbTestHelper.cleanUpEntityInDb(tableNames.plannedDataItem, d),
      ]),
    ]);
  });

  it("when post to Arweave succeeds, promotes new_bundle to posted_bundle and transaction returns from arlocal as expected", async () => {
    // Run handler as AWS would
    await postBundleHandler(planId, {
      objectStore,
      database: db,
      gateway,
    });

    const postedBundleDbResult = await db["writer"]<PostedBundleDBResult>(
      tableNames.postedBundle
    ).where(columnNames.bundleId, bundleId);
    expect(postedBundleDbResult.length).to.equal(1);
    expect(postedBundleDbResult[0].plan_id).to.equal(planId);
    expect(postedBundleDbResult[0].planned_date).to.exist;

    await mineArLocalBlock(arweave);

    const bundleTxFromArLocal = (
      await axios.get(`${gatewayUrl.origin}/tx/${bundleId}`)
    ).data;

    expect(bundleTxFromArLocal.data_root).to.equal(
      // cspell:disable
      "JfGW9Ths4z-IH-UJwvhq4U14kyQmpZomOx6jeiFsM-Y"
    ); // cspell:enable
    expect(bundleTxFromArLocal.data_size).to.equal(1211);
    expect(bundleTxFromArLocal.id).to.equal(bundleId);
    expect(bundleTxFromArLocal.owner_address).to.equal(
      bundleTxStubOwnerAddress
    );
  });

  it("when post to Arweave fails, promotes new_bundle to failed_bundle and demotes each planned_data_item back to new_data_item", async () => {
    stub(gateway, "postBundleTx").throws();

    // We expect this handler to run without error so SQS will not attempt to retry this work
    await postBundleHandler(planId, {
      objectStore,
      database: db,
      gateway: gateway,
    });

    const newBundleDbResult = await db["writer"]<NewBundleDBResult>(
      tableNames.newBundle
    ).where(columnNames.bundleId, bundleId);

    expect(newBundleDbResult.length).to.equal(0);

    const postedBundleDbResult = await db["writer"]<PostedBundleDBResult>(
      tableNames.postedBundle
    ).where(columnNames.bundleId, bundleId);

    expect(postedBundleDbResult.length).to.equal(0);

    const failedBundleDbResult = await db["writer"]<FailedBundleDBResult>(
      tableNames.failedBundle
    ).where(columnNames.bundleId, bundleId);

    expect(failedBundleDbResult.length).to.equal(1);

    const newDataItemDbResult = await db["writer"]<NewDataItemDBResult>(
      tableNames.newDataItem
    ).whereIn(columnNames.dataItemId, dataItemIds);

    expect(newDataItemDbResult.length).to.equal(3);
    newDataItemDbResult.forEach(({ data_item_id }) =>
      expect(dataItemIds).to.include(data_item_id)
    );
  });

  it("throw and error as expected when post to Arweave fails and balance for wallet is empty", async () => {
    stub(gateway, "postBundleTx").throws();
    stub(gateway, "getBalanceForWallet").resolves(new Winston(0));

    // We expect this handler to encounter an error so SQS will retry this work then send to DLQ
    await expectAsyncErrorThrow({
      promiseToError: postBundleHandler(planId, {
        objectStore,
        database: db,
        gateway: gateway,
      }),
      errorMessage:
        "Wallet does not have enough balance for this bundle post! Current Balance: 0, Reward for Bundle: 2379774852",
    });

    const newBundleDbResult = await db["writer"]<NewBundleDBResult>(
      tableNames.newBundle
    ).where(columnNames.bundleId, bundleId);

    expect(newBundleDbResult.length).to.equal(1);

    const postedBundleDbResult = await db["writer"]<PostedBundleDBResult>(
      tableNames.postedBundle
    ).where(columnNames.bundleId, bundleId);

    expect(postedBundleDbResult.length).to.equal(0);

    const failedBundleDbResult = await db["writer"]<FailedBundleDBResult>(
      tableNames.failedBundle
    ).where(columnNames.bundleId, bundleId);

    expect(failedBundleDbResult.length).to.equal(0);

    const plannedDataItemDbResult = await db["writer"]<NewDataItemDBResult>(
      tableNames.plannedDataItem
    ).whereIn(columnNames.dataItemId, dataItemIds);

    expect(plannedDataItemDbResult.length).to.equal(3);
    plannedDataItemDbResult.forEach(({ data_item_id }) =>
      expect(dataItemIds).to.include(data_item_id)
    );
  });
});

describe("Verify bundle job handler function integrated with PostgresDatabase class", () => {
  // cspell:disable
  const bundleId = "uMguurlEh9a7MKYiauKGlbxG6OjP2xaGmWa1-vrHVh8"; // cspell:enable
  const planId = stubTxId10;
  const dataItemIds = [stubTxId14, stubTxId15, stubTxId16];
  beforeEach(async () => {
    await dbTestHelper.insertStubSeededBundle({
      bundleId,
      planId,
      dataItemIds: dataItemIds,
    });
    await dbTestHelper.insertStubPostedBundle({
      bundleId,
      planId,
      dataItemIds: dataItemIds,
    });
  });
  afterEach(async () => {
    await Promise.all([
      dbTestHelper.cleanUpEntityInDb(tableNames.postedBundle, bundleId),
      dbTestHelper.cleanUpEntityInDb(tableNames.seededBundle, bundleId),
      dbTestHelper.cleanUpEntityInDb(tableNames.permanentBundle, bundleId),
      dbTestHelper.cleanUpEntityInDb(tableNames.failedBundle, bundleId),
    ]);
    await Promise.all(
      dataItemIds.map((dataItemId) => {
        dbTestHelper.cleanUpEntityInDb(tableNames.plannedDataItem, dataItemId);
        dbTestHelper.cleanUpEntityInDb(
          tableNames.permanentDataItem,
          dataItemId
        );
        dbTestHelper.cleanUpEntityInDb(tableNames.newDataItem, dataItemId);
      })
    );
  });

  it("inserts db record to permanent bundle if sufficient confirmations", async () => {
    stub(gateway, "getTransactionStatus").resolves({
      status: "found",
      transactionStatus: {
        block_height: 100000,
        block_indep_hash: "",
        number_of_confirmations: 80,
      },
    });

    stub(gateway, "isTransactionQueryableOnGQL").resolves(true);
    stub(gateway, "getBlockHeightForTxAnchor").resolves(100000);
    stub(gateway, "getCurrentBlockHeight").resolves(100010);

    await verifyBundleHandler({
      database: db,
      gateway: gateway,
      objectStore,
    });

    const permanentBundleDbResult = await db["writer"]<PermanentBundleDBResult>(
      tableNames.permanentBundle
    ).where(columnNames.bundleId, bundleId);

    expect(permanentBundleDbResult.length).to.equal(1);
  });

  it("inserts db record to failed bundle if tx anchor block height and current block height difference is > 50", async () => {
    stub(gateway, "getTransactionStatus").resolves({
      status: "not found",
    });

    stub(gateway, "isTransactionQueryableOnGQL").resolves(true);
    stub(gateway, "getBlockHeightForTxAnchor").resolves(100000);
    stub(gateway, "getCurrentBlockHeight").resolves(100070);

    await verifyBundleHandler({
      database: db,
      gateway: gateway,
      objectStore,
    });

    const failedBundleDbResult = await db["writer"]<FailedBundleDBResult>(
      tableNames.failedBundle
    ).where(columnNames.bundleId, bundleId);

    expect(failedBundleDbResult.length).to.equal(1);
  });

  it("does not insert any db record if tx anchor block height and current block height difference is <= 50", async () => {
    stub(gateway, "getTransactionStatus").resolves({
      status: "not found",
    });

    stub(gateway, "isTransactionQueryableOnGQL").resolves(true);
    stub(gateway, "getBlockHeightForTxAnchor").resolves(100000);
    stub(gateway, "getCurrentBlockHeight").resolves(100010);

    await verifyBundleHandler({
      database: db,
      gateway: gateway,
      objectStore,
    });

    const failedBundleDbResult = await db["writer"]<FailedBundleDBResult>(
      tableNames.failedBundle
    ).where(columnNames.bundleId, bundleId);

    expect(failedBundleDbResult.length).to.equal(0);
    const permanentBundleDbResult = await db["writer"]<PermanentBundleDBResult>(
      tableNames.permanentBundle
    ).where(columnNames.bundleId, bundleId);

    expect(permanentBundleDbResult.length).to.equal(0);
  });

  it("does not insert db record to permanent bundle if no confirmations found", async () => {
    stub(gateway, "getTransactionStatus").throws(Error);
    stub(gateway, "isTransactionQueryableOnGQL").resolves(false);

    await verifyBundleHandler({
      database: db,
      gateway: gateway,
      objectStore,
    });
    const permanentBundleDbResult = await db["writer"]<PermanentBundleDBResult>(
      tableNames.permanentBundle
    ).where(columnNames.bundleId, bundleId);

    expect(permanentBundleDbResult.length).to.equal(0);
  });

  it("does not insert db record to permanent bundle if confirmations found but dataitem is not queryable", async () => {
    stub(gateway, "getTransactionStatus").resolves();

    stub(gateway, "isTransactionQueryableOnGQL").resolves(false);

    await verifyBundleHandler({
      database: db,
      gateway: gateway,
      objectStore,
    });

    const permanentBundleDbResult = await db["writer"]<PermanentBundleDBResult>(
      tableNames.permanentBundle
    ).where(columnNames.bundleId, bundleId);

    expect(permanentBundleDbResult.length).to.equal(0);
  });

  /**
   * Simulate 2 concurrent executions, which should cause locking errors to occur
   *
   * Note: this is a brittle test, as it's not always guaranteed to produce locking
   * errors, BUT, it should ALWAYS pass.
   * */
  it("updates seed result appropriately with 2 concurrent executions, handling locking errors gracefully", async () => {
    stub(gateway, "getTransactionStatus").resolves({
      status: "found",
      transactionStatus: {
        block_height: 100000,
        block_indep_hash: "",
        number_of_confirmations: 80,
      },
    });

    stub(gateway, "isTransactionQueryableOnGQL").resolves(true);
    stub(gateway, "getBlockHeightForTxAnchor").resolves(100000);
    stub(gateway, "getCurrentBlockHeight").resolves(100010);

    const input = {
      database: db,
      gateway: gateway,
      objectStore,
    };

    await Promise.all([verifyBundleHandler(input), verifyBundleHandler(input)]);

    const permanentBundleDbResult = await db["writer"]<PermanentBundleDBResult>(
      tableNames.permanentBundle
    ).where(columnNames.bundleId, bundleId);

    expect(permanentBundleDbResult.length).to.equal(1);
  });
});
