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
import axios from "axios";
import { expect } from "chai";
import { stub } from "sinon";

import { ArweaveGateway } from "../src/arch/arweaveGateway";
import { columnNames, tableNames } from "../src/arch/db/dbConstants";
import { PostgresDatabase } from "../src/arch/db/postgres";
import { FileSystemObjectStore } from "../src/arch/fileSystemObjectStore";
import { TurboPaymentService } from "../src/arch/payment";
import { defaultOverdueThresholdMs, gatewayUrl } from "../src/constants";
import { planBundleHandler } from "../src/jobs/plan";
import { postBundleHandler } from "../src/jobs/post";
import {
  BundlePlanDBResult,
  FailedBundleDBResult,
  NewBundleDBResult,
  NewDataItemDBResult,
  PlanId,
  PlannedDataItemDBResult,
  PostedBundleDBResult,
} from "../src/types/dbTypes";
import { Winston } from "../src/types/winston";
import { DbTestHelper } from "./helpers/dbTestHelpers";
import {
  bundleTxStubOwnerAddress,
  stubDates,
  stubTxId10,
  stubTxId11,
  stubTxId12,
  stubUsdToArRate,
  validBundleIdOnFileSystem,
} from "./stubs";
import {
  expectAsyncErrorThrow,
  fundArLocalWalletAddress,
  mineArLocalBlock,
  testArweave,
} from "./test_helpers";

const db = new PostgresDatabase();
const dbTestHelper = new DbTestHelper(db);
const objectStore = new FileSystemObjectStore();
const paymentService = new TurboPaymentService();
const gateway = new ArweaveGateway({ endpoint: gatewayUrl });
describe("Plan bundle job handler function integrated with PostgresDatabase class", () => {
  const dataItemIds = [stubTxId10, stubTxId11, stubTxId12];

  beforeEach(async () => {
    const overdueThresholdTimeISOStr = new Date(
      new Date().getTime() - defaultOverdueThresholdMs
    ).toISOString();
    await Promise.all(
      dataItemIds.map((dataItemId) =>
        dbTestHelper.insertStubNewDataItem({
          dataItemId,
          uploadedDate: overdueThresholdTimeISOStr,
        })
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
  it.skip("only creates one bundle ID when invoked with 2 concurrent executions", async () => {
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
  const bundleId = validBundleIdOnFileSystem;
  const planId = "Unique Post Bundle Job Test Plan Id";
  const signedDate = stubDates.earliestDate;
  const dataItemIds = [stubTxId10, stubTxId11, stubTxId12];

  beforeEach(async () => {
    await dbTestHelper.insertStubNewBundle({
      bundleId,
      planId,
      signedDate,
      dataItemIds,
    });

    await fundArLocalWalletAddress(testArweave, bundleTxStubOwnerAddress);
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
    // stub the response from payment service
    stub(paymentService, "getFiatToARConversionRate").resolves(stubUsdToArRate);
    // Run handler as AWS would
    await postBundleHandler(planId, {
      objectStore,
      database: db,
      arweaveGateway: gateway,
      paymentService,
    });

    const postedBundleDbResult = await db["writer"]<PostedBundleDBResult>(
      tableNames.postedBundle
    ).where(columnNames.bundleId, bundleId);
    expect(postedBundleDbResult.length).to.equal(1);
    expect(postedBundleDbResult[0].plan_id).to.equal(planId);
    expect(postedBundleDbResult[0].planned_date).to.exist;
    expect(postedBundleDbResult[0].planned_date).to.exist;
    // by default knex returns string values for decimals to avoid losing precision, so we have to cast as a number to validate
    expect(postedBundleDbResult[0].usd_to_ar_rate).to.exist;
    expect(+postedBundleDbResult[0].usd_to_ar_rate!).to.equal(stubUsdToArRate); // eslint-disable-line

    await mineArLocalBlock(testArweave);

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

  it("when get fiat to ar conversion rate fails, the post bundle handler still runs as expected", async () => {
    stub(gateway, "postBundleTx").resolves();
    stub(paymentService, "getFiatToARConversionRate").rejects();

    // Run handler as AWS would
    await postBundleHandler(planId, {
      objectStore,
      database: db,
      arweaveGateway: gateway,
      paymentService,
    });

    const postedBundleDbResult = await db["writer"]<PostedBundleDBResult>(
      tableNames.postedBundle
    ).where(columnNames.bundleId, bundleId);
    expect(postedBundleDbResult.length).to.equal(1);
    expect(postedBundleDbResult[0].plan_id).to.equal(planId);
    expect(postedBundleDbResult[0].planned_date).to.exist;
    expect(postedBundleDbResult[0].planned_date).to.exist;
    expect(postedBundleDbResult[0].usd_to_ar_rate).to.not.exist;

    await mineArLocalBlock(testArweave);

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
    stub(paymentService, "getFiatToARConversionRate").resolves(stubUsdToArRate);

    // We expect this handler to run without error so SQS will not attempt to retry this work
    await postBundleHandler(planId, {
      objectStore,
      database: db,
      arweaveGateway: gateway,
      paymentService,
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
        arweaveGateway: gateway,
        paymentService,
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
