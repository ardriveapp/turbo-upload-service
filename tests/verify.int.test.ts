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
import { readFileSync } from "node:fs";
import { Readable } from "node:stream";
import { stub } from "sinon";

import { ArweaveGateway } from "../src/arch/arweaveGateway";
import { columnNames, tableNames } from "../src/arch/db/dbConstants";
import { PostgresDatabase } from "../src/arch/db/postgres";
import { FileSystemObjectStore } from "../src/arch/fileSystemObjectStore";
import {
  gatewayUrl,
  rePostDataItemThresholdNumberOfBlocks,
} from "../src/constants";
import { verifyBundleHandler } from "../src/jobs/verify";
import {
  FailedBundleDBResult,
  PermanentBundleDBResult,
  PermanentDataItemDBResult,
  PlannedDataItemDBResult,
} from "../src/types/dbTypes";
import { DbTestHelper } from "./helpers/dbTestHelpers";
import {
  stubTxId10,
  stubTxId14,
  stubTxId15,
  stubTxId16,
  stubUsdToArRate,
  validBundleIdOnFileSystem,
} from "./stubs";

const db = new PostgresDatabase();
const dbTestHelper = new DbTestHelper(db);
const objectStore = new FileSystemObjectStore();
const gateway = new ArweaveGateway({ endpoint: gatewayUrl });

describe("Verify bundle job handler function integrated with PostgresDatabase class", () => {
  const bundleId = "Verify Job Integration Stub BundleID";
  const planId = stubTxId10;
  const dataItemIds = [stubTxId14, stubTxId15, stubTxId16];
  const usdToArRate = stubUsdToArRate;
  beforeEach(async () => {
    await dbTestHelper.insertStubSeededBundle({
      bundleId,
      planId,
      dataItemIds: dataItemIds,
      usdToArRate,
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
      dataItemIds.map(async (dataItemId) => {
        await dbTestHelper.cleanUpEntityInDb(
          tableNames.plannedDataItem,
          dataItemId
        );
        await dbTestHelper.cleanUpEntityInDb(
          tableNames.permanentDataItem,
          dataItemId
        );
        await dbTestHelper.cleanUpEntityInDb(
          tableNames.newDataItem,
          dataItemId
        );
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

    stub(gateway, "getDataItemsFromGQL").resolves(
      dataItemIds.map((id) => ({ id, blockHeight: 100000 }))
    );
    stub(gateway, "getBlockHeightForTxAnchor").resolves(100000);
    stub(gateway, "getCurrentBlockHeight").resolves(100010);

    await verifyBundleHandler({
      database: db,
      arweaveGateway: gateway,
      objectStore,
    });

    const permanentBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.permanentBundle)
      .where(columnNames.bundleId, bundleId);

    expect(permanentBundleDbResult.length).to.equal(1);

    const permanentDataItemDbResult = await db[
      "writer"
    ]<PermanentDataItemDBResult>(tableNames.permanentDataItem).whereIn(
      columnNames.dataItemId,
      dataItemIds
    );
    expect(permanentDataItemDbResult.length).to.equal(3);
  });

  it("inserts expected permanent result for a batches data items", async () => {
    const numberOfDataItems = 500;
    const dataItemIds = Array.from(
      { length: numberOfDataItems },
      (_, i) => `dataItemId${i}`
    );
    const bundleId = "A Very unique batching verify bundleId";
    const planId = "A Very Unique batching verify planId";
    const usdToArRate = 1;

    await dbTestHelper.insertStubSeededBundle({
      bundleId,
      planId,
      dataItemIds: dataItemIds,
      usdToArRate,
    });

    stub(gateway, "getTransactionStatus").resolves({
      status: "found",
      transactionStatus: {
        block_height: 100000,
        block_indep_hash: "",
        number_of_confirmations: 80,
      },
    });

    stub(gateway, "getDataItemsFromGQL").callsFake((dataItemIds) => {
      return Promise.resolve(
        dataItemIds.map((id) => ({ id, blockHeight: 100000 }))
      );
    });

    stub(gateway, "getBlockHeightForTxAnchor").resolves(100000);
    stub(gateway, "getCurrentBlockHeight").resolves(100010);

    await verifyBundleHandler({
      database: db,
      arweaveGateway: gateway,
      objectStore,
      // Test 100 batches of 5 data items each
      batchSize: 5,
    });

    const permanentBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.permanentBundle)
      .where(columnNames.bundleId, bundleId);

    expect(permanentBundleDbResult.length).to.equal(1);

    const permanentDataItemDbResult = await db[
      "writer"
    ]<PermanentDataItemDBResult>(tableNames.permanentDataItem).whereIn(
      columnNames.dataItemId,
      dataItemIds
    );
    expect(permanentDataItemDbResult.length).to.equal(numberOfDataItems);
  });

  it("inserts failed_bundle and moves data items back to new_data_item if bundle tx could not be found and the tx anchor block height and current block height difference is > 50", async () => {
    stub(gateway, "getTransactionStatus").resolves({
      status: "not found",
    });
    stub(gateway, "getBlockHeightForTxAnchor").resolves(100000);
    stub(gateway, "getCurrentBlockHeight").resolves(100070);

    // stub the object store to return a valid bundle tx to read tx_anchor from
    stub(objectStore, "getObject").resolves({
      readable: Readable.from(
        readFileSync(`temp/bundle/${validBundleIdOnFileSystem}`)
      ),
      etag: "stubEtag",
    });

    await verifyBundleHandler({
      database: db,
      arweaveGateway: gateway,
      objectStore,
    });

    const failedBundleDbResult = await dbTestHelper
      .knex<FailedBundleDBResult>(tableNames.failedBundle)
      .where(columnNames.bundleId, bundleId);

    expect(failedBundleDbResult.length).to.equal(1);

    const permanentBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.permanentBundle)
      .where(columnNames.bundleId, bundleId);
    expect(permanentBundleDbResult.length).to.equal(0);

    const permanentDataItemDbResult = await db[
      "writer"
    ]<PermanentDataItemDBResult>(tableNames.permanentDataItem).whereIn(
      columnNames.dataItemId,
      dataItemIds
    );
    expect(permanentDataItemDbResult.length).to.equal(0);

    const newDataItemDbResult = await dbTestHelper
      .knex<PermanentDataItemDBResult>(tableNames.newDataItem)
      .whereIn(columnNames.dataItemId, dataItemIds);
    expect(newDataItemDbResult.length).to.equal(3);
  });

  it("does not insert any db record if bundle tx could not be found and the tx anchor block height and current block height difference is <= 50", async () => {
    stub(gateway, "getTransactionStatus").resolves({
      status: "not found",
    });

    stub(gateway, "getBlockHeightForTxAnchor").resolves(100000);
    stub(gateway, "getCurrentBlockHeight").resolves(100010);

    await verifyBundleHandler({
      database: db,
      arweaveGateway: gateway,
      objectStore,
    });

    const failedBundleDbResult = await dbTestHelper
      .knex<FailedBundleDBResult>(tableNames.failedBundle)
      .where(columnNames.bundleId, bundleId);
    expect(failedBundleDbResult.length).to.equal(0);

    const permanentBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.permanentBundle)
      .where(columnNames.bundleId, bundleId);
    expect(permanentBundleDbResult.length).to.equal(0);

    const plannedDataItemDbResult = await dbTestHelper
      .knex<PlannedDataItemDBResult>(tableNames.plannedDataItem)
      .whereIn(columnNames.dataItemId, dataItemIds);
    expect(plannedDataItemDbResult.length).to.equal(3);
  });

  it("does not insert any db record if gateway cannot resolve transaction status", async () => {
    stub(gateway, "getTransactionStatus").throws(Error);

    await verifyBundleHandler({
      database: db,
      arweaveGateway: gateway,
      objectStore,
    });

    const permanentBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.permanentBundle)
      .where(columnNames.bundleId, bundleId);
    expect(permanentBundleDbResult.length).to.equal(0);

    const seededBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.seededBundle)
      .where(columnNames.bundleId, bundleId);
    expect(seededBundleDbResult.length).to.equal(1);

    const plannedDataItemDbResult = await dbTestHelper
      .knex<PlannedDataItemDBResult>(tableNames.plannedDataItem)
      .whereIn(columnNames.dataItemId, dataItemIds);
    expect(plannedDataItemDbResult.length).to.equal(3);
  });

  it("does not any insert db record to permanent if confirmations found but not yet above the permanent threshold", async () => {
    stub(gateway, "getTransactionStatus").resolves({
      status: "found",
      transactionStatus: {
        block_height: 100000,
        block_indep_hash: "",
        number_of_confirmations: 49,
      },
    });

    await verifyBundleHandler({
      database: db,
      arweaveGateway: gateway,
      objectStore,
    });

    const permanentBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.permanentBundle)
      .where(columnNames.bundleId, bundleId);

    expect(permanentBundleDbResult.length).to.equal(0);

    const seededBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.seededBundle)
      .where(columnNames.bundleId, bundleId);
    expect(seededBundleDbResult.length).to.equal(1);

    const plannedDataItemDbResult = await dbTestHelper
      .knex<PlannedDataItemDBResult>(tableNames.plannedDataItem)
      .whereIn(columnNames.dataItemId, dataItemIds);
    expect(plannedDataItemDbResult.length).to.equal(3);
  });

  // TODO: Test for under re-post data item threshold, test for byte found backoff

  it("when a bundle transaction is found to be permanent, data items that are not queryable or do not have blocks via GQL are moved back to new data item while those that are found and have blocks are moved to permanent", async () => {
    stub(gateway, "getTransactionStatus").resolves({
      status: "found",
      transactionStatus: {
        block_height: 100000,
        block_indep_hash: "",
        number_of_confirmations: rePostDataItemThresholdNumberOfBlocks + 1,
      },
    });
    stub(gateway, "getDataItemsFromGQL").resolves([
      { id: stubTxId14, blockHeight: 100000 },
      // we simulate stubTxId15 being not found on GQL and stubTxId16 not having a block
      { id: stubTxId16, blockHeight: undefined },
    ]);

    await verifyBundleHandler({
      database: db,
      arweaveGateway: gateway,
      objectStore,
    });

    const permanentBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.permanentBundle)
      .where(columnNames.bundleId, bundleId);

    expect(permanentBundleDbResult.length).to.equal(1);

    const seededBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.seededBundle)
      .where(columnNames.bundleId, bundleId);
    expect(seededBundleDbResult.length).to.equal(0);

    const plannedDataItemDbResult = await dbTestHelper
      .knex<PlannedDataItemDBResult>(tableNames.plannedDataItem)
      .whereIn(columnNames.dataItemId, dataItemIds);
    expect(plannedDataItemDbResult.length).to.equal(0);

    const permanentDataItemDbResult = await db[
      "writer"
    ]<PermanentDataItemDBResult>(tableNames.permanentDataItem).where(
      columnNames.dataItemId,
      stubTxId14
    );
    expect(permanentDataItemDbResult.length).to.equal(1);

    const newDataItemDbResult = await dbTestHelper
      .knex<PermanentDataItemDBResult>(tableNames.newDataItem)
      .whereIn(columnNames.dataItemId, [stubTxId15, stubTxId16]);
    expect(newDataItemDbResult.length).to.equal(2);
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
        number_of_confirmations: 150,
      },
    });

    stub(gateway, "getBlockHeightForTxAnchor").resolves(100000);
    stub(gateway, "getCurrentBlockHeight").resolves(100010);

    const input = {
      database: db,
      arweaveGateway: gateway,
      objectStore,
    };

    await Promise.all([verifyBundleHandler(input), verifyBundleHandler(input)]);

    const permanentBundleDbResult = await dbTestHelper
      .knex<PermanentBundleDBResult>(tableNames.permanentBundle)
      .where(columnNames.bundleId, bundleId);

    expect(permanentBundleDbResult.length).to.equal(1);
  });
});
