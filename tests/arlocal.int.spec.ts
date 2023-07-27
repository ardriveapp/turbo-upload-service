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
import Arweave from "arweave";
import { JWKInterface } from "arweave/node/lib/wallet";
import axios from "axios";
import { expect } from "chai";
import { createReadStream, readFileSync } from "fs";

import { columnNames, tableNames } from "../src/arch/db/dbConstants";
import { PostgresDatabase } from "../src/arch/db/postgres";
import { FileSystemObjectStore } from "../src/arch/fileSystemObjectStore";
import FileDataItem from "../src/bundles/dataItem";
import {
  gatewayUrl,
  txConfirmationThreshold,
  txPermanentThreshold,
  txWellSeededThreshold,
} from "../src/constants";
import { planBundleHandler } from "../src/jobs/plan";
import { postBundleHandler } from "../src/jobs/post";
import { prepareBundleHandler } from "../src/jobs/prepare";
import { seedBundleHandler } from "../src/jobs/seed";
import { verifyBundleHandler } from "../src/jobs/verify";
import {
  BundlePlanDBResult,
  NewBundleDBResult,
  PermanentBundleDBResult,
  PermanentDataItemDBResult,
  PlannedDataItemDBResult,
  PostedBundleDBResult,
  SeededBundleDBResult,
} from "../src/types/dbTypes";
import { TransactionId } from "../src/types/types";
import { jwkToPublicArweaveAddress } from "../src/utils/base64";
import { putDataItemRaw } from "../src/utils/objectStoreUtils";
import { DbTestHelper } from "./helpers/dbTestHelpers";
import {
  arweave,
  fundArLocalWalletAddress,
  mineArLocalBlock,
} from "./test_helpers";

const db = new PostgresDatabase();
const dbTestHelper = new DbTestHelper(db);
const objectStore = new FileSystemObjectStore();

describe("ArLocal <--> Jobs Integration Test", function () {
  const dataItemIds: TransactionId[] = [];
  let jwk: JWKInterface;
  let expectedDataItemCount: number;
  before(async () => {
    jwk = await Arweave.crypto.generateJWK();

    const address = jwkToPublicArweaveAddress(jwk);
    await fundArLocalWalletAddress(arweave, address);

    const dataItems = [
      new FileDataItem("tests/stubFiles/integrationStubDataItem0"),
      new FileDataItem("tests/stubFiles/integrationStubDataItem1"),
      new FileDataItem("tests/stubFiles/integrationStubDataItem2"),
      new FileDataItem("tests/stubFiles/integrationStubDataItem3"),
      new FileDataItem("tests/stubFiles/zeroBytePayloadDataItem"),
    ];
    expectedDataItemCount = dataItems.length;
    await Promise.all(
      dataItems.map(async (dataItem) => {
        expect(await dataItem.isValid()).to.be.true;

        const dataItemId = await dataItem.getTxId();
        const byte_count = (await dataItem.size()).toString();
        const dataItemReadStream = createReadStream(dataItem.filename);

        dataItemIds.push(dataItemId);

        await Promise.all([
          dbTestHelper.insertStubNewDataItem({
            dataItemId,
            byte_count,
          }),
          putDataItemRaw(objectStore, dataItemId, dataItemReadStream),
        ]);
      })
    );
  });

  it("each handler works as expected when given a set of data items", async () => {
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

    expect(plannedDataItemDbResults.length).to.equal(expectedDataItemCount);
    plannedDataItemDbResults.forEach(({ data_item_id }) =>
      expect(dataItemIds).to.include(data_item_id)
    );

    const planId = plannedDataItemDbResults[0].plan_id;

    const bundlePlanDbResult = await db["writer"]<BundlePlanDBResult>(
      tableNames.bundlePlan
    ).where(columnNames.planId, planId);

    expect(bundlePlanDbResult.length).to.equal(1);
    expect(bundlePlanDbResult[0].plan_id).to.equal(planId);
    expect(bundlePlanDbResult[0].planned_date).to.exist;

    await prepareBundleHandler(planId, { jwk, objectStore });

    // newBundle in database
    const newBundleDbResult = await db["writer"]<NewBundleDBResult>(
      tableNames.newBundle
    ).where(columnNames.planId, planId);
    expect(newBundleDbResult.length).to.equal(1);
    expect(newBundleDbResult[0].signed_date).to.exist;
    expect(newBundleDbResult[0].bundle_id).to.exist;

    const bundleId = newBundleDbResult[0].bundle_id;

    // bundlePlan is gone
    const bundlePlanDbResultAfterPrepare = await db[
      "writer"
    ]<BundlePlanDBResult>(tableNames.bundlePlan).where(
      columnNames.planId,
      planId
    );
    expect(bundlePlanDbResultAfterPrepare.length).to.equal(0);

    // bundle tx on disk
    const bundleTx = readFileSync(`temp/bundle/${bundleId}`);
    expect(bundleTx.byteLength).to.equal(2130);

    // bundle header on disk
    const bundleHead = readFileSync(`temp/header/${planId}`);
    expect(bundleHead.byteLength).to.equal(800);

    // bundle payload on disk
    const bundlePayload = readFileSync(`temp/bundle-payload/${planId}`);
    expect(bundlePayload.byteLength).to.equal(800058);

    await postBundleHandler(planId, { objectStore });
    await mineArLocalBlock(arweave);

    // arlocal has the transaction
    const bundleTxFromArLocal = (
      await axios.get(`${gatewayUrl.origin}/tx/${bundleId}`)
    ).data;

    expect(bundleTxFromArLocal.data_root).to.exist;
    expect(bundleTxFromArLocal.data_size).to.equal(800058);
    expect(bundleTxFromArLocal.id).to.equal(bundleId);
    expect(bundleTxFromArLocal.owner_address).to.equal(
      jwkToPublicArweaveAddress(jwk)
    );

    // postedBundle is in the database
    const postedBundleDbResult = await db["writer"]<PostedBundleDBResult>(
      tableNames.postedBundle
    ).where(columnNames.bundleId, bundleId);
    expect(postedBundleDbResult.length).to.equal(1);
    expect(postedBundleDbResult[0].plan_id).to.equal(planId);
    expect(postedBundleDbResult[0].posted_date).to.exist;

    // newBundle is gone
    const newBundleDbResultAfterPost = await db["writer"]<NewBundleDBResult>(
      tableNames.newBundle
    ).where(columnNames.planId, planId);
    expect(newBundleDbResultAfterPost.length).to.equal(0);

    await seedBundleHandler(planId, { objectStore });

    // seededBundle is in the database
    const seededBundleDbResult = await db["writer"]<SeededBundleDBResult>(
      tableNames.seededBundle
    ).where(columnNames.bundleId, bundleId);
    expect(seededBundleDbResult.length).to.equal(1);
    expect(seededBundleDbResult[0].plan_id).to.equal(planId);
    expect(seededBundleDbResult[0].seeded_date).to.exist;

    // newBundle is gone
    const postedBundleDbResultAfterPost = await db[
      "writer"
    ]<PostedBundleDBResult>(tableNames.postedBundle).where(
      columnNames.planId,
      planId
    );
    expect(postedBundleDbResultAfterPost.length).to.equal(0);

    // Wait for 1 second to let arlocal handle that seeding!
    await new Promise((resolve) => {
      setTimeout(resolve, 1000);
    });

    // arlocal has the data
    const bundleDataFromArLocal = (
      await axios.get(`${gatewayUrl.origin}/${bundleId}`)
    ).data;

    // Currently we cannot seem to get a deep equal on the data back from ArLocal that we seeded
    expect(bundleDataFromArLocal).to.exist;

    for (let i = 0; i < txConfirmationThreshold; i++) {
      // Mine blocks until expected confirmed state
      await mineArLocalBlock(arweave);
    }
    await verifyBundleHandler({ objectStore });
    expect(
      (
        await db["writer"]<SeededBundleDBResult>(tableNames.seededBundle).where(
          {
            bundle_id: bundleId,
          }
        )
      ).length
    ).to.equal(1);

    for (let i = 0; i < txWellSeededThreshold; i++) {
      // Mine blocks until expected well-seeded state
      await mineArLocalBlock(arweave);
    }
    await verifyBundleHandler({ objectStore });
    expect(
      (
        await db["writer"]<SeededBundleDBResult>(tableNames.seededBundle).where(
          {
            bundle_id: bundleId,
          }
        )
      ).length
    ).to.equal(1);

    for (let i = 0; i < txPermanentThreshold - txWellSeededThreshold; i++) {
      // Mine blocks until expected permanent state
      await mineArLocalBlock(arweave);
    }
    await verifyBundleHandler({ objectStore });

    // planned data items moved to permanent
    expect(
      (
        await db["writer"](tableNames.plannedDataItem).whereIn(
          columnNames.dataItemId,
          dataItemIds
        )
      ).length
    ).to.equal(0);

    const permanentDataItemDbResults = await db[
      "writer"
    ]<PermanentDataItemDBResult>(tableNames.permanentDataItem).whereIn(
      columnNames.dataItemId,
      dataItemIds
    );

    expect(permanentDataItemDbResults.length).to.equal(expectedDataItemCount);
    permanentDataItemDbResults.forEach(({ data_item_id }) =>
      expect(dataItemIds).to.include(data_item_id)
    );

    const permanentBundleDbResult = await db["writer"]<PermanentBundleDBResult>(
      tableNames.permanentBundle
    ).where(columnNames.planId, planId);

    expect(permanentBundleDbResult.length).to.equal(1);
    expect(permanentBundleDbResult[0].plan_id).to.equal(planId);
    expect(permanentBundleDbResult[0].planned_date).to.exist;

    // seededBundle is gone
    const seededBundleDbResultAfterVerify = await db[
      "writer"
    ]<BundlePlanDBResult>(tableNames.seededBundle).where(
      columnNames.planId,
      planId
    );
    expect(seededBundleDbResultAfterVerify.length).to.equal(0);
  });
});
