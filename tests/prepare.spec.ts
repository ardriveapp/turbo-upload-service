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
import { expect } from "chai";
import { stub } from "sinon";

import { ArweaveGateway } from "../src/arch/arweaveGateway";
import { ArDriveCommunityOracle } from "../src/arch/community/ardriveCommunityOracle";
import { columnNames, tableNames } from "../src/arch/db/dbConstants";
import { PostgresDatabase } from "../src/arch/db/postgres";
import { FileSystemObjectStore } from "../src/arch/fileSystemObjectStore";
import { PricingService } from "../src/arch/pricing";
import { gatewayUrl } from "../src/constants";
import { prepareBundleHandler } from "../src/jobs/prepare";
import { BundlePlanDBResult, NewBundleDBResult } from "../src/types/dbTypes";
import { JWKInterface } from "../src/types/jwkTypes";
import { W } from "../src/types/winston";
import {
  getBundleHeader,
  getBundlePayload,
  getBundleTx,
} from "../src/utils/objectStoreUtils";
import { streamToBuffer } from "../src/utils/streamToBuffer";
import { DbTestHelper } from "./helpers/dbTestHelpers";
import {
  stubOwnerAddress,
  stubPlanId,
  stubTxId20,
  stubTxId21,
  stubTxId22,
} from "./stubs";
import { deleteStubRawDataItems, writeStubRawDataItems } from "./test_helpers";

const db = new PostgresDatabase();
const dbTestHelper = new DbTestHelper(db);

describe("Prepare bundle job handler", () => {
  let jwk: JWKInterface;

  const dataItemIds = [stubTxId20, stubTxId21, stubTxId22];
  const planId = stubPlanId;
  const stubDataItemPath = "tests/stubFiles/stub1115ByteDataItem";

  before(async function () {
    jwk = await Arweave.crypto.generateJWK();

    await writeStubRawDataItems(dataItemIds, stubDataItemPath);
  });

  after(async () => {
    deleteStubRawDataItems(dataItemIds);
  });

  beforeEach(async () => {
    await dbTestHelper.insertStubBundlePlan({ dataItemIds, planId });
  });

  afterEach(async () => {
    await dbTestHelper.cleanUpNewBundleInDb({
      planId,
      dataItemIds,
    });
  });

  it("removes bundle_plan, inserts new_bundle, and writes the expected bundle tx, bundle payload, and bundle header to Object Store", async () => {
    const objectStore = new FileSystemObjectStore();

    await prepareBundleHandler(planId, { objectStore, jwk });

    const bundlePlanDbResult = await db["writer"]<BundlePlanDBResult>(
      tableNames.bundlePlan
    ).where(columnNames.planId, planId);
    expect(bundlePlanDbResult.length).to.equal(0);

    const newBundleDbResult = await db["writer"]<NewBundleDBResult>(
      tableNames.newBundle
    ).where(columnNames.planId, planId);
    expect(newBundleDbResult.length).to.equal(1);

    const bundleTxId = newBundleDbResult[0].bundle_id;

    const bundleTx = await getBundleTx(objectStore, bundleTxId, 2125);

    // We expect no tips on bundle transactions by default
    expect(bundleTx.quantity).to.equal("0");
    expect(bundleTx.target).to.equal("");

    const bundlePayload = await getBundlePayload(objectStore, planId);
    expect((await streamToBuffer(bundlePayload, 3569)).byteLength).to.equal(
      3569
    );

    const bundleHeader = await getBundleHeader(objectStore, planId);
    expect((await streamToBuffer(bundleHeader, 224)).byteLength).to.equal(224);
  });

  it("adds the expected quantity and target values when ADD_COMMUNITY_TIP is true", async () => {
    const objectStore = new FileSystemObjectStore();

    const communityOracle = new ArDriveCommunityOracle();
    stub(communityOracle, "getCommunityWinstonTip").resolves(W(1));
    stub(communityOracle, "selectTokenHolder").resolves(stubOwnerAddress);

    await prepareBundleHandler(planId, {
      objectStore,
      jwk,
      pricing: new PricingService(
        new ArweaveGateway({
          endpoint: gatewayUrl,
        }),
        communityOracle,
        true
      ),
    });

    const bundleTxId = (
      await db["writer"]<NewBundleDBResult>(tableNames.newBundle).where(
        columnNames.planId,
        planId
      )
    )[0].bundle_id;

    const bundleTx = await getBundleTx(objectStore, bundleTxId, 2168);

    // We expect a tip on bundle transactions when env var is true
    expect(bundleTx.quantity).to.equal("1");
    expect(bundleTx.target).to.equal(stubOwnerAddress);
  });
});
