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
import { DataItem } from "arbundles";
import Arweave from "arweave";
import { JWKInterface } from "arweave/node/lib/wallet";
import axios from "axios";
import { expect } from "chai";
import { createReadStream, readFileSync } from "fs";
import { stub } from "sinon";

import { ArweaveGateway } from "../src/arch/arweaveGateway";
import { columnNames, tableNames } from "../src/arch/db/dbConstants";
import { PostgresDatabase } from "../src/arch/db/postgres";
import { FileSystemObjectStore } from "../src/arch/fileSystemObjectStore";
import { TurboPaymentService } from "../src/arch/payment";
import {
  defaultOverdueThresholdMs,
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
import { fromB64Url, jwkToPublicArweaveAddress } from "../src/utils/base64";
import { putDataItemRaw } from "../src/utils/objectStoreUtils";
import { DbTestHelper } from "./helpers/dbTestHelpers";
import { stubUsdToArRate } from "./stubs";
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

    const dataItemsAndSigs = [
      {
        path: "tests/stubFiles/integrationStubDataItem0",
        signature: fromB64Url(
          "WtAooOuCHTrwu4Ryqhp8mh1DxSBf4D_2TuZUj4rIFSMCXGQ782rza7uLvp-XJhEC9l3f7OuFK1NfpPFbYWs6Qo9TjVbkjoEcroOV5m_FUty1OTEuYmlN-M_IqAUIy-d-LUraR7sF3IkC5RYX29rZBJv6CAvur4OikTpMgVZxs90uuF8Xf0Tzm_l23qpIFZNJIp7ugFusFqu6s1CfMLD5rsNrVn-kq931Tu8oZKBPYZtVIs-teSmY_MLY3vCxGk0zR9Oe8Y6gpkB5a_1kuuKW_JHWM9NNZcpoMZvcR2GKmTAbqxy61WFbSscjAkUnUgmNW11_jZDePFzU7QuQ6owXMZ5xQP3-6yyfcnlUVrkWlWDcPU6MBUfir3-4oE6TMXG2VcNrIViWRFpcfrRTBHTvdYetlKx_yaw5s0KoAARifewxJiRDZi1qe1Dql9n4pOJdWjOBLhn_YaZAA6bXFK3u-rY-LQ-kPiQnrWXMJYeZAggtF25FxzYoCJh8heI5BFoZmEEbLHIHo21VQNgSQQ6K4nCaifKlwdKKNF6aiWhr5Aa4VXH-h2_G8ieF-wp0BMUd9SEK6f0E-NY4DD0C1tP9QRto97W_B0-IEgo7VTVvn8m8e6Gp3OyhsHupdTD4ylXtvxWpcWbQaAG1NAODFcH_sZc-sP0R3DLarPurLiLAz6M"
        ),
      },
      {
        path: "tests/stubFiles/integrationStubDataItem1",
        signature: fromB64Url(
          "yU0v9sH6Jg3jv3AXLUyxvY_-B-8IlCM4wRENQfCN7mY5aDXMirgPZk5M3uy5J3fM3ESqBH0zRukGsyutVhDIHKg386KvHvUjgEPGYM47kPmU7CaJ-2V6ce6CnutJkgsLeHs_DfSjKactntCI_0JAozVyU6ZRDw9SDcI-iVeiTOQ6HH8XeGDcu47rPnxs27VO1Rez8oJ6N3o7bq9kDifI7GIXTg7OsXOmA5WxyEkllvgayjHBkOSshGTF8g2VsD8c-o0H9XzF9U4EUYOdEiUQSJFsgNWwcVfIRIcxWJ2PPFnVgKHMf-vm6myb-t_dW8TAdV1aOqj5xnx7znQLi8ccDs1pZHxe4cqMvtkFd9sozMcmd5YS7KjdEpFHlap2fdo7jvZfvpk2h1hyi5tjcdji18qd1B2P7p376JRZZfFZ56HyJ9bW0QHwC_AFvT9KiuT5WtQ9bMB99_4bscKxzgmRNxRqQidNre6mjD54MOswzXYt0AL9DGdazYkpw5AkmjTdQOgrgqEjTUHECQ64VlHJdlwLvfEj2z6BP9-_0crq5yJBmhRO9O1fqUNt1CFkaxkQJF3WmcdKVnWjlPZbxbF1I15SoFG4XIULm6w6THf6Nyil3I5AQnUM28JxL1wgCZJnD9XevXWsNiHvqpjpp03VLW_EJNlFybjxDj7tWdOPS0E"
        ),
      },
      {
        path: "tests/stubFiles/integrationStubDataItem2",
        signature: fromB64Url(
          "VnQtqfJ3rJb1Sz-YPpER-ud2u9IeOyVcr3O25iFBOGbrbU5OoujIIDTTi8-Ng71-tQqZ2Xgze8CmCaO9UHBK-5NlLX9WTgRzlbBYyiQKb95cyrnM99Ha3QfKr3bA5RAq9-H_fsKdKNm5y6W1arEUCG1AFWgbMGM7yvfqf8qeryMqXtwd_37ndO3GmdIMrFbrAGwLIyn9m3jSZjDiDvdfNOJJwJZplpWb-mWIWauc0RhbDGqJe2oyzOMBr8sJcfaseNbFldilDF9PNASr_WSUfDvU_VaaBQYBgqVPp5zMsUaLuQvpCHHdWgeOh9xRfP602cIjajKXKqlVMSVuZFJFLDOmo3jvffb4l5t3JhqTr4z2DLtIJzya2ZBQkLeIvUd9np3qRoW6Se0d54tmcmGQlnRHy8jnRvdg4Ix6E4YaHDdZXpeYgi5sDlY9oX-RKhEQz_zsl_55a9k7TYbamC69E9xQNV5zUr4b9cLkGWqB2ox13E_797jFdpVVcb16d9ypjPtGEOIeptD3vJ3FIfWOdtIq7jznM-wG6TUfoTuXC5T5p3ddFfAkuXC7H_x_6itxiRW3I-nsAG2SzpWi10r05ma4XDtQ2M42lTe2xOgBVk3vH86Bn5SQVoH8rThPN0rYbJnjkIEYrl8x_tLYqpujo_Pg81gAqJQcHS-LecSO4YE"
        ),
      },
      {
        path: "tests/stubFiles/integrationStubDataItem3",
        signature: fromB64Url(
          "CTRtSIxNseP3BU7VsCMiSWKpV7HGhE5kNleuLTCJSMTEBEMAyMFj_-oFi9NqBJueKvKBoi4Pk87JeBz3ugIkLcYDzIabb_81GvcpTlmvxNDwjm1qgxeP-KU_IsOe7jGm_5PpNdZ1DXXJ4lqGEiX8Oh-sFHAXuw_j4Ln4XMMXHylIcpZCIWsWP5K6zdEjaHOZNhFOVblHPPy6CMNw2NR3yEza2BrpCWrTNeIA3LfIdFnd35ieHsl3-SlAtrJZaGP_taqt7AKfHWZi2arak77yER_jE0Cn_pG6zipollcxBvSpftWjU6zU47Piutub-XHzcePHiPR5JWRt-c4LTOh215hjxapc14kHY2efd71TDL5ajjqVs9jSEOxff75r8hvFwfRYfPjIyJsoY8UpMIFOFkTZpT7DfzOhz5tUunsUopbVoVRnIe9bANEPrnR-rAiQ94J8CqWeDRIIRE7UBJW6tULpSlLX-oAmkeOqfWXydESjcPfM_vkaqpqcMM0NzYxoEsMaddk97eTSz3vm2-oHy0WKfgWO3DXMueRn2vt04ujWCFRXCj2odeUIrqzWC3MYOCYF6nEnXfl2Ak5_Qzsdq-5bktfzmggJoOXq1Nm5f3pYZVTonpe6MKe1GA_DdrpMb6V4d9lFl-m_no-kpHePhgWfB8yYTSvSJuX1QosjmMU"
        ),
      },
      {
        path: "tests/stubFiles/zeroBytePayloadDataItem",
        signature: fromB64Url(
          "HxS86VGVZDqVHv7Y9tJiJssfLMFkEnRz4joXzWXxpVyAdYgmcFIKXE0AdOrk37snD2PWpubtDBQ94gK8th1NTShQBqHEi95cZQe6OzGLUc9pZV4KeUxD5jRSLCngw9V30ny78z5zkvFGU19KOZj3GUvc7B6gmkclORFbaeogdOPK-3LVCP5g7Bh-4tBJj9QjREK1Wcvc3H6BQ0IZsg2pURBwoAWfFTPm1Q6RzhFxo_qOMP9lB1-zFxSHKvYPMxt1qZn7kw1s6J7zLjAyoILLFRb07O35gvJvNpLYmNTvlRpxpmj1J-Lq1YCxBx6lV4oSVLp_2P7NY6F587rTWWl2mH8EoyrkyoctOHZDPolbTdUr5atw47JkIe-hv00Wxx_eQlJAnOxcCrbh-C6tCcVZ8j8DLw6BH9NWKNXyAHe4iME4fm0n436b1sgs0fT01jHa7lf-NQAsmiap1UPx7PlN7fqGBvUtsgYzjIdBaHy9XBrx1jcFYT5jUWDgF_-ZahVi80ZBcmax_I9LdR3dQ0E1KUSi1XQUidJ7rPSpvhHMM3RdKisZiI8OiK1POt7_6gedkWK56Fi2BOu3-UhdtA9GS8qxevoALtL-7lSKs70jjMcfV9I8MswucWCPfsF15DyvqS7olBRDnDANl1USCBGn1mI9al1oYlysX_z6baB1Dkw"
        ),
      },
    ];

    expectedDataItemCount = dataItemsAndSigs.length;
    await Promise.all(
      dataItemsAndSigs.map(async ({ path, signature }) => {
        const dataItem = new DataItem(readFileSync(path));
        expect(await dataItem.isValid()).to.be.true;
        const dataItemId = dataItem.id;
        const byte_count = dataItem.getRaw().byteLength.toString();
        const dataItemReadStream = createReadStream(path);

        dataItemIds.push(dataItemId);
        const overdueThresholdTimeISOStr = new Date(
          new Date().getTime() - defaultOverdueThresholdMs
        ).toISOString();

        await Promise.all([
          dbTestHelper.insertStubNewDataItem({
            dataItemId,
            byte_count,
            signature,
            uploadedDate: overdueThresholdTimeISOStr,
          }),
          putDataItemRaw(
            objectStore,
            dataItemId,
            dataItemReadStream,
            "application/octet-stream",
            1100
          ),
        ]);
      })
    );
  });

  it.skip("each handler works as expected when given a set of data items", async () => {
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
    expect(bundleTx.byteLength).to.equal(2125);

    // bundle payload on disk
    const bundlePayload = readFileSync(`temp/bundle-payload/${planId}`);
    expect(bundlePayload.byteLength).to.equal(268722);

    const paymentService = new TurboPaymentService();
    stub(paymentService, "getFiatToARConversionRate").resolves(stubUsdToArRate);

    await postBundleHandler(planId, { objectStore, paymentService });
    await mineArLocalBlock(arweave);

    // arlocal has the transaction
    const bundleTxFromArLocal = (
      await axios.get(`${gatewayUrl.origin}/tx/${bundleId}`)
    ).data;

    expect(bundleTxFromArLocal.data_root).to.exist;
    expect(bundleTxFromArLocal.data_size).to.equal(268722);
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
    const gateway = new ArweaveGateway({});
    // Stub GQL response as arlocal does not seem to serve unbundled data items on GQL
    stub(gateway, "getDataItemsFromGQL").resolves(
      dataItemIds.map((id) => ({ id, blockHeight: 1 }))
    );
    await verifyBundleHandler({ objectStore, gateway });

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
