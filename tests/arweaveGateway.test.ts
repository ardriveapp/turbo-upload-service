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
import { ArweaveSigner, bundleAndSignData, createData } from "arbundles";
import axios from "axios";
import { expect } from "chai";
import { randomBytes } from "crypto";
import { describe } from "mocha";
import { stub } from "sinon";

import { ArweaveGateway } from "../src/arch/arweaveGateway";
import { ExponentialBackoffRetryStrategy } from "../src/arch/retryStrategy";
import { gatewayUrl } from "../src/constants";
import { TransactionId } from "../src/types/types";
import { jwkToPublicArweaveAddress } from "../src/utils/base64";
import {
  arweave,
  fundArLocalWalletAddress,
  mineArLocalBlock,
  testArweaveJWK,
} from "./test_helpers";

describe("ArweaveGateway Class", function () {
  let gateway: ArweaveGateway;
  let validDataItemId: TransactionId;
  let validAnchor: string;

  before(async () => {
    const jwk = testArweaveJWK;
    await fundArLocalWalletAddress(arweave, jwkToPublicArweaveAddress(jwk));

    const signer = new ArweaveSigner(jwk);
    const dataItem = createData(randomBytes(10), signer);

    const bundle = await bundleAndSignData([dataItem], signer);

    validDataItemId = bundle.items[0].id;
    const tx = await bundle.toTransaction({}, arweave, jwk);
    validAnchor = tx.last_tx;
    await axios.post(`${gatewayUrl.origin}/tx`, tx);
    await mineArLocalBlock(arweave);
  });

  beforeEach(() => {
    // recreate for each test to avoid caching issues
    gateway = new ArweaveGateway({
      endpoint: gatewayUrl,
      retryStrategy: new ExponentialBackoffRetryStrategy({
        maxRetriesPerRequest: 0,
      }),
    });
  });

  it("getDataItemsFromGQL can get blocks for valid data items from GQL", async () => {
    const result = await gateway.getDataItemsFromGQL([validDataItemId]);
    expect(result).to.have.lengthOf(1);
    expect(result[0]).to.have.property("id", validDataItemId);
    expect(result[0]).to.have.property("blockHeight");
  });

  it("getDataItemsFromGQL returns empty array for invalid data items", async () => {
    const result = await gateway.getDataItemsFromGQL(["invalid item"]);
    expect(result).to.have.lengthOf(0);
  });

  it("Given a valid txAnchor getBlockHeightForTxAnchor returns correct height", async () => {
    const result = await gateway.getBlockHeightForTxAnchor(validAnchor);
    expect(result).to.be.above(-1);
  });

  it("getCurrentBlockHeight returns a height", async () => {
    const result = await gateway.getCurrentBlockHeight();
    expect(result).to.be.above(0);
  });

  it("getCurrentBlockHeight falls back to /block/current when GQL fails", async function () {
    // TODO: remove this stub once arlocal supports /block/current endpoint - REF: https://github.com/textury/arlocal/issues/158
    stub(gateway["axiosInstance"], "get").resolves({
      status: 200,
      data: {
        height: 12345679,
        timestamp: Date.now(),
      },
    });
    const postStub = stub(gateway["axiosInstance"], "post");
    // mock gql response to fail to force fallback
    for (const failedStatus of [400, 404, 500, 502, 503, 504]) {
      // TODO: use fake timers to fast forward the cache, but doing so will impact how retries are handled
      postStub.rejects({
        status: failedStatus,
        message: "Internal Server Error",
      });
      const result = await gateway.getCurrentBlockHeight();
      expect(result).to.equal(12345679);
    }
  });
});
