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
import { ArweaveSigner, createData } from "arbundles";
import Arweave from "arweave";
import axios from "axios";
import { expect } from "chai";
import { readFileSync, statSync } from "fs";
import { Server } from "http";
import { stub } from "sinon";

import { TurboPaymentService } from "../src/arch/payment";
import { octetStreamContentType } from "../src/constants";
import logger from "../src/logger";
import { createServer } from "../src/server";
import { JWKInterface } from "../src/types/jwkTypes";
import { W, Winston } from "../src/types/winston";
import { generateJunkDataItem, signDataItem } from "./helpers/dataItemHelpers";
import { assertExpectedHeadersWithContentLength } from "./helpers/expectations";
import {
  ethereumDataItem,
  invalidDataItem,
  localTestUrl,
  postStubDataItem,
  solanaDataItem,
  validDataItem,
} from "./test_helpers";

describe("Router tests", () => {
  let server: Server;

  function closeServer() {
    server.close();
    logger.info("Server closed!");
  }

  describe('generic routes"', () => {
    before(() => {
      server = createServer({});
    });

    after(() => {
      closeServer();
    });

    it("GET / returns 'OK' in the body, a 200 status, and the correct content-length", async () => {
      const { status, statusText, headers, data } = await axios.get(
        localTestUrl
      );

      expect(status).to.equal(200);
      expect(statusText).to.equal("OK");

      assertExpectedHeadersWithContentLength(headers, 2);

      expect(data).to.equal("OK");
    });
  });

  describe("Data Item POST `/v1/tx` Route", () => {
    describe("with a default Koa server", () => {
      const paymentService = new TurboPaymentService();
      let nonAllowListedWallet: JWKInterface;

      before(async function () {
        server = createServer({
          paymentService,
        });
        nonAllowListedWallet = await Arweave.crypto.generateJWK();
      });

      after(() => {
        closeServer();
      });

      it("returns the expected data result, a 200 status, the correct content-length, and the data item exists on disk with the correct byte size when signed with an Arweave wallet", async () => {
        const { status, data } = await postStubDataItem(validDataItem);

        expect(status).to.equal(200);

        // cspell:disable
        const id = "QpmY8mZmFEC8RxNsgbxSV6e36OF6quIYaPRKzvUco0o";
        const owner = "J40R1BgFSI1_7p25QW49T7P46BePJJnlDrsFGY1YWbM"; // cspell:enable

        const expectedData = {
          id,
          owner,
          dataCaches: ["arweave.net"],
        };

        expect(data).to.deep.equal(expectedData);

        const fileStats = statSync(`temp/data/${id}`);
        expect(fileStats.size).to.equal(5);

        const rawFileStats = statSync(`temp/raw-data-item/${id}`);
        expect(rawFileStats.size).to.equal(1115);
      });

      it("with a data item signed by a non allow listed wallet with balance", async function () {
        const tags = [{ name: "test", value: "value" }];
        const dataItem = await signDataItem(
          generateJunkDataItem(512, nonAllowListedWallet, tags),
          nonAllowListedWallet
        );

        stub(paymentService, "reserveBalanceForData").resolves({
          costOfDataItem: W("500"),
          isReserved: true,
          walletExists: true,
        });
        const { data } = await postStubDataItem(dataItem);

        expect(data).to.have.property("id");
        expect(data).to.have.property("owner");
        expect(data.dataCaches).to.deep.equal(["arweave.net"]);
      });

      it("with a data item signed by a non allow listed wallet without balance", async () => {
        const tags = [{ name: "test", value: "value" }];
        const dataItem = await signDataItem(
          generateJunkDataItem(512, nonAllowListedWallet, tags),
          nonAllowListedWallet
        );

        stub(paymentService, "reserveBalanceForData").resolves({
          costOfDataItem: W("500"),
          isReserved: false,
          walletExists: true,
        });
        const { status, headers, data, statusText } = await postStubDataItem(
          dataItem
        );
        expect(data).to.equal("Insufficient balance");
        expect(statusText).to.equal("Insufficient balance");
        expect(status).to.equal(401);
        assertExpectedHeadersWithContentLength(headers, 20);
      });

      it("when reserveBalance throws return the correct error", async () => {
        const tags = [{ name: "test", value: "value" }];
        const dataItem = await signDataItem(
          generateJunkDataItem(512, nonAllowListedWallet, tags),
          nonAllowListedWallet
        );

        stub(paymentService, "reserveBalanceForData").throws();
        const { status, data } = await postStubDataItem(dataItem);

        expect(data).to.contain(
          "Upload Service is Unavailable. Payment Service is unreachable"
        );
        expect(status).to.equal(503);
      });

      it("returns the expected result for an empty data item", async () => {
        stub(paymentService, "reserveBalanceForData").resolves({
          costOfDataItem: new Winston(1),
          isReserved: true,
          walletExists: true,
        });
        const signer = new ArweaveSigner(nonAllowListedWallet);
        const createdDataItem = createData("", signer, {});
        await createdDataItem.sign(signer);

        const { data, status } = await axios.post(
          `${localTestUrl}/v1/tx`,
          createdDataItem.getRaw(),
          { headers: { ["Content-Type"]: octetStreamContentType } }
        );
        expect(status).to.equal(200);
        expect(data.id).to.equal(createdDataItem.id);
      });

      it("returns the expected data result, a 200 status, the correct content-length, and the data item exists on disk with the correct byte size when signed with a Solana wallet", async () => {
        const { status, data } = await postStubDataItem(solanaDataItem);

        expect(status).to.equal(200);

        // cspell:disable
        const id = "35jbLhCGEfXLWe2H3VZr2i7f610kwP8Nkw-bFfx14-E";
        const owner = "VrRCYEai_2IveGr0lCiivqLGqenh4wpBnfZNgL-FtWY"; // cspell:enable

        const expectedData = {
          id,
          owner,
          dataCaches: ["arweave.net"],
        };

        expect(data).to.deep.equal(expectedData);

        const fileStats = statSync(`temp/data/${id}`);
        expect(fileStats.size).to.equal(5);

        const rawFileStats = statSync(`temp/raw-data-item/${id}`);
        expect(rawFileStats.size).to.equal(211);
      });

      it("returns the expected data result, a 200 status, the correct content-length, and the data item exists on disk with the correct byte size when signed with an Ethereum wallet", async () => {
        const { status, data } = await postStubDataItem(ethereumDataItem);

        expect(status).to.equal(200);

        // cspell:disable
        const id = "7j-sF0lsslGVZ8lhEGXe5CtueB4iRYM3_oZ9m4GY_40";
        const owner = "xsi06LVwuRe2SaNFo0Yc1UtF3GSyi-GtzUtTkjLXrEw"; // cspell:enable

        const expectedData = {
          id,
          owner,
          dataCaches: ["arweave.net"],
        };

        expect(data).to.deep.equal(expectedData);

        const fileStats = statSync(`temp/data/${id}`);
        expect(fileStats.size).to.equal(5);

        const rawFileStats = statSync(`temp/raw-data-item/${id}`);
        expect(rawFileStats.size).to.equal(245);
      });

      it("with an invalid data item returns an error response", async () => {
        stub(paymentService, "reserveBalanceForData").resolves({
          costOfDataItem: W("1337"),
          isReserved: true,
          walletExists: true,
        });
        const refundSpy = stub(
          paymentService,
          "refundBalanceForData"
        ).resolves();

        const { status, statusText, headers, data } = await postStubDataItem(
          invalidDataItem
        );

        // Refund balance was called
        expect(refundSpy.calledOnce).to.be.true;
        expect(refundSpy.args[0][0].ownerPublicAddress).to.equal(
          "J40R1BgFSI1_7p25QW49T7P46BePJJnlDrsFGY1YWbM"
        );
        expect(refundSpy.args[0][0].winston.toString()).to.equal("1337");

        expect(status).to.equal(400);
        assertExpectedHeadersWithContentLength(headers, 18);

        const expectedData = "Invalid Data Item!";

        expect(statusText).to.equal(expectedData);
        expect(data).to.equal(expectedData);
      });

      it("returns the expected error response when submitting a duplicated data item", async () => {
        await postStubDataItem(
          readFileSync("tests/stubFiles/anotherStubDataItem")
        );

        const { status, statusText, headers, data } = await postStubDataItem(
          readFileSync("tests/stubFiles/anotherStubDataItem")
        );

        expect(status).to.equal(202);
        assertExpectedHeadersWithContentLength(headers, 104);

        const expectedData = // cspell:disable
          "Data item with ID PPqimlPSz890fAufmEs7XnpReEq_o70FvJvz-Leiw1A has already been uploaded to this service!"; // cspell:enable

        expect(statusText).to.equal(expectedData);
        expect(data).to.equal(expectedData);
      });

      it("with the wrong content type in the headers returns an error response", async () => {
        const { status, statusText, headers, data } = await postStubDataItem(
          validDataItem,
          { "Content-Type": "application/json" }
        );

        expect(status).to.equal(400);
        assertExpectedHeadersWithContentLength(headers, 20);

        const expectedData = "Invalid Content Type";

        expect(statusText).to.equal(expectedData);
        expect(data).to.equal(expectedData);
      });
    });

    describe("with a Koa server stubbed with a payment service that allows ARFS data", () => {
      const paymentService = new TurboPaymentService(true);
      before(() => {
        server = createServer({
          paymentService,
        });
      });

      after(() => {
        closeServer();
      });

      it("with a data item smaller than 100KiB signed by a non allow listed wallet and ALLOW_ARFS_DATA set to 'true' returns the expected successful response", async () => {
        const { status, data } = await postStubDataItem(
          readFileSync("tests/stubFiles/stubDataItemFromNonAllowListedWallet")
        );

        expect(status).to.equal(200);

        // cspell:disable
        const id = "4fQZvrmOiRCRIvm_DdtN-EokUKz9DVuFRi1ajtjDMOI";
        const owner = "jaxl_dxqJ00gEgQazGASFXVRvO4h-Q0_vnaLtuOUoWU"; // cspell:enable

        const expectedData = {
          id,
          owner,
          dataCaches: ["arweave.net"],
        };

        expect(data).to.deep.equal(expectedData);

        const fileStats = statSync(`temp/data/${id}`);
        expect(fileStats.size).to.equal(155);

        const rawFileStats = statSync(`temp/raw-data-item/${id}`);
        expect(rawFileStats.size).to.equal(1464);
      });
    });
  });
});
