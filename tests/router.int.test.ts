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

import { ArweaveGateway } from "../src/arch/arweaveGateway";
import { TurboPaymentService } from "../src/arch/payment";
import { octetStreamContentType, receiptVersion } from "../src/constants";
import logger from "../src/logger";
import { createServer } from "../src/server";
import { JWKInterface } from "../src/types/jwkTypes";
import { W } from "../src/types/winston";
import { verifyReceipt } from "../src/utils/verifyReceipt";
import { generateJunkDataItem, signDataItem } from "./helpers/dataItemHelpers";
import { assertExpectedHeadersWithContentLength } from "./helpers/expectations";
import {
  ethereumDataItem,
  invalidDataItem,
  localTestUrl,
  postStubDataItem,
  solanaDataItem,
  stubDataItemWithEmptyStringsForTagNamesAndValues,
  validDataItem,
} from "./test_helpers";

const publicAndSigLength = 683;

describe("Router tests", function () {
  let server: Server;

  function closeServer() {
    server.close();
    logger.info("Server closed!");
  }

  describe('generic routes"', () => {
    before(() => {
      server = createServer({
        getArweaveWallet: () =>
          Promise.resolve(
            JSON.parse(
              readFileSync("tests/stubFiles/testWallet.json", {
                encoding: "utf-8",
              })
            )
          ),
      });
    });

    after(() => {
      closeServer();
    });

    it("GET / returns arweave address, receipt version, and gateway in the body, a 200 status, and the correct content-length", async () => {
      const { status, statusText, headers, data } = await axios.get(
        localTestUrl
      );

      expect(status).to.equal(200);
      expect(statusText).to.equal("OK");

      expect(headers["content-type"]).to.equal(
        "application/json; charset=utf-8"
      );
      expect(headers.connection).to.equal("close");

      expect(data).to.deep.equal({
        version: "0.2.0",
        addresses: {
          arweave: "8wgRDgvYOrtSaWEIV21g0lTuWDUnTu4_iYj4hmA7PI0",
        },
        gateway: "arlocal",
      });
    });
  });

  describe("Data Item POST `/v1/tx` Route", () => {
    const paymentService = new TurboPaymentService();

    const arweaveGateway = new ArweaveGateway({
      endpoint: new URL("http://fake.com"),
    });

    let receiptSigningWallet: JWKInterface;
    beforeEach(() => {
      stub(arweaveGateway, "getCurrentBlockHeight").resolves(500);
    });

    before(async () => {
      receiptSigningWallet = await Arweave.crypto.generateJWK();
    });

    describe("with a default Koa server", () => {
      before(async function () {
        server = createServer({
          paymentService,
          getArweaveWallet: () => Promise.resolve(receiptSigningWallet),
          arweaveGateway,
        });
      });

      after(() => {
        closeServer();
      });

      describe("with stubbed successful reserve and check balance methods", () => {
        beforeEach(() => {
          stub(paymentService, "reserveBalanceForData").resolves({
            costOfDataItem: W("500"),
            isReserved: true,
            walletExists: true,
          });
          stub(paymentService, "checkBalanceForData").resolves({
            bytesCostInWinc: W("500"),
            userHasSufficientBalance: true,
            userBalanceInWinc: W("1000"),
          });
        });

        it("returns the expected data result, a 200 status, the correct content-length, and the data item exists on disk with the correct byte size when signed with an Arweave wallet", async () => {
          const { status, data } = await postStubDataItem(validDataItem);

          expect(status).to.equal(200);

          const {
            id,
            owner,
            dataCaches,
            deadlineHeight,
            fastFinalityIndexes,
            public: pubKey,
            signature,
            timestamp,
            version,
            winc,
          } = data;

          expect(id).to.equal("QpmY8mZmFEC8RxNsgbxSV6e36OF6quIYaPRKzvUco0o");
          expect(owner).to.equal("J40R1BgFSI1_7p25QW49T7P46BePJJnlDrsFGY1YWbM");
          expect(dataCaches).to.deep.equal(["arweave.net"]);
          expect(deadlineHeight).to.equal(700);
          expect(fastFinalityIndexes).to.deep.equal([]);
          expect(pubKey).to.equal(receiptSigningWallet.n);
          expect(signature).to.have.length(publicAndSigLength);
          // expect timestamp to be time since UNIX in MS (13 digits until Year 2038)
          expect(timestamp.toString()).to.have.length(13);
          expect(version).to.equal(receiptVersion);
          expect(winc).to.equal("500");

          expect(await verifyReceipt(data)).to.be.true;

          const fileStats = statSync(`temp/data/${id}`);
          expect(fileStats.size).to.equal(5);

          const rawFileStats = statSync(`temp/raw-data-item/${id}`);
          expect(rawFileStats.size).to.equal(1115);
        });

        it("returns the expected data result with a data item that contains empty tag names and values", async () => {
          const { status, data } = await postStubDataItem(
            stubDataItemWithEmptyStringsForTagNamesAndValues
          );

          expect(status).to.equal(200);

          const { id, owner } = data;

          expect(await verifyReceipt(data)).to.be.true;

          expect(id).to.equal("hSIHAdxTDUpW9oJb26nb2zhQkJn3yNBtTakMOwJuXC0");
          expect(owner).to.equal("jaxl_dxqJ00gEgQazGASFXVRvO4h-Q0_vnaLtuOUoWU");

          const fileStats = statSync(`temp/data/${id}`);
          expect(fileStats.size).to.equal(1024);

          const rawFileStats = statSync(`temp/raw-data-item/${id}`);
          expect(rawFileStats.size).to.equal(2325);
        });

        it("with a data item signed by a non allow listed wallet with balance", async function () {
          const tags = [{ name: "test", value: "value" }];
          const dataItem = await signDataItem(
            generateJunkDataItem(512, receiptSigningWallet, tags),
            receiptSigningWallet
          );

          const { data } = await postStubDataItem(dataItem);
          expect(await verifyReceipt(data)).to.be.true;

          expect(data).to.have.property("id");
          expect(data).to.have.property("owner");
          expect(data.dataCaches).to.deep.equal(["arweave.net"]);
        });

        it("returns the expected result for an empty data item", async () => {
          const signer = new ArweaveSigner(receiptSigningWallet);
          const createdDataItem = createData("", signer, {});
          await createdDataItem.sign(signer);

          const { data, status } = await axios.post(
            `${localTestUrl}/v1/tx`,
            createdDataItem.getRaw(),
            { headers: { ["Content-Type"]: octetStreamContentType } }
          );
          expect(await verifyReceipt(data)).to.be.true;

          expect(status).to.equal(200);
          expect(data.id).to.equal(createdDataItem.id);
        });

        it("returns the expected data result, a 200 status, the correct content-length, and the data item exists on disk with the correct byte size when signed with a Solana wallet", async () => {
          const { status, data } = await postStubDataItem(solanaDataItem);

          expect(status).to.equal(200);

          // cspell:disable
          const id = "35jbLhCGEfXLWe2H3VZr2i7f610kwP8Nkw-bFfx14-E";
          const owner = "VrRCYEai_2IveGr0lCiivqLGqenh4wpBnfZNgL-FtWY"; // cspell:enable

          expect(data.id).to.equal(id);
          expect(data.owner).to.equal(owner);
          expect(await verifyReceipt(data)).to.be.true;

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

          expect(data.id).to.equal(id);
          expect(data.owner).to.equal(owner);
          expect(await verifyReceipt(data)).to.be.true;

          const fileStats = statSync(`temp/data/${id}`);
          expect(fileStats.size).to.equal(5);

          const rawFileStats = statSync(`temp/raw-data-item/${id}`);
          expect(rawFileStats.size).to.equal(245);
        });

        it("with an invalid data item returns an error response", async () => {
          const { status, statusText, headers, data } = await postStubDataItem(
            invalidDataItem
          );

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

        it("with an invalid data item and 0 cost it does not refund balance", async () => {
          const refundSpy = stub(
            paymentService,
            "refundBalanceForData"
          ).resolves();

          const { status, statusText, headers, data } = await postStubDataItem(
            invalidDataItem
          );

          // Refund balance was not called
          expect(refundSpy.called).to.be.false;

          expect(status).to.equal(400);
          assertExpectedHeadersWithContentLength(headers, 18);

          const expectedData = "Invalid Data Item!";

          expect(statusText).to.equal(expectedData);
          expect(data).to.equal(expectedData);
        });
      });

      it("when reserveBalance throws return the correct error", async () => {
        const tags = [{ name: "test", value: "value" }];
        const dataItem = await signDataItem(
          generateJunkDataItem(512, receiptSigningWallet, tags),
          receiptSigningWallet
        );

        stub(paymentService, "reserveBalanceForData").throws();
        const { status, data } = await postStubDataItem(dataItem);

        expect(data).to.contain(
          "Upload Service is Unavailable. Payment Service is unreachable"
        );
        expect(status).to.equal(503);
      });

      it("with a data item signed by a non allow listed wallet without balance", async () => {
        const tags = [{ name: "test", value: "value" }];
        const dataItem = await signDataItem(
          generateJunkDataItem(512, receiptSigningWallet, tags),
          receiptSigningWallet
        );

        stub(paymentService, "checkBalanceForData").resolves({
          bytesCostInWinc: W("500"),
          userBalanceInWinc: W("20"),
          userHasSufficientBalance: false,
        });
        const { status, headers, data, statusText } = await postStubDataItem(
          dataItem
        );
        expect(data).to.equal("Insufficient balance");
        expect(statusText).to.equal("Insufficient balance");
        expect(status).to.equal(402);
        assertExpectedHeadersWithContentLength(headers, 20);
      });
    });

    describe("with a Koa server stubbed with a payment service that allows ARFS data", () => {
      const paymentService = new TurboPaymentService(true);
      before(() => {
        server = createServer({
          paymentService,
          getArweaveWallet: () => Promise.resolve(receiptSigningWallet),
          arweaveGateway,
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

        expect(data.id).to.equal(id);
        expect(data.owner).to.equal(owner);
        expect(await verifyReceipt(data)).to.be.true;

        const fileStats = statSync(`temp/data/${id}`);
        expect(fileStats.size).to.equal(155);

        const rawFileStats = statSync(`temp/raw-data-item/${id}`);
        expect(rawFileStats.size).to.equal(1464);
      });
    });
  });
});
