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
import { rmSync, writeFileSync } from "fs";

import FileDataItem from "../../src/bundles/dataItem";
import { fromB64Url } from "../../src/utils/base64";
import {
  generateJunkDataItem,
  signDataItem,
  toDataItem,
} from "./dataItemHelpers";

describe("toDataItem", async () => {
  after(() => {
    rmSync("hereData");
  });
  const jwk = await Arweave.crypto.generateJWK();
  it("should create a data item from a string, JWK, and tags", async () => {
    const data = "hello world";
    const tags = [{ name: "test", value: "value" }];
    const dataItem = toDataItem(data, jwk, tags);

    expect(dataItem).to.be.instanceOf(Buffer);
    expect(dataItem.length).to.be.above(0);

    writeFileSync("hereData", dataItem);
    const fileData = new FileDataItem("hereData");

    expect(fromB64Url(await fileData.data()).toString()).to.equal(data);
  });

  it("should create a data item of the specified size with JWK, and tags", async () => {
    const tags = [{ name: "test", value: "value" }];
    const dataItem = generateJunkDataItem(300, jwk, tags);

    expect(dataItem).to.be.instanceOf(Buffer);
    expect(dataItem.length).to.be.above(0);

    writeFileSync("hereData", dataItem);
    const fileData = new FileDataItem("hereData");

    expect(fromB64Url(await fileData.data()).length).to.equal(300 * 1024);
  });

  it("should throw an error if the JWK owner is not the correct length", async () => {
    const data = "hello world";
    const badJwk = {
      ...jwk,
      n: jwk.n + jwk.n,
    };

    const tags = [{ name: "test", value: "value" }];
    expect(() => toDataItem(data, badJwk, tags)).to.throw();
  });

  it("should create a data item with the correct owner", async () => {
    const data = "hello world";
    const tags = [{ name: "test", value: "value" }];
    const dataItem = toDataItem(data, jwk, tags);

    writeFileSync("hereData", dataItem);
    const fileData = new FileDataItem("hereData");

    expect(await fileData.owner()).to.equal(jwk.n);
  });
});

describe("signDataItem", () => {
  after(() => {
    rmSync("hereData");
  });
  it("should sign a data item with a JWK", async function () {
    const data = "hello world";
    const jwk = await Arweave.crypto.generateJWK();
    const tags = [{ name: "test", value: "value" }];
    const dataItem = toDataItem(data, jwk, tags);

    const signedDataItem = await signDataItem(dataItem, jwk);

    expect(signedDataItem).to.be.instanceOf(Buffer);
    expect(signedDataItem.length).to.be.above(0);

    writeFileSync("hereData", signedDataItem);
    const fileData = new FileDataItem("hereData");

    const isValid = await fileData.isValid();
    expect(isValid).to.equal(true);
  });
});
