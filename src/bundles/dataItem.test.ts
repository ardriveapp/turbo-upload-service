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
import { expect } from "chai";

import { stubDataItemBase64Signature } from "../../tests/stubs";
import FileDataItem from "./dataItem";

describe("FileDataItem class", () => {
  const stubDataItem = new FileDataItem("tests/stubFiles/stub1115ByteDataItem");

  it("isValid method returns true with a valid data item", async () => {
    expect(await stubDataItem.isValid()).to.be.true;
  });

  it("isValid method returns false with a invalid data item", async () => {
    const stubDataItem = new FileDataItem(
      "tests/stubFiles/stubInvalidDataItem"
    );
    expect(await stubDataItem.isValid()).to.be.false;
  });

  it("size method returns the correct size", async () => {
    expect(await stubDataItem.size()).to.equal(1115);
  });

  const arweaveSignatureType = 1;
  it("signatureType method returns the correct type", async () => {
    expect(await stubDataItem.signatureType()).to.equal(arweaveSignatureType);
  });

  it("signature method returns the correct signature", async () => {
    expect(await stubDataItem.signature()).to.equal(
      stubDataItemBase64Signature
    );
  });

  it("owner method returns the correct owner", async () => {
    expect(await stubDataItem.owner()).to.equal(
      "0zBGbs8Y4wvdS58cAVyxp7mDffScOkbjh50ZrqnWKR_5NGwjezT6J40ejIg5cm1KnuDnw9OhvA7zO6sv1hEE6IaGNnNJWiXFecRMxCl7iw78frrT8xJvhBgtD4fBCV7eIvydqLoMl8K47sacTUxEGseaLfUdYVJ5CSock5SktEEdqqoe3MAso7x4ZsB5CGrbumNcCTifr2mMsrBytocSoHuiCEi7-Nwv4CqzB6oqymBtEECmKYWdINnNQHVyKK1l0XP1hzByHv_WmhouTPos9Y77sgewZrvLF-dGPNWSc6LaYGy5IphCnq9ACFrEbwkiCRgZHnKsRFH0dfGaCgGb3GZE-uspmICJokJ9CwDPDJoxkCBEF0tcLSIA9_ofiJXaZXbrZzu3TUXWU3LQiTqYr4j5gj_7uTclewbyZSsY-msfbFQlaACc02nQkEkr4pMdpEOdAXjWP6qu7AJqoBPNtDPBqWbdfsLXgyK90NbYmf3x4giAmk8L9REy7SGYugG4VyqG39pNQy_hdpXdcfyE0ftCr5tSHVpMreJ0ni7v3IDCbjZFcvcHp0H6f6WPfNCoHg1BM6rHUqkXWd84gdHUzo9LTGq9-7wSBCizpcc_12_I-6yvZsROJvdfYOmjPnd5llefa_X3X1dVm5FPYFIabydGlh1Vs656rRu4dzeEQwc"
    );
  });

  it("target method returns the correct target", async () => {
    expect(await stubDataItem.target()).to.equal("");
  });

  it("anchor method returns the correct anchor", async () => {
    expect(await stubDataItem.anchor()).to.equal("");
  });

  it("tags method returns the correct tags", async () => {
    expect(JSON.parse(JSON.stringify(await stubDataItem.tags()))).to.deep.equal(
      [
        { name: "Content-Type", value: "text/plain" },
        { name: "App-Name", value: "ArDrive-CLI" },
        { name: "App-Version", value: "1.21.0" },
      ]
    );
  });

  it("data method returns the correct data", async () => {
    expect(await stubDataItem.data()).to.equal("NTY3MAo");
  });
});
