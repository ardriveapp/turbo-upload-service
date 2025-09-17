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
import { createReadStream, readFileSync } from "fs";

import { streamToBuffer } from "../utils/streamToBuffer";
import { InMemoryDataItem, StreamingDataItem } from "./streamingDataItem";

describe("createVerifiedDataItemStream function", () => {
  it("succeeds with the expected results for an Arweave data item", async () => {
    const dataItemPath = "tests/stubFiles/stub1115ByteDataItem";
    const dataItemStream = createReadStream(dataItemPath);
    const streamingDataItem = new StreamingDataItem(dataItemStream);
    expect(await streamingDataItem.getSignatureType()).to.equal(1);
    expect(await streamingDataItem.getSignature()).to.equal(
      "wUIlPaBflf54QyfiCkLnQcfakgcS5B4Pld-hlOJKyALY82xpAivoc0fxBJWjoeg3zy9aXz8WwCs_0t0MaepMBz2bQljRrVXnsyWUN-CYYfKv0RRglOl-kCmTiy45Ox13LPMATeJADFqkBoQKnGhyyxW81YfuPnVlogFWSz1XHQgHxrFMAeTe9epvBK8OCnYqDjch4pwyYUFrk48JFjHM3-I2kcQnm2dAFzFTfO-nnkdQ7ulP3eoAUr-W-KAGtPfWdJKFFgWFCkr_FuNyHYQScQo-FVOwIsvj_PVWEU179NwiqfkZtnN8VoBgCSxbL1Wmh4NYL-GsRbKz_94hpcj5RiIgq0_H5dzAp-bIb49M4SP-DcuIJ5oT2v2AfPWvznokDDVTeikQJxCD2n9usBOJRpLw_P724Yurbl30eNow0U-Jmrl8S6N64cjwKVLI-hBUfcpviksKEF5_I4XCyciW0TvZj1GxK6ET9lx0s6jFMBf27-GrFx6ZDJUBncX6w8nDvuL6A8TG_ILGNQU_EDoW7iil6NcHn5w11yS_yLkqG6dw_zuC1Vkg1tbcKY3703tmbF-jMEZUvJ6oN8vRwwodinJjzGdj7bxmkUPThwVWedCc8wCR3Ak4OkIGASLMUahSiOkYmELbmwq5II-1Txp2gDPjCpAf9gT6Iu0heAaXhjk"
    );
    expect(await streamingDataItem.getDataItemId()).to.equal(
      "QpmY8mZmFEC8RxNsgbxSV6e36OF6quIYaPRKzvUco0o"
    );
    expect(await streamingDataItem.getOwner()).to.equal(
      "0zBGbs8Y4wvdS58cAVyxp7mDffScOkbjh50ZrqnWKR_5NGwjezT6J40ejIg5cm1KnuDnw9OhvA7zO6sv1hEE6IaGNnNJWiXFecRMxCl7iw78frrT8xJvhBgtD4fBCV7eIvydqLoMl8K47sacTUxEGseaLfUdYVJ5CSock5SktEEdqqoe3MAso7x4ZsB5CGrbumNcCTifr2mMsrBytocSoHuiCEi7-Nwv4CqzB6oqymBtEECmKYWdINnNQHVyKK1l0XP1hzByHv_WmhouTPos9Y77sgewZrvLF-dGPNWSc6LaYGy5IphCnq9ACFrEbwkiCRgZHnKsRFH0dfGaCgGb3GZE-uspmICJokJ9CwDPDJoxkCBEF0tcLSIA9_ofiJXaZXbrZzu3TUXWU3LQiTqYr4j5gj_7uTclewbyZSsY-msfbFQlaACc02nQkEkr4pMdpEOdAXjWP6qu7AJqoBPNtDPBqWbdfsLXgyK90NbYmf3x4giAmk8L9REy7SGYugG4VyqG39pNQy_hdpXdcfyE0ftCr5tSHVpMreJ0ni7v3IDCbjZFcvcHp0H6f6WPfNCoHg1BM6rHUqkXWd84gdHUzo9LTGq9-7wSBCizpcc_12_I-6yvZsROJvdfYOmjPnd5llefa_X3X1dVm5FPYFIabydGlh1Vs656rRu4dzeEQwc"
    );
    expect(await streamingDataItem.getOwnerAddress()).to.equal(
      "J40R1BgFSI1_7p25QW49T7P46BePJJnlDrsFGY1YWbM"
    );
    expect(await streamingDataItem.getTargetFlag()).to.equal(0);
    expect(await streamingDataItem.getTarget()).to.be.undefined;
    expect(await streamingDataItem.getAnchorFlag()).to.equal(0);
    expect(await streamingDataItem.getAnchor()).to.be.undefined;
    expect(await streamingDataItem.getNumTags()).to.equal(3);
    expect(await streamingDataItem.getNumTagsBytes()).to.equal(66);
    expect(await streamingDataItem.getTags()).to.eql([
      { name: "Content-Type", value: "text/plain" },
      { name: "App-Name", value: "ArDrive-CLI" },
      { name: "App-Version", value: "1.21.0" },
    ]);
    expect(
      (
        await streamToBuffer(await streamingDataItem.getPayloadStream())
      ).toString()
    ).to.equal("5670\n");
    expect(await streamingDataItem.getPayloadSize()).to.equal(5);
    expect(await streamingDataItem.isValid()).to.be.true;
    // TODO: CONSIDER USING DIFFERENT CHUNK SIZES TO EXECUTE DIFFERENT PATHS DURING TESTS
  });

  it("succeeds with a Solana-signed data item", async () => {
    const dataItemPath = "tests/stubFiles/stubSolanaDataItem";
    const dataItemStream = createReadStream(dataItemPath);
    const streamingDataItem = new StreamingDataItem(dataItemStream);
    expect(await streamingDataItem.getSignatureType()).to.equal(2);
    expect(await streamingDataItem.getSignature()).to.equal(
      "1iio1i4GRP-Dy8KtA-2kz4LcV6Y-Dy8fCwQ_KFsbFz5rukGV5byOPSkq4v7wyk0ALSxgUOxiXyWP4YnhDC0dCg"
    );
    expect(await streamingDataItem.getDataItemId()).to.equal(
      "35jbLhCGEfXLWe2H3VZr2i7f610kwP8Nkw-bFfx14-E"
    );
    expect(await streamingDataItem.getOwner()).to.equal(
      "C9XVj4dTM9Z_xAqJdaC7jNgw_mhs3HqyrD468El-AEc"
    );
    expect(await streamingDataItem.getOwnerAddress()).to.equal(
      "VrRCYEai_2IveGr0lCiivqLGqenh4wpBnfZNgL-FtWY"
    );
    expect(await streamingDataItem.getTargetFlag()).to.equal(1);
    expect(await streamingDataItem.getTarget()).to.equal(
      "1234567890123456789012345678901234567890abc"
    );
    expect(await streamingDataItem.getAnchorFlag()).to.equal(1);
    expect(await streamingDataItem.getAnchor()).to.equal(
      "abcdefghijklmnopqrstuvwxyz123456"
    );
    expect(await streamingDataItem.getNumTags()).to.equal(1);
    expect(await streamingDataItem.getNumTagsBytes()).to.equal(26);
    expect(await streamingDataItem.getTags()).to.eql([
      { name: "Content-Type", value: "text/plain" },
    ]);
    expect(
      (
        await streamToBuffer(await streamingDataItem.getPayloadStream())
      ).toString()
    ).to.equal("hello");
    expect(await streamingDataItem.getPayloadSize()).to.equal(5);
    expect(await streamingDataItem.isValid()).to.be.true;
  });

  it("succeeds with an Ethereum-signed data item", async () => {
    const dataItemPath = "tests/stubFiles/stubEthereumDataItem";
    const dataItemStream = createReadStream(dataItemPath);
    const streamingDataItem = new StreamingDataItem(dataItemStream);
    expect(await streamingDataItem.getSignatureType()).to.equal(3);
    expect(await streamingDataItem.getSignature()).to.equal(
      "ftcvdr3BGbCEvpHBJhTSTAhGXIBmZgrtnp5Bcne1bYsbuiyYk4yckCnvKdOJ9ZILS0gGiYkjq3803CM8l15VAhw"
    );
    expect(await streamingDataItem.getDataItemId()).to.equal(
      "7j-sF0lsslGVZ8lhEGXe5CtueB4iRYM3_oZ9m4GY_40"
    );
    expect(await streamingDataItem.getOwner()).to.equal(
      "BBzWLFGOYVUfTMwXTVpS9KNDupL5HAvPxOBxVzEwpjFjuMkxI1IPlJ7mxHYT5e1jIMicuo7WtMkYi4Q_V_DmI4Q"
    );
    expect(await streamingDataItem.getOwnerAddress()).to.equal(
      "xsi06LVwuRe2SaNFo0Yc1UtF3GSyi-GtzUtTkjLXrEw"
    );
    expect(await streamingDataItem.getTargetFlag()).to.equal(1);
    expect(await streamingDataItem.getTarget()).to.equal(
      "1234567890123456789012345678901234567890abc"
    );
    expect(await streamingDataItem.getAnchorFlag()).to.equal(1);
    expect(await streamingDataItem.getAnchor()).to.equal(
      "abcdefghijklmnopqrstuvwxyz123456"
    );
    expect(await streamingDataItem.getNumTags()).to.equal(1);
    expect(await streamingDataItem.getNumTagsBytes()).to.equal(26);
    expect(await streamingDataItem.getTags()).to.eql([
      { name: "Content-Type", value: "text/plain" },
    ]);
    expect(
      (
        await streamToBuffer(await streamingDataItem.getPayloadStream())
      ).toString()
    ).to.equal("hello");
    expect(await streamingDataItem.getPayloadSize()).to.equal(5);
    expect(await streamingDataItem.isValid()).to.be.true;
  });
});

describe("InMemoryDataItem", () => {
  it("succeeds with the expected results for an Arweave data item", async () => {
    const dataItemPath = "tests/stubFiles/stub1115ByteDataItem";
    const dataItemBuffer = readFileSync(dataItemPath);
    const inMemoryDataItem = new InMemoryDataItem(dataItemBuffer);
    expect(await inMemoryDataItem.getSignatureType()).to.equal(1);
    expect(await inMemoryDataItem.getSignature()).to.equal(
      "wUIlPaBflf54QyfiCkLnQcfakgcS5B4Pld-hlOJKyALY82xpAivoc0fxBJWjoeg3zy9aXz8WwCs_0t0MaepMBz2bQljRrVXnsyWUN-CYYfKv0RRglOl-kCmTiy45Ox13LPMATeJADFqkBoQKnGhyyxW81YfuPnVlogFWSz1XHQgHxrFMAeTe9epvBK8OCnYqDjch4pwyYUFrk48JFjHM3-I2kcQnm2dAFzFTfO-nnkdQ7ulP3eoAUr-W-KAGtPfWdJKFFgWFCkr_FuNyHYQScQo-FVOwIsvj_PVWEU179NwiqfkZtnN8VoBgCSxbL1Wmh4NYL-GsRbKz_94hpcj5RiIgq0_H5dzAp-bIb49M4SP-DcuIJ5oT2v2AfPWvznokDDVTeikQJxCD2n9usBOJRpLw_P724Yurbl30eNow0U-Jmrl8S6N64cjwKVLI-hBUfcpviksKEF5_I4XCyciW0TvZj1GxK6ET9lx0s6jFMBf27-GrFx6ZDJUBncX6w8nDvuL6A8TG_ILGNQU_EDoW7iil6NcHn5w11yS_yLkqG6dw_zuC1Vkg1tbcKY3703tmbF-jMEZUvJ6oN8vRwwodinJjzGdj7bxmkUPThwVWedCc8wCR3Ak4OkIGASLMUahSiOkYmELbmwq5II-1Txp2gDPjCpAf9gT6Iu0heAaXhjk"
    );
    expect(await inMemoryDataItem.getDataItemId()).to.equal(
      "QpmY8mZmFEC8RxNsgbxSV6e36OF6quIYaPRKzvUco0o"
    );
    expect(await inMemoryDataItem.getOwner()).to.equal(
      "0zBGbs8Y4wvdS58cAVyxp7mDffScOkbjh50ZrqnWKR_5NGwjezT6J40ejIg5cm1KnuDnw9OhvA7zO6sv1hEE6IaGNnNJWiXFecRMxCl7iw78frrT8xJvhBgtD4fBCV7eIvydqLoMl8K47sacTUxEGseaLfUdYVJ5CSock5SktEEdqqoe3MAso7x4ZsB5CGrbumNcCTifr2mMsrBytocSoHuiCEi7-Nwv4CqzB6oqymBtEECmKYWdINnNQHVyKK1l0XP1hzByHv_WmhouTPos9Y77sgewZrvLF-dGPNWSc6LaYGy5IphCnq9ACFrEbwkiCRgZHnKsRFH0dfGaCgGb3GZE-uspmICJokJ9CwDPDJoxkCBEF0tcLSIA9_ofiJXaZXbrZzu3TUXWU3LQiTqYr4j5gj_7uTclewbyZSsY-msfbFQlaACc02nQkEkr4pMdpEOdAXjWP6qu7AJqoBPNtDPBqWbdfsLXgyK90NbYmf3x4giAmk8L9REy7SGYugG4VyqG39pNQy_hdpXdcfyE0ftCr5tSHVpMreJ0ni7v3IDCbjZFcvcHp0H6f6WPfNCoHg1BM6rHUqkXWd84gdHUzo9LTGq9-7wSBCizpcc_12_I-6yvZsROJvdfYOmjPnd5llefa_X3X1dVm5FPYFIabydGlh1Vs656rRu4dzeEQwc"
    );
    expect(await inMemoryDataItem.getOwnerAddress()).to.equal(
      "J40R1BgFSI1_7p25QW49T7P46BePJJnlDrsFGY1YWbM"
    );
    expect(await inMemoryDataItem.getTarget()).to.be.undefined;
    expect(await inMemoryDataItem.getAnchor()).to.be.undefined;
    expect(await inMemoryDataItem.getNumTags()).to.equal(3);
    expect(await inMemoryDataItem.getNumTagsBytes()).to.equal(66);
    expect(await inMemoryDataItem.getTags()).to.eql([
      { name: "Content-Type", value: "text/plain" },
      { name: "App-Name", value: "ArDrive-CLI" },
      { name: "App-Version", value: "1.21.0" },
    ]);
    expect(
      (
        await streamToBuffer(await inMemoryDataItem.getPayloadStream())
      ).toString()
    ).to.equal("5670\n");
    expect(await inMemoryDataItem.getPayloadSize()).to.equal(5);
    expect(await inMemoryDataItem.isValid()).to.be.true;
  });

  it("succeeds with a Solana-signed data item", async () => {
    const dataItemPath = "tests/stubFiles/stubSolanaDataItem";
    const dataItemBuffer = readFileSync(dataItemPath);
    const inMemoryDataItem = new InMemoryDataItem(dataItemBuffer);
    expect(await inMemoryDataItem.getSignatureType()).to.equal(2);
    expect(await inMemoryDataItem.getSignature()).to.equal(
      "1iio1i4GRP-Dy8KtA-2kz4LcV6Y-Dy8fCwQ_KFsbFz5rukGV5byOPSkq4v7wyk0ALSxgUOxiXyWP4YnhDC0dCg"
    );
    expect(await inMemoryDataItem.getDataItemId()).to.equal(
      "35jbLhCGEfXLWe2H3VZr2i7f610kwP8Nkw-bFfx14-E"
    );
    expect(await inMemoryDataItem.getOwner()).to.equal(
      "C9XVj4dTM9Z_xAqJdaC7jNgw_mhs3HqyrD468El-AEc"
    );
    expect(await inMemoryDataItem.getOwnerAddress()).to.equal(
      "VrRCYEai_2IveGr0lCiivqLGqenh4wpBnfZNgL-FtWY"
    );
    expect(await inMemoryDataItem.getTarget()).to.equal(
      "1234567890123456789012345678901234567890abc"
    );
    expect(await inMemoryDataItem.getAnchor()).to.equal(
      "abcdefghijklmnopqrstuvwxyz123456"
    );
    expect(await inMemoryDataItem.getNumTags()).to.equal(1);
    expect(await inMemoryDataItem.getNumTagsBytes()).to.equal(26);
    expect(await inMemoryDataItem.getTags()).to.eql([
      { name: "Content-Type", value: "text/plain" },
    ]);
    expect(
      (
        await streamToBuffer(await inMemoryDataItem.getPayloadStream())
      ).toString()
    ).to.equal("hello");
    expect(await inMemoryDataItem.getPayloadSize()).to.equal(5);
    expect(await inMemoryDataItem.isValid()).to.be.true;
  });

  it("succeeds with an Ethereum-signed data item", async () => {
    const dataItemPath = "tests/stubFiles/stubEthereumDataItem";
    const dataItemBuffer = readFileSync(dataItemPath);
    const inMemoryDataItem = new InMemoryDataItem(dataItemBuffer);
    expect(await inMemoryDataItem.getSignatureType()).to.equal(3);
    expect(await inMemoryDataItem.getSignature()).to.equal(
      "ftcvdr3BGbCEvpHBJhTSTAhGXIBmZgrtnp5Bcne1bYsbuiyYk4yckCnvKdOJ9ZILS0gGiYkjq3803CM8l15VAhw"
    );
    expect(await inMemoryDataItem.getDataItemId()).to.equal(
      "7j-sF0lsslGVZ8lhEGXe5CtueB4iRYM3_oZ9m4GY_40"
    );
    expect(await inMemoryDataItem.getOwner()).to.equal(
      "BBzWLFGOYVUfTMwXTVpS9KNDupL5HAvPxOBxVzEwpjFjuMkxI1IPlJ7mxHYT5e1jIMicuo7WtMkYi4Q_V_DmI4Q"
    );
    expect(await inMemoryDataItem.getOwnerAddress()).to.equal(
      "xsi06LVwuRe2SaNFo0Yc1UtF3GSyi-GtzUtTkjLXrEw"
    );
    expect(await inMemoryDataItem.getTarget()).to.equal(
      "1234567890123456789012345678901234567890abc"
    );
    expect(await inMemoryDataItem.getAnchor()).to.equal(
      "abcdefghijklmnopqrstuvwxyz123456"
    );
    expect(await inMemoryDataItem.getNumTags()).to.equal(1);
    expect(await inMemoryDataItem.getNumTagsBytes()).to.equal(26);
    expect(await inMemoryDataItem.getTags()).to.eql([
      { name: "Content-Type", value: "text/plain" },
    ]);
    expect(
      (
        await streamToBuffer(await inMemoryDataItem.getPayloadStream())
      ).toString()
    ).to.equal("hello");
    expect(await inMemoryDataItem.getPayloadSize()).to.equal(5);
    expect(await inMemoryDataItem.isValid()).to.be.true;
  });
});
