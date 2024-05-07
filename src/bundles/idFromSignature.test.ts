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

import {
  stubDataItemRawReadStream,
  stubDataItemRawSignatureReadStream,
} from "../../tests/stubs";
import { fromB64Url, toB64Url } from "../utils/base64";
import {
  bufferIdFromBufferSignature,
  bufferIdFromReadableSignature,
} from "./idFromSignature";
import { StreamingDataItem } from "./streamingDataItem";

// cspell:disable
const expectedId = "QpmY8mZmFEC8RxNsgbxSV6e36OF6quIYaPRKzvUco0o"; // cspell:enable

describe("rawIdFromRawSignature function", () => {
  it("returns the expected rawId buffer when signature length is not specified", async () => {
    const rawDataItemSig = stubDataItemRawSignatureReadStream();

    const rawIdBuffer = await bufferIdFromReadableSignature(rawDataItemSig);

    expect(toB64Url(rawIdBuffer)).to.equal(expectedId);
  });

  it("returns the expected rawId buffer when signature length is specified", async () => {
    const rawDataItemSig = stubDataItemRawSignatureReadStream();

    const rawIdBuffer = await bufferIdFromReadableSignature(
      rawDataItemSig,
      512
    );

    expect(toB64Url(rawIdBuffer)).to.equal(expectedId);
  });
});

describe("rawIdFromSignature function", () => {
  it("returns the expected rawId buffer", async () => {
    const stubDataItemStream = stubDataItemRawReadStream();
    const streamingDataItem = new StreamingDataItem(stubDataItemStream);
    const signature = await streamingDataItem.getSignature();
    const rawIdBuffer = await bufferIdFromBufferSignature(
      fromB64Url(signature)
    );

    expect(toB64Url(rawIdBuffer)).to.equal(expectedId);
  });
});
