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

import { stubDataItemRawSignatureReadStream } from "../../tests/stubs";
import { toB64Url } from "../utils/base64";
import { streamToBuffer } from "../utils/streamToBuffer";
import {
  BundleHeaderInfo,
  assembleBundleHeader,
  bundleHeaderInfoFromBuffer,
  totalBundleSizeFromHeaderInfo,
} from "./assembleBundleHeader";
import { bufferIdFromReadableSignature } from "./idFromSignature";

describe("assembleBundleHeader", () => {
  it("returns the expected bundleHeader when provided a single data item's raw signature", async () => {
    const dataItemRawSig = stubDataItemRawSignatureReadStream();

    const stubDataItemRawId = await bufferIdFromReadableSignature(
      dataItemRawSig,
      512
    );
    const bundleHeaderStream = await assembleBundleHeader([
      { dataItemRawId: stubDataItemRawId, byteCount: 1115 },
    ]);

    expect(toB64Url(await streamToBuffer(bundleHeaderStream))).to.equal(
      "AQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABbBAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEKZmPJmZhRAvEcTbIG8Ulent-jheqriGGj0Ss71HKNK"
    );
  });
});

describe("bundleHeaderInfoFromBuffer", () => {
  it("returns the correct data from test data", async () => {
    // TODO: Use a bundle header with multiple data items
    const dataItemRawSig = stubDataItemRawSignatureReadStream();

    const stubDataItemRawId = await bufferIdFromReadableSignature(
      dataItemRawSig,
      512
    );
    const bundleHeaderStream = await assembleBundleHeader([
      { dataItemRawId: stubDataItemRawId, byteCount: 1115 },
    ]);
    const bundleHeaderInfo = bundleHeaderInfoFromBuffer(
      await streamToBuffer(bundleHeaderStream)
    );
    expect(bundleHeaderInfo).to.deep.equal({
      numDataItems: 1,
      dataItems: [
        {
          size: 1115,
          id: "QpmY8mZmFEC8RxNsgbxSV6e36OF6quIYaPRKzvUco0o",
          dataOffset: 96,
        },
      ],
    });
  });
});

describe("totalBundleSizeFromHeaderInfo function", () => {
  it("returns the expected value", () => {
    const stubHeaderInfo: BundleHeaderInfo = {
      numDataItems: 2,
      dataItems: [
        {
          size: 123,
          id: "stubId",
          dataOffset: 96,
        },
        {
          size: 456,
          id: "stubId2",
          dataOffset: 96 + 123,
        },
      ],
    };
    expect(totalBundleSizeFromHeaderInfo(stubHeaderInfo)).to.equal(739);
  });
});
