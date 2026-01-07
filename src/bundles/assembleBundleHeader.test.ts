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
import { Readable } from "stream";

import { stubDataItemRawSignatureReadStream } from "../../tests/stubs";
import { ByteCount } from "../types/types";
import { toB64Url } from "../utils/base64";
import { streamToBuffer } from "../utils/streamToBuffer";
import {
  BundleHeaderInfo,
  assembleBundleHeader,
  bundleHeaderInfoFromBuffer,
  parseBundleHeaderInfo,
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

describe("parseBundleHeaderInfo", () => {
  it("parses header from a single-chunk stream and leaves payload intact", async () => {
    const items: { dataItemRawId: Buffer; byteCount: ByteCount }[] = [
      { dataItemRawId: Buffer.alloc(32, 1), byteCount: 4 },
      { dataItemRawId: Buffer.alloc(32, 2), byteCount: 3 },
    ];

    const headerStream = await assembleBundleHeader(items);
    const headerBuf = await readAll(headerStream);

    const payloads = [Buffer.from("abcd"), Buffer.from("xyz")];
    const combined = Buffer.concat([headerBuf, ...payloads]);

    const stream = Readable.from([combined]);
    stream.pause();

    const { bundleHeaderInfo: info, rest } = await parseBundleHeaderInfo(
      stream
    );
    expect(info.numDataItems).to.equal(2);
    expect(info.dataItems.map((d) => d.size)).to.deep.equal([4, 3]);

    // consume remaining data from returned rest stream
    const restBuf = await readAll(rest);
    expect(restBuf).to.deep.equal(Buffer.concat(payloads));
  });

  it("parses header split across many small chunks", async () => {
    const items: { dataItemRawId: Buffer; byteCount: ByteCount }[] = [
      { dataItemRawId: Buffer.alloc(32, 3), byteCount: 5 },
      { dataItemRawId: Buffer.alloc(32, 4), byteCount: 2 },
      { dataItemRawId: Buffer.alloc(32, 5), byteCount: 1 },
    ];

    const headerStream = await assembleBundleHeader(items);
    const headerBuf = await readAll(headerStream);
    const payloads = [
      Buffer.alloc(5, 7),
      Buffer.alloc(2, 8),
      Buffer.alloc(1, 9),
    ];
    const combined = Buffer.concat([headerBuf, ...payloads]);

    // split into many tiny chunks
    const chunks: Buffer[] = [];
    for (let i = 0; i < combined.length; i += 3) {
      chunks.push(combined.slice(i, i + 3));
    }

    const stream = Readable.from(chunks);
    stream.pause();

    const { bundleHeaderInfo: info, rest } = await parseBundleHeaderInfo(
      stream
    );
    expect(info.numDataItems).to.equal(3);
    expect(info.dataItems.map((d) => d.size)).to.deep.equal([5, 2, 1]);

    const restBuf = await readAll(rest);
    expect(restBuf).to.deep.equal(Buffer.concat(payloads));
  });

  it("throws when stream ends prematurely", async () => {
    const items: { dataItemRawId: Buffer; byteCount: ByteCount }[] = [
      { dataItemRawId: Buffer.alloc(32, 6), byteCount: 10 },
    ];
    const headerStream = await assembleBundleHeader(items);
    const headerBuf = await readAll(headerStream);

    // give only part of the header (first 16 bytes)
    const partial = headerBuf.slice(0, 16);
    const stream = Readable.from([partial]);
    stream.pause();

    try {
      await parseBundleHeaderInfo(stream);
      throw new Error("Expected parseBundleHeaderInfo to throw");
    } catch (err) {
      expect(err).to.exist;
    }
  });
});

async function readAll(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  return new Promise((resolve, reject) => {
    const onData = (c: Buffer) => chunks.push(c);
    const onEnd = () => {
      stream.off("data", onData);
      resolve(Buffer.concat(chunks));
    };
    const onError = (e: Error) => {
      stream.off("data", onData);
      reject(e);
    };

    stream.on("data", onData);
    stream.once("end", onEnd);
    stream.once("error", onError);

    // Ensure the stream flows even if currently paused
    stream.resume();
  });
}
