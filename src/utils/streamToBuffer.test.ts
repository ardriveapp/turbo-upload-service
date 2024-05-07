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

import "../bundles/assembleBundleHeader";
import "./objectStoreUtils";
import { streamIntoBufferAtOffset, streamToBuffer } from "./streamToBuffer";

describe("streamToBuffer function", () => {
  describe("without a size argument supplied", () => {
    it("should produce a buffer with the expected data from the provided stream", async () => {
      const sourceBuffer = Buffer.from("Don't let your dreams be dreams.");
      const sourceStream = Readable.from(sourceBuffer);
      const resultBuffer = await streamToBuffer(sourceStream);
      expect(resultBuffer).to.deep.equal(sourceBuffer);
    });
  });

  describe("with a size argument supplied", () => {
    it("should produce a buffer with the expected data from the provided stream", async () => {
      const sourceBuffer = Buffer.from("Don't let your dreams be dreams.");
      const sourceStream = Readable.from(sourceBuffer);
      const resultBuffer = await streamToBuffer(
        sourceStream,
        sourceBuffer.byteLength
      );
      expect(resultBuffer).to.deep.equal(sourceBuffer);
    });
  });
});

describe("streamIntoBufferAtOffset function", () => {
  describe("without an offset argument supplied", () => {
    it("should modify the input buffer correctly based on the data from the provided stream", async () => {
      const sourceBuffer = Buffer.from("Don't let your dreams be dreams.");
      const destBuffer = Buffer.alloc(sourceBuffer.byteLength);
      const sourceStream = Readable.from(sourceBuffer);
      await streamIntoBufferAtOffset(sourceStream, destBuffer);
      expect(destBuffer).to.deep.equal(sourceBuffer);
    });
  });

  describe("with an offset argument supplied", () => {
    it("should modify the input buffer correctly based on the data from the provided stream", async () => {
      const sourceBuffer = Buffer.from("Don't let your dreams be dreams.");
      const destBuffer = Buffer.alloc(2 * sourceBuffer.byteLength);
      const sourceStream = Readable.from(sourceBuffer);
      await streamIntoBufferAtOffset(
        sourceStream,
        destBuffer,
        sourceBuffer.byteLength
      );
      expect(destBuffer.subarray(sourceBuffer.byteLength)).to.deep.equal(
        sourceBuffer
      );
    });
  });

  it("should throw an error if the source buffer is too big for the destination buffer", async () => {
    const sourceBuffer = Buffer.from("Don't let your dreams be dreams.");
    const destBuffer = Buffer.alloc(sourceBuffer.byteLength - 1);
    const sourceStream = Readable.from(sourceBuffer);
    expect(async () => {
      await streamIntoBufferAtOffset(sourceStream, destBuffer);
    }).to.throw;
  });
});
