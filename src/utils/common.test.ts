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

import { rePostDataItemThresholdNumberOfBlocks } from "../constants";
import {
  filterKeysFromObject,
  generateArrayChunks,
  getByteCountBasedRePackThresholdBlockCount,
} from "./common";

describe("filterKeysFromObject function", () => {
  it("filters top level keys from objects as expected", () => {
    const testObject = {
      ["Test Key 1"]: { "Test Key 1": 42 },
      testKey2: ["string", 22],
      testKeyThree: "two words",
    };

    const filteredResult = filterKeysFromObject(testObject, [
      "Test Key 1",
      "testKeyThree",
    ]);

    expect(filteredResult).to.deep.equal({
      testKey2: ["string", 22],
    });
  });
});

describe("The generateArrayChunks function", () => {
  it("throws when a negative chunk size is supplied", () => {
    expect(() => [...generateArrayChunks([], -1)]).to.throw();
  });

  it("throws when a zero chunk size is supplied", () => {
    expect(() => [...generateArrayChunks([], 0)]).to.throw();
  });

  it("throws when a non-integer chunk size is supplied", () => {
    expect(() => [...generateArrayChunks([], 1.5)]).to.throw();
  });

  it("returns the expected value when supplied an empty array", () => {
    expect([...generateArrayChunks([], 1)]).to.deep.equal([]);
  });

  it("returns the same array when the array length is less than the chunk size", () => {
    expect([...generateArrayChunks([1, 2, 3], 4)]).to.deep.equal([[1, 2, 3]]);
  });

  it("returns the same array when the array length is equal to the chunk size", () => {
    expect([...generateArrayChunks([1, 2, 3], 3)]).to.deep.equal([[1, 2, 3]]);
  });

  it("returns the expected value when supplied an array with a length that is a multiple of the chunk size", () => {
    expect([...generateArrayChunks([1, 2, 3, 4, 5, 6], 3)]).to.deep.equal([
      [1, 2, 3],
      [4, 5, 6],
    ]);
  });

  it("returns the expected value when supplied an array with a length that is not a multiple of the chunk size", () => {
    expect([...generateArrayChunks([1, 2, 3, 4, 5, 6, 7], 3)]).to.deep.equal([
      [1, 2, 3],
      [4, 5, 6],
      [7],
    ]);
  });

  it("returns the expected value when the chunk size is 1", () => {
    expect([...generateArrayChunks([1, 2, 3], 1)]).to.deep.equal([
      [1],
      [2],
      [3],
    ]);
  });
});

describe("getByteCountBasedRePackThresholdBlockCount", () => {
  it("returns the correct block count for a 500 MiB payload", () => {
    expect(
      getByteCountBasedRePackThresholdBlockCount(500 * 1024 * 1024)
    ).to.equal(rePostDataItemThresholdNumberOfBlocks);
  });

  it("returns the correct block count for a 1 GiB payload", () => {
    expect(
      getByteCountBasedRePackThresholdBlockCount(1024 * 1024 * 1024)
    ).to.equal(rePostDataItemThresholdNumberOfBlocks * 1.5);
  });

  it("returns the correct block count for a 5 GiB payload", () => {
    expect(
      getByteCountBasedRePackThresholdBlockCount(5 * 1024 * 1024 * 1024)
    ).to.equal(rePostDataItemThresholdNumberOfBlocks * 2);
  });

  it("returns the correct block count for a 10 GiB payload", () => {
    expect(
      getByteCountBasedRePackThresholdBlockCount(10 * 1024 * 1024 * 1024)
    ).to.equal(rePostDataItemThresholdNumberOfBlocks * 3);
  });

  it("returns the correct block count for a 20 GiB payload", () => {
    expect(
      getByteCountBasedRePackThresholdBlockCount(20 * 1024 * 1024 * 1024)
    ).to.equal(rePostDataItemThresholdNumberOfBlocks * 4);
  });

  it("returns the correct block count for a 30 GiB payload", () => {
    expect(
      getByteCountBasedRePackThresholdBlockCount(30 * 1024 * 1024 * 1024)
    ).to.equal(rePostDataItemThresholdNumberOfBlocks * 5);
  });
});
