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
  arioProcesses,
  dedicatedBundleTypes,
  rePostDataItemThresholdNumberOfBlocks,
} from "../constants";
import { ParsedDataItemHeader } from "../types/types";
import {
  filterKeysFromObject,
  generateArrayChunks,
  getByteCountBasedRePackThresholdBlockCount,
  getErrorCodeFromErrorObject,
  getPremiumFeatureType,
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

describe("getPremiumFeatureType function", () => {
  const signatureType = 1;
  const ownerPublicAddress = "0x1234567890abcdef";
  it("returns the correct premium feature type for an ArDrive upload", async () => {
    const tags = [{ name: "App-Name", value: "ArDrive-CLI" }];

    const result = getPremiumFeatureType(
      ownerPublicAddress,
      tags,
      signatureType,
      []
    );

    expect(result).to.equal("ardrive_dedicated_bundles");
  });

  it("returns the default premium feature type for an unknown upload", async () => {
    expect(
      getPremiumFeatureType(
        ownerPublicAddress,
        [{ name: "App-Name", value: "UnknownApp" }],
        signatureType,
        []
      )
    ).to.equal("default");
  });

  it("returns the correct premium feature type for a Warp upload", async () => {
    const warpWalletAddress =
      dedicatedBundleTypes["warp_dedicated_bundles"].allowedWallets[0];
    const tags = [{ name: "Sequencer", value: "Warp" }];

    const result = getPremiumFeatureType(
      warpWalletAddress,
      tags,
      signatureType,
      []
    );

    expect(result).to.equal("warp_dedicated_bundles");
  });

  it("returns the correct premium feature type for a Redstone upload", async () => {
    const redstoneWalletAddress =
      dedicatedBundleTypes["redstone_oracle_dedicated_bundles"]
        .allowedWallets[0];
    const tags = [{ name: "Sequencer", value: "Redstone" }];

    const result = getPremiumFeatureType(
      redstoneWalletAddress,
      tags,
      signatureType,
      []
    );

    expect(result).to.equal("redstone_oracle_dedicated_bundles");
  });

  const aoAuthoritySignerAddress =
    dedicatedBundleTypes["ao_dedicated_bundles"].allowedWallets[0];

  it("returns the correct premium feature type for an AO upload", async () => {
    expect(
      getPremiumFeatureType(aoAuthoritySignerAddress, [], signatureType, [])
    ).to.equal("ao_dedicated_bundles");
  });

  it("returns the correct premium feature type for an AO upload containing an AR.IO Process tag", async () => {
    expect(
      getPremiumFeatureType(
        aoAuthoritySignerAddress,
        [{ name: "Process", value: arioProcesses[0] }],
        signatureType,
        []
      )
    ).to.equal("ario_dedicated_bundles");
  });

  it("returns the AR.IO Network premium feature type for an Action: Eval upload where the target is an AR.IO Network Process", async () => {
    expect(
      getPremiumFeatureType(
        aoAuthoritySignerAddress,
        [{ name: "Action", value: "Eval" }],
        signatureType,
        [],
        arioProcesses[0]
      )
    ).to.equal("ario_dedicated_bundles");
  });

  it("returns the AR.IO Network premium feature type when an AO upload has nested headers that contain an AR.IO Network Process tag", async () => {
    expect(
      getPremiumFeatureType(
        aoAuthoritySignerAddress,
        [],
        signatureType,
        [
          { tags: [{ name: "Process", value: arioProcesses[0] }] },
        ] as ParsedDataItemHeader[] // Mocking nested headers
      )
    ).to.equal("ario_dedicated_bundles");
  });

  it("returns the AR.IO Network premium feature type when an AO upload has nested headers that contain an AR.IO Network Process as target", async () => {
    expect(
      getPremiumFeatureType(
        aoAuthoritySignerAddress,
        [],
        signatureType,
        [{ target: arioProcesses[0] }] as ParsedDataItemHeader[] // Mocking nested headers
      )
    ).to.equal("ario_dedicated_bundles");
  });

  describe("getErrorCodeFromErrorObject function", () => {
    it("returns the error code when error is an object with a string code property", () => {
      const error = { code: "ENOENT", message: "File not found" };
      expect(getErrorCodeFromErrorObject(error)).to.equal("ENOENT");
    });

    it("returns 'unknown' when error is null", () => {
      expect(getErrorCodeFromErrorObject(null)).to.equal("unknown");
    });

    it("returns 'unknown' when error is undefined", () => {
      expect(getErrorCodeFromErrorObject(undefined)).to.equal("unknown");
    });

    it("returns 'unknown' when error is not an object", () => {
      expect(getErrorCodeFromErrorObject("string error")).to.equal("unknown");
      expect(getErrorCodeFromErrorObject(123)).to.equal("unknown");
      expect(getErrorCodeFromErrorObject(true)).to.equal("unknown");
    });

    it("returns 'unknown' when error object does not have a code property", () => {
      const error = { message: "Some error", stack: "stack trace" };
      expect(getErrorCodeFromErrorObject(error)).to.equal("unknown");
    });

    it("returns 'unknown' when error object has a code property that is not a string", () => {
      const errorWithNumberCode = { code: 404, message: "Not found" };
      expect(getErrorCodeFromErrorObject(errorWithNumberCode)).to.equal(
        "unknown"
      );

      const errorWithBooleanCode = { code: true, message: "Some error" };
      expect(getErrorCodeFromErrorObject(errorWithBooleanCode)).to.equal(
        "unknown"
      );

      const errorWithObjectCode = {
        code: { nested: "value" },
        message: "Some error",
      };
      expect(getErrorCodeFromErrorObject(errorWithObjectCode)).to.equal(
        "unknown"
      );
    });

    it("returns the error code when error is an Error instance with a string code property", () => {
      const error = new Error("File system error");
      (error as any).code = "EACCES";
      expect(getErrorCodeFromErrorObject(error)).to.equal("EACCES");
    });

    it("returns 'unknown' when error is an Error instance without a code property", () => {
      const error = new Error("Generic error");
      expect(getErrorCodeFromErrorObject(error)).to.equal("unknown");
    });

    it("handles edge cases with empty string code", () => {
      const error = { code: "", message: "Empty code error" };
      expect(getErrorCodeFromErrorObject(error)).to.equal("");
    });

    it("handles complex error objects with additional properties", () => {
      const error = {
        code: "CUSTOM_ERROR",
        message: "Custom error message",
        stack: "Error stack trace",
        details: { nested: "information" },
        timestamp: new Date(),
      };
      expect(getErrorCodeFromErrorObject(error)).to.equal("CUSTOM_ERROR");
    });
  });
});
