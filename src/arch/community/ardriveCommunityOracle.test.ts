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

import { stubCommunityContract } from "../../../tests/stubs";
import { W } from "../../types/winston";
import { ArDriveCommunityOracle } from "./ardriveCommunityOracle";

describe("The ArDriveCommunityOracle", () => {
  const stubContractReader = {
    async readContract() {
      return stubCommunityContract;
    },
  };

  describe("getCommunityWinstonTip method", () => {
    it("returns the expected community tip result", async () => {
      const communityOracle = new ArDriveCommunityOracle([stubContractReader]);

      // 50% stubbed fee of 100 million Winston is 50 million winston
      expect(
        +(await communityOracle.getCommunityWinstonTip(W(100_000_000)))
      ).to.equal(50_000_000);
    });

    it("returns the expected minimum community tip result when the derived tip is below the minimum", async () => {
      const communityOracle = new ArDriveCommunityOracle([stubContractReader]);

      expect(
        +(await communityOracle.getCommunityWinstonTip(W(10_000_000)))
      ).to.equal(10_000_000);
    });
  });

  describe("selectTokenHolder method", () => {
    it("returns the expected arweave address", async () => {
      const communityOracle = new ArDriveCommunityOracle([stubContractReader]);

      expect(`${await communityOracle.selectTokenHolder()}`).to.equal(
        "1234567890123456789012345678901231234567890"
      );
    });
  });
});
