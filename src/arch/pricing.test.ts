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
import { stub } from "sinon";

import { W } from "../types/winston";
import { RetryHttpClient } from "../utils/httpClient";
import { ArweaveGateway } from "./arweaveGateway";
import { PricingService } from "./pricing";

describe("PricingService", () => {
  const httpClient = new RetryHttpClient();
  const gateway = new ArweaveGateway();
  const pricingService = new PricingService({
    minimumMUsdcAmount: 1000,
    x402MarkupPercentage: 30,
    gateway,
    httpClient,
  });

  it("getUsdcForByteCount calculates correct minimum USDC amount", async () => {
    stub(gateway, "getWinstonPriceForByteCount").resolves(W("123456789"));
    stub(httpClient, "get").resolves({
      data: { arweave: { usd: 2.5 } },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);

    const byteCount = 1_000_000; // 1 MB
    const { winc, mUsdc } = await pricingService.getUsdcForByteCount(byteCount);

    // These expected values would be based on mocked responses from gateway and coingecko
    const expectedWinc = "123456789"; // Example expected winston cost

    expect(winc.toString()).to.equal(expectedWinc);
    expect(mUsdc).to.equal(1000); // Since calculated amount is less than minimum, should return minimum
  });
  it("getUsdcForByteCount calculates correct USDC amount", async () => {
    stub(gateway, "getWinstonPriceForByteCount").resolves(W(1_000_000_000_000));
    stub(httpClient, "get").resolves({
      data: { arweave: { usd: 2.5 } },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any);

    const pricingService = new PricingService({
      minimumMUsdcAmount: 0,
      x402MarkupPercentage: 30,
      gateway,
      httpClient,
    });

    const { mUsdc } = await pricingService.getUsdcForByteCount(1); // byteCount doesn't matter due to stubbing

    // Example math:
    // 1 AR * $2.5 = $2.5
    // $2.5 * 1_000_000 = 2_500_000 mUSDC
    // *1.3 markup = 3_250_000
    expect(mUsdc).to.equal(3_250_000);
  });
});
