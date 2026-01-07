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
import { ReadThroughPromiseCache } from "@ardrive/ardrive-promise-cache";
import { Logger } from "winston";

import { gatewayUrl, winstonPerAr } from "../constants";
import defaultLogger from "../logger";
import { PlannedDataItem } from "../types/dbTypes";
import { ByteCount, TxAttributes, Winston } from "../types/types";
import { RetryHttpClient, createRetryHttpClient } from "../utils/httpClient";
import { ArweaveGateway, Gateway } from "./arweaveGateway";

const coinGeckoUrl = "https://api.coingecko.com/api/v3/simple/price";
const cacheTTLMillis = 5 * 60 * 1000; // 5 minutes

interface PricingServiceParams extends X402PricingSettings {
  httpClient?: RetryHttpClient;
  gateway?: Gateway;
  logger?: Logger;
}

interface X402PricingSettings {
  minimumMUsdcAmount?: number;
  x402MarkupPercentage?: number;
}

export class PricingService {
  private arUsdPriceReadThroughPromiseCache: ReadThroughPromiseCache<
    "price",
    string
  > = new ReadThroughPromiseCache<"price", string>({
    cacheParams: { cacheCapacity: 100, cacheTTLMillis: cacheTTLMillis }, // 5 minutes
    readThroughFunction: async (): Promise<string> => {
      const response = await this.httpClient.get(coinGeckoUrl, {
        params: {
          ids: "arweave",
          vs_currencies: "usd",
        },
      });
      const arPriceInUsd = response.data?.arweave?.usd;
      if (arPriceInUsd === undefined) {
        throw new Error(
          `Failed to get AR price from Coingecko response: ${JSON.stringify(
            response.data
          )}`
        );
      }
      return arPriceInUsd.toString();
    },
  });

  private httpClient: RetryHttpClient;
  private gateway: Gateway;
  private logger: Logger;

  private minimumMUsdcAmount: number;
  private x402MarkupPercentage: number;

  constructor({
    httpClient = createRetryHttpClient(),
    gateway = new ArweaveGateway({ endpoint: gatewayUrl }),
    logger = defaultLogger,
    minimumMUsdcAmount = +(process.env.MINIMUM_X402_MUSDC_AMOUNT || 1_000), // 0.001 USDC for smallest uploads
    x402MarkupPercentage = +(process.env.X402_MARKUP_PERCENTAGE || 30), // 30% markup
  }: PricingServiceParams = {}) {
    this.httpClient = httpClient;
    this.gateway = gateway;
    this.logger = logger;
    if (!Number.isFinite(minimumMUsdcAmount)) {
      throw new Error(
        "MINIMUM_X402_MUSDC_AMOUNT must resolve to a finite number."
      );
    }
    if (!Number.isFinite(x402MarkupPercentage)) {
      throw new Error(
        "X402_MARKUP_PERCENTAGE must resolve to a finite number."
      );
    }
    this.minimumMUsdcAmount = minimumMUsdcAmount;
    this.x402MarkupPercentage = x402MarkupPercentage;
  }

  public async getTxAttributesForDataItems(
    dataItems: PlannedDataItem[]
  ): Promise<TxAttributes> {
    const totalDataItemByteCount = dataItems
      .map((d) => d.byteCount)
      .reduce((a, b) => a + b);
    const totalDataItems = dataItems.length;
    const bundledByteCount = bundledByteCountOfBundleToPack(
      totalDataItemByteCount,
      totalDataItems
    );

    const txAttributes: TxAttributes = {};

    // TODO: call the real pricing service (or the pricing oracle within payment service)
    txAttributes.reward = (
      await this.gateway.getWinstonPriceForByteCount(
        bundledByteCount,
        txAttributes.target
      )
    ).toString();
    txAttributes.last_tx = await this.gateway.getBlockHash();

    return txAttributes;
  }

  public async getUsdcForByteCount(
    byteCount: ByteCount
  ): Promise<{ winc: Winston; mUsdc: number }> {
    const winstonCost = await this.gateway.getWinstonPriceForByteCount(
      byteCount
    );
    const arUsdPriceString = await this.arUsdPriceReadThroughPromiseCache.get(
      "price"
    );
    const arUsdPrice = parseFloat(arUsdPriceString);
    const mUsdcAmount = Math.max(
      this.minimumMUsdcAmount,
      Math.ceil(
        (+winstonCost / winstonPerAr) *
          arUsdPrice *
          1e6 *
          // Add infrastructure markup
          (1 + this.x402MarkupPercentage / 100)
      )
    );

    this.logger.debug("Calculated USDC amount for byte count", {
      byteCount,
      winstonCost: winstonCost.toString(),
      arUsdPrice,
      mUsdcAmount,
    });

    return { winc: winstonCost, mUsdc: mUsdcAmount };
  }
}

/** Calculate the bundled size from the total dataItem byteCount and the number of dataItems */
function bundledByteCountOfBundleToPack(
  totalDataItemByteCount: ByteCount,
  numberOfDataItems: number
): ByteCount {
  // 32 byte array for representing the number of data items in the bundle
  const byteArraySize = 32;

  // Each data item gets a 64 byte header added to the bundle
  const headersSize = numberOfDataItems * 64;

  return byteArraySize + +totalDataItemByteCount + headersSize;
}
