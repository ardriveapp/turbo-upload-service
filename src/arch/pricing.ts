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
import { gatewayUrl } from "../constants";
import { PlannedDataItem } from "../types/dbTypes";
import { ByteCount, TxAttributes } from "../types/types";
import { ArweaveGateway, Gateway } from "./arweaveGateway";

export class PricingService {
  constructor(
    private readonly gateway: Gateway = new ArweaveGateway({
      endpoint: gatewayUrl,
    })
  ) {}

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
