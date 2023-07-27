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
import { createReadStream } from "fs";
import { Knex } from "knex";

import { PostgresDatabase } from "../src/arch/db/postgres";
import {
  NewBundle,
  NewDataItem,
  PlanId,
  PlannedDataItem,
} from "../src/types/dbTypes";
import {
  ByteCount,
  PublicArweaveAddress,
  TransactionId,
} from "../src/types/types";
import { W, Winston } from "../src/types/winston";

export const stubTxId1 = "0000000000000000000000000000000000000000001";
export const stubTxId2 = "0000000000000000000000000000000000000000002";
export const stubTxId3 = "0000000000000000000000000000000000000000003";
export const stubTxId4 = "0000000000000000000000000000000000000000004";
export const stubTxId5 = "0000000000000000000000000000000000000000005";
export const stubTxId6 = "0000000000000000000000000000000000000000006";
export const stubTxId7 = "0000000000000000000000000000000000000000007";
export const stubTxId8 = "0000000000000000000000000000000000000000008";
export const stubTxId9 = "0000000000000000000000000000000000000000009";
export const stubTxId10 = "0000000000000000000000000000000000000000010";
export const stubTxId11 = "0000000000000000000000000000000000000000011";
export const stubTxId12 = "0000000000000000000000000000000000000000012";
export const stubTxId13 = "0000000000000000000000000000000000000000013";
export const stubTxId14 = "0000000000000000000000000000000000000000014";
export const stubTxId15 = "0000000000000000000000000000000000000000015";
export const stubTxId16 = "0000000000000000000000000000000000000000016";
export const stubTxId17 = "0000000000000000000000000000000000000000017";
export const stubTxId18 = "0000000000000000000000000000000000000000018";
export const stubTxId19 = "0000000000000000000000000000000000000000019";
export const stubTxId20 = "0000000000000000000000000000000000000000020";
export const stubTxId21 = "0000000000000000000000000000000000000000021";
export const stubTxId22 = "0000000000000000000000000000000000000000022";

export const stubOwnerAddress: PublicArweaveAddress =
  "1234567890123456789012345678901231234567890";
export const bundleTxStubOwnerAddress: PublicArweaveAddress = // cspell:disable
  "qq3rgKMjvb6GcmC4K1LWP425yTgS5ltx55b2LuaruFw"; // cspell:enable
export const stubByteCount: ByteCount = 123;
export const stubWinstonPrice: Winston = W(1234);
export const stubBlockHeight = 123456;

export const stubPlanId = "00000000-0000-0000-0000-000000000001";
export const stubPlanId2 = "00000000-0000-0000-0000-000000000002";
export const stubPlanId3 = "00000000-0000-0000-0000-000000000003";

const baseDate = new Date("2022-09-01 16:20:00");
export const stubDates = {
  earliestDate: baseDate.toISOString(),
  middleDate: new Date(baseDate.getTime() + 60_000).toISOString(),
  latestDate: new Date(baseDate.getTime() + 60_000).toISOString(),
};

export function stubNewDataItem(
  dataItemId: TransactionId,
  byteCount?: ByteCount
): NewDataItem {
  return {
    assessedWinstonPrice: stubWinstonPrice,
    byteCount: byteCount ?? stubByteCount,
    dataItemId,
    ownerPublicAddress: stubOwnerAddress,
    uploadedDate: stubDates.earliestDate,
    failedBundles: [],
  };
}

export function stubPlannedDataItem(
  dataItemId: TransactionId,
  planId: PlanId = stubPlanId,
  byteCount?: ByteCount
): PlannedDataItem {
  return {
    ...stubNewDataItem(dataItemId, byteCount),
    planId,
    plannedDate: stubDates.earliestDate,
  };
}

export function stubNextBundleToPost(): NewBundle {
  return {
    bundleId: stubTxId1,
    signedDate: stubDates.earliestDate,
    reward: stubWinstonPrice,
    planId: stubPlanId,
    plannedDate: stubDates.earliestDate,
  };
}

export class StubDatabase extends PostgresDatabase {
  constructor() {
    super(null as unknown as Knex);
  }
}

export const stubDataItemBase64Signature = // cspell:disable
  "wUIlPaBflf54QyfiCkLnQcfakgcS5B4Pld-hlOJKyALY82xpAivoc0fxBJWjoeg3zy9aXz8WwCs_0t0MaepMBz2bQljRrVXnsyWUN-CYYfKv0RRglOl-kCmTiy45Ox13LPMATeJADFqkBoQKnGhyyxW81YfuPnVlogFWSz1XHQgHxrFMAeTe9epvBK8OCnYqDjch4pwyYUFrk48JFjHM3-I2kcQnm2dAFzFTfO-nnkdQ7ulP3eoAUr-W-KAGtPfWdJKFFgWFCkr_FuNyHYQScQo-FVOwIsvj_PVWEU179NwiqfkZtnN8VoBgCSxbL1Wmh4NYL-GsRbKz_94hpcj5RiIgq0_H5dzAp-bIb49M4SP-DcuIJ5oT2v2AfPWvznokDDVTeikQJxCD2n9usBOJRpLw_P724Yurbl30eNow0U-Jmrl8S6N64cjwKVLI-hBUfcpviksKEF5_I4XCyciW0TvZj1GxK6ET9lx0s6jFMBf27-GrFx6ZDJUBncX6w8nDvuL6A8TG_ILGNQU_EDoW7iil6NcHn5w11yS_yLkqG6dw_zuC1Vkg1tbcKY3703tmbF-jMEZUvJ6oN8vRwwodinJjzGdj7bxmkUPThwVWedCc8wCR3Ak4OkIGASLMUahSiOkYmELbmwq5II-1Txp2gDPjCpAf9gT6Iu0heAaXhjk"; // cspell:enable

export const stubDataItemRawSignatureReadStream = () =>
  createReadStream("tests/stubFiles/stub1115ByteDataItem", {
    // We only grab the byte range of the raw signature
    start: 2,
    end: 513,
  });

export const stubCommunityContract = {
  settings: [["fee", 50]],
  vault: { [`${stubOwnerAddress}`]: [{ balance: 500, start: 1, end: 2 }] },
  balances: { [`${stubOwnerAddress}`]: 200 },
};
