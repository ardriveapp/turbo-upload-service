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
import Arweave from "arweave";
import axios, { AxiosRequestHeaders, AxiosResponse } from "axios";
import { expect } from "chai";
import {
  PathLike,
  createReadStream,
  createWriteStream,
  readFileSync,
  rmSync,
} from "fs";
import { pipeline } from "stream/promises";

import { octetStreamContentType, port } from "../src/constants";
import { gatewayUrl } from "../src/constants";
import { TransactionId } from "../src/types/types";

// upload service url
export const localTestUrl = `http://localhost:${port}`;

// stubbed arweave against local arweave gateway
export const arweave = Arweave.init({
  host: gatewayUrl.hostname,
  port: gatewayUrl.port,
  protocol: gatewayUrl.protocol.replace(":", ""),
});

interface expectAsyncErrorThrowParams {
  promiseToError: Promise<unknown>;

  // errorType: 'Error' | 'TypeError' | ...
  errorType?: string;
  errorMessage?: string;
}

/**
 * Test helper function that takes a promise and will expect a caught error
 *
 * @param promiseToError the promise on which to expect a thrown error
 * @param errorType type of error to expect, defaults to 'Error'
 * @param errorMessage exact error message to expect
 * */
export async function expectAsyncErrorThrow({
  promiseToError,
  errorType = "Error",
  errorMessage,
}: expectAsyncErrorThrowParams): Promise<void> {
  let error: null | Error = null;
  try {
    await promiseToError;
  } catch (err) {
    error = err as Error | null;
  }

  expect(error?.name).to.equal(errorType);

  if (errorMessage) {
    expect(error?.message).to.equal(errorMessage);
  }
}

export async function fundArLocalWalletAddress(
  arweave: Arweave,
  address: string
): Promise<void> {
  await arweave.api.get(`/mint/${address}/9999999999999999`);
}

export async function mineArLocalBlock(arweave: Arweave): Promise<void> {
  await arweave.api.get("mine");
}

export const validDataItem = readFileSync(
  "tests/stubFiles/stub1115ByteDataItem"
);
export const invalidDataItem = readFileSync(
  "tests/stubFiles/stubInvalidDataItem"
);
export const solanaDataItem = readFileSync(
  "tests/stubFiles/stubSolanaDataItem"
);

export const ethereumDataItem = readFileSync(
  "tests/stubFiles/stubEthereumDataItem"
);

export async function postStubDataItem(
  dataItemBuffer: Buffer,
  headers: AxiosRequestHeaders = {
    "Content-Type": octetStreamContentType,
  }
): Promise<AxiosResponse> {
  return axios.post(`${localTestUrl}/v1/tx`, dataItemBuffer, {
    headers,
    maxBodyLength: Infinity,
    validateStatus: () => true,
  });
}

const baseRawDataItemPath = "temp/raw-data-item/";
export async function writeStubRawDataItems(
  targetDataItemIds: TransactionId[],
  fromPath: PathLike
) {
  await Promise.all(
    targetDataItemIds.map((dataItemId) => {
      const writeStream = createWriteStream(
        `${baseRawDataItemPath}${dataItemId}`
      );
      const readStream = createReadStream(fromPath);
      return pipeline(readStream, writeStream);
    })
  );
}

export function deleteStubRawDataItems(targetDataItemIds: TransactionId[]) {
  for (const dataItemId of targetDataItemIds) {
    rmSync(`${baseRawDataItemPath}${dataItemId}`);
  }
}
