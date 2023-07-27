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
import { readContract } from "smartweave";

import { TransactionId } from "../../types/types";
import { ContractReader } from "./contractOracle";

/**
 *  Oracle class responsible for retrieving and reading
 *  Smartweave Contracts from Arweave with the `smartweave` package
 */
export class SmartweaveContractReader implements ContractReader {
  constructor(private readonly arweave: Arweave) {}

  /** Fetches smartweave contracts from Arweave with smartweave-js */
  async readContract(txId: TransactionId): Promise<unknown> {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    return readContract(this.arweave, `${txId}`);
  }
}
