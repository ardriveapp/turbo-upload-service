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
import axios, { AxiosResponse } from "axios";

import { TransactionId } from "../../types/types";
import { ContractReader } from "./contractOracle";

/**
 *  Oracle class responsible for retrieving and
 *  reading Smartweave Contracts from the Verto cache
 */
export class VertoContractReader implements ContractReader {
  /** Fetches smartweave contracts from the Verto cache */
  public async readContract(txId: TransactionId): Promise<unknown> {
    const response: AxiosResponse = await axios.get(
      `https://v2.cache.verto.exchange/${txId}`
    );
    const contract = response.data;
    return contract.state;
  }
}
