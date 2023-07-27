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
import { ArweaveInterface } from "../../arweaveJs";
import { PublicArweaveAddress } from "../../types/types";
import { W, Winston } from "../../types/winston";
import { ArDriveContractOracle } from "./ardriveContractOracle";
import { CommunityOracle } from "./communityOracle";
import { ContractOracle, ContractReader } from "./contractOracle";
import { SmartweaveContractReader } from "./smartweaveContractOracle";
import { VertoContractReader } from "./vertoContractReader";
import { weightedRandom } from "./weightedRandom";

/**
 * Minimum ArDrive community tip from the Community Improvement Proposal Doc:
 * https://arweave.net/Yop13NrLwqlm36P_FDCdMaTBwSlj0sdNGAC4FqfRUgo
 */
export const minArDriveCommunityWinstonTip = W(10_000_000);

/**
 * Oracle class responsible for determining the community tip
 * and selecting the PST token holder for tip distribution
 *

 */
export class ArDriveCommunityOracle implements CommunityOracle {
  constructor(
    contractReaders: ContractReader[] = [
      new VertoContractReader(),
      new SmartweaveContractReader(new ArweaveInterface()["arweaveJs"]),
    ]
  ) {
    this.contractOracle = new ArDriveContractOracle(contractReaders);
  }

  private readonly contractOracle: ContractOracle;

  /**
   * Given a Winston data cost, returns a calculated ArDrive community tip amount in Winston
   *

   */
  async getCommunityWinstonTip(winstonCost: Winston): Promise<Winston> {
    const communityTipPercentage =
      await this.contractOracle.getTipPercentageFromContract();
    const arDriveCommunityTip = winstonCost.times(communityTipPercentage);
    return Winston.max(arDriveCommunityTip, minArDriveCommunityWinstonTip);
  }

  /**
   * Gets a random ArDrive token holder based off their weight (amount of tokens they hold)
   *

   */
  async selectTokenHolder(): Promise<PublicArweaveAddress> {
    // Read the ArDrive Smart Contract to get the latest state
    const contract = await this.contractOracle.getCommunityContract();

    const balances = contract.balances;
    const vault = contract.vault;

    // Get the total number of token holders
    let total = 0;
    for (const addr of Object.keys(balances)) {
      total += balances[addr];
    }

    // Check for how many tokens the user has staked/vaulted
    for (const addr of Object.keys(vault)) {
      if (!vault[addr].length) continue;

      const vaultBalance = vault[addr]
        .map((a: { balance: number; start: number; end: number }) => a.balance)
        .reduce((a: number, b: number) => a + b, 0);

      total += vaultBalance;

      if (addr in balances) {
        balances[addr] += vaultBalance;
      } else {
        balances[addr] = vaultBalance;
      }
    }

    // Create a weighted list of token holders
    const weighted: { [addr: string]: number } = {};
    for (const addr of Object.keys(balances)) {
      weighted[addr] = balances[addr] / total;
    }
    // Get a random holder based off of the weighted list of holders
    const randomHolder = weightedRandom(weighted);

    if (randomHolder === undefined) {
      throw new Error(
        "Token holder target could not be determined for community tip distribution.."
      );
    }

    return randomHolder;
  }
}
