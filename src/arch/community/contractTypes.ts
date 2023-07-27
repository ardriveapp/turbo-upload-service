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
import { PublicArweaveAddress } from "../../types/types";

export type CommunityTipPercentage = number;

/** Shape of the ArDrive Community Smart Contract */
export interface CommunityContractData {
  name: "ArDrive";
  ticker: "ARDRIVE";
  votes: communityContractVotes[];
  settings: CommunityContractSettings;
  balances: { [tokenHolderAddress: string]: number };
  vault: {
    [tokenHolderAddress: string]: {
      balance: number;
      start: number;
      end: number;
    }[];
  };
}

interface communityContractVotes {
  status: "passed" | "quorumFailed";
  type: "burnVault" | "mintLocked" | "mint" | "set";
  note: string;
  yays: number;
  nays: number;
  voted: PublicArweaveAddress[];
  start: number;
  totalWeight: number;
  recipient?: PublicArweaveAddress;
  qty?: number;
  lockLength?: number;
}

type CommunityContractSettings = [
  ["quorum", number],
  ["support", number],
  ["voteLength", number],
  ["lockMinLength", number],
  ["lockMaxLength", number],
  ["communityAppUrl", string],
  ["communityDiscussionLinks", string[]],
  ["communityDescription", string],
  ["communityLogo", PublicArweaveAddress],
  ["fee", number]
];
