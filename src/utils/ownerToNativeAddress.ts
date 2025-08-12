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
import { pubkeyToAddress } from "@cosmjs/amino";
import { Secp256k1 } from "@cosmjs/crypto";
import { toBase64 } from "@cosmjs/encoding";
import { computePublicKey } from "@ethersproject/signing-key";
import bs58 from "bs58";
import { computeAddress } from "ethers";

import { sigNameToSigInfo } from "../constants";
import { SignatureType } from "../types/dbTypes";
import { NativeAddress, SignatureConfig } from "../types/types";
import { fromB64Url, ownerToNormalizedB64Address } from "./base64";

export function ownerToNativeAddress(
  owner: string,
  signatureType: SignatureType
): NativeAddress {
  sigNameToSigInfo;
  switch (signatureType) {
    case SignatureConfig.ED25519:
    case SignatureConfig.SOLANA:
      return bs58.encode(fromB64Url(owner));

    case SignatureConfig.ETHEREUM:
      return computeAddress(computePublicKey(fromB64Url(owner)));

    case SignatureConfig.KYVE:
      return pubkeyToAddress(
        {
          type: "tendermint/PubKeySecp256k1",
          value: toBase64(Secp256k1.compressPubkey(fromB64Url(owner))),
        },
        "kyve"
      );

    case SignatureConfig.ARWEAVE:
    default:
      return ownerToNormalizedB64Address(owner);
  }
}
