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
import { computePublicKey } from "@ethersproject/signing-key";
import { createHash } from "crypto";
import { computeAddress } from "ethers";

import { JWKInterface } from "../types/jwkTypes";
import { Base64String, PublicArweaveAddress } from "../types/types";
import { getPublicKeyFromJwk } from "./common";

export function jwkToPublicArweaveAddress(
  jwk: JWKInterface
): PublicArweaveAddress {
  return ownerToNormalizedB64Address(getPublicKeyFromJwk(jwk));
}

export function ownerToNormalizedB64Address(
  owner: Base64String
): PublicArweaveAddress {
  return sha256B64Url(fromB64Url(owner));
}

export function ownerToEthAddress(owner: Base64String) {
  return computeAddress(computePublicKey(fromB64Url(owner)));
}

export function fromB64Url(input: Base64String) {
  const paddingLength = input.length % 4 == 0 ? 0 : 4 - (input.length % 4);

  const base64 = input
    .replace(/-/g, "+")
    .replace(/_/g, "/")
    .concat("=".repeat(paddingLength));

  return Buffer.from(base64, "base64");
}

export function toB64Url(buffer: Buffer): Base64String {
  return buffer.toString("base64url");
}

export function sha256B64Url(input: Buffer): Base64String {
  return toB64Url(createHash("sha256").update(input).digest());
}

// check if it is a valid arweave base64url for a wallet public address, transaction id or smartweave contract
export function isValidArweaveBase64URL(base64URL: Base64String) {
  const base64URLRegex = new RegExp("^[a-zA-Z0-9_-]{43}$");
  return base64URLRegex.test(base64URL);
}
