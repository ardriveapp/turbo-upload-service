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
import { deepHash, stringToBuffer } from "@dha-team/arbundles";
import { DeepHashChunk } from "@dha-team/arbundles/build/node/esm/src/deepHash";
import Arweave from "arweave";

import { JWKInterface } from "../types/jwkTypes";
import { toB64Url } from "./base64";
import { getPublicKeyFromJwk } from "./common";

export type UnsignedReceipt = {
  id: string;
  deadlineHeight: number;
  timestamp: number;
  version: string;
  dataCaches: string[];
  fastFinalityIndexes: string[];
  // Added in v0.2.0
  winc: string;
};

export type IrysUnsignedReceipt = Omit<
  UnsignedReceipt,
  "dataCaches" | "fastFinalityIndexes" | "winc"
>;

export type SignedReceipt = UnsignedReceipt & {
  public: string;
  signature: string;
};

export type IrysSignedReceipt = IrysUnsignedReceipt & {
  public: string;
  signature: string;
};

// TODO: Add receipt version input and use it to prepare hashes for different versions
export function prepareHash(receipt: UnsignedReceipt): DeepHashChunk {
  return [
    stringToBuffer("Bundlr"),
    stringToBuffer(receipt.version),
    stringToBuffer(receipt.id),
    stringToBuffer(receipt.deadlineHeight.toString()),
    stringToBuffer(receipt.timestamp.toString()),

    /**
     * Temporarily excluded for irys migration
     */
    // stringToBuffer(receipt.dataCaches.join(",")),
    // stringToBuffer(receipt.fastFinalityIndexes.join(",")),
    // stringToBuffer(receipt.winc),
  ];
}

export async function signReceipt(
  receipt: UnsignedReceipt,
  privateKey: JWKInterface
): Promise<SignedReceipt> {
  const dh = await deepHash(prepareHash(receipt));

  const signatureBuffer = await Arweave.crypto.sign(privateKey, dh, {
    saltLength: 0,
  });
  const signature = toB64Url(Buffer.from(signatureBuffer));

  return { ...receipt, public: getPublicKeyFromJwk(privateKey), signature };
}

export async function signIrysReceipt(
  receipt: IrysUnsignedReceipt,
  privateKey: JWKInterface
): Promise<IrysSignedReceipt> {
  const dh = await deepHash([
    stringToBuffer("Bundlr"),
    stringToBuffer(receipt.version),
    stringToBuffer(receipt.id),
    stringToBuffer(receipt.deadlineHeight.toString()),
    stringToBuffer(receipt.timestamp.toString()),
  ]);

  const signatureBuffer = await Arweave.crypto.sign(privateKey, dh, {
    saltLength: 0,
  });
  const signature = toB64Url(Buffer.from(signatureBuffer));

  return { ...receipt, public: getPublicKeyFromJwk(privateKey), signature };
}
