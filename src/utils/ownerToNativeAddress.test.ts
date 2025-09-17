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
import { Secp256k1HdWallet, makeCosmoshubPath } from "@cosmjs/amino";
import { Slip10, Slip10Curve } from "@cosmjs/crypto";
import { toHex } from "@cosmjs/encoding";
import {
  ArweaveSigner,
  EthereumSigner,
  KyveSigner,
  SolanaSigner,
  createData,
} from "@dha-team/arbundles";
import KyveSDK from "@kyvejs/sdk";
import base58 from "bs58";
import { expect } from "chai";
import { Wallet } from "ethers";
import { readFileSync } from "node:fs";

import { testArweaveJWK } from "../../tests/test_helpers";
import { ownerToNativeAddress } from "./ownerToNativeAddress";

describe("ownerToNativeAddress", () => {
  it("should return a native address for a solana owner", async () => {
    const solanaWallet = base58.encode(
      JSON.parse(
        readFileSync(
          "tests/stubFiles/testSolanaWallet.5aUnUVi1HcUK3uuSV92otUEG5MiWYmUuMfpxmPMf96y4.json",
          {
            encoding: "utf-8",
          }
        )
      )
    );

    const solanaSigner = new SolanaSigner(solanaWallet);

    const dataItem = createData("data", solanaSigner);
    await dataItem.sign(solanaSigner);

    const { owner, signatureType } = dataItem;
    expect(signatureType).to.equal(2);

    const result = ownerToNativeAddress(owner, signatureType);

    expect(result).to.equal("5aUnUVi1HcUK3uuSV92otUEG5MiWYmUuMfpxmPMf96y4");
  });

  it("should return a native address for an ethereum owner", async () => {
    const wallet = Wallet.createRandom();
    const signer = new EthereumSigner(wallet.privateKey);

    const dataItem = createData("data", signer);
    await dataItem.sign(signer);

    const { owner, signatureType } = dataItem;
    expect(signatureType).to.equal(3);

    const result = ownerToNativeAddress(owner, signatureType);

    expect(result).to.equal(wallet.address);
  });

  it("should return a native address for an arweave owner", async () => {
    const wallet = testArweaveJWK;
    const signer = new ArweaveSigner(wallet);
    const dataItem = createData("data", signer);
    await dataItem.sign(signer);

    const { owner, signatureType } = dataItem;

    const result = ownerToNativeAddress(owner, signatureType);

    // cspell:disable
    expect(result).to.equal("8wgRDgvYOrtSaWEIV21g0lTuWDUnTu4_iYj4hmA7PI0"); // cspell:enable
  });

  it("should return a native address for a kyve owner", async () => {
    const KYVE_SEED_PHRASE = await KyveSDK.generateMnemonic();
    const kyveAddress = await KyveSDK.getAddressFromMnemonic(KYVE_SEED_PHRASE);

    const kyveWallet = await Secp256k1HdWallet.fromMnemonic(KYVE_SEED_PHRASE, {
      prefix: "kyve",
    });

    const privateKey = toHex(
      Slip10.derivePath(
        Slip10Curve.Secp256k1,
        kyveWallet["seed"],
        makeCosmoshubPath(0)
      ).privkey
    );
    const signer = new KyveSigner(privateKey);

    const dataItem = createData("data", signer);
    await dataItem.sign(signer);

    const { owner, signatureType } = dataItem;
    const result = ownerToNativeAddress(owner, signatureType);

    expect(result).to.equal(kyveAddress);
  });
});
