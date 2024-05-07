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
import {
  ArweaveSigner,
  EthereumSigner,
  SolanaSigner,
  createData,
} from "arbundles";
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
});
