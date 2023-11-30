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
import { expect } from "chai";
import { readFileSync } from "fs";

import { UnsignedReceipt, signReceipt } from "./signReceipt";
import { verifyReceipt } from "./verifyReceipt";

describe("signReceipt", () => {
  it("should sign a receipt that can be verified", async () => {
    const receipt: UnsignedReceipt = {
      id: "QpmY8mZmFEC8RxNsgbxSV6e36OF6quIYaPRKzvUco0o",
      deadlineHeight: 1310000,
      timestamp: 1700590909589,
      version: "0.1.0",
      dataCaches: ["arweave.net"],
      fastFinalityIndexes: ["arweave.net"],
      winc: "0",
    };
    const privateKey = JSON.parse(
      readFileSync("tests/stubFiles/testWallet.json", {
        encoding: "utf-8",
        flag: "r",
      })
    );
    const signedReceipt = await signReceipt(receipt, privateKey);
    const { signature, public: pubKey } = signedReceipt;

    expect(signature).to.equal(
      "WMedj9iwlmsXojtRV-JZcjU8N5IO1ssmbqeDl8hRn0EsG-0fHql1ieEmmp4LZKjc6nMAtst5BZ3SFAJJKdFOjGrh-2_nVsd0E91vky4TRzOLf4v78-3HI6y-qHkvZx5Ldzu7RT6PGGqDZrR2_cGLyYyHI5jj0ZpnHlDxreLP9BFjd_1vCYQ4Lm3PjWQnqbncsvGO0biaOJcA1Kn7FWlLPgoFAR3q10DxMR1ppXMGAocC9rEW-OYS-CEdOjqFPQvsZz22V17wTyXptgB9diZpcuMhZr0rgi0tn_98mPSnCedVK5j1NZplXQu8z2msc-IdlYHMILrEc_9x2qByj3L50cz2FUrm820WC5M7Q6C4fiFO73jhbsJUTdbMdOJnadOgpIfAE5iPJhQEx3F3i8Ekyv5SCK9c861O_SS0hfcartu9k7qj6SjCF4AxjkXB125N18bU0N8br6VnUzm-uQ1n9tddVYrt3DbTPqajs8TgFxhzzGO1SKivgU0WARMz2WAWkXeBN6lRtagr94YivE26WrUTH1xyFNNjmKG8X1bTyV_Xjlz2cEWfQ_UEk9uDQfJFDXkQuPOT8audI4Gy2vkBy3nI88u8WbbIL-UTnJQTtVc6xAvgs98_1b8HzjBTsrKRwnFY-XVx4dzXujzb7Zfl1UcTDztvLeWcVF4gUd6uRks"
    );
    expect(pubKey).to.equal(
      "qREovbmD6oxgHYNCzOeTei07lSz0-YLcjnvgSDzqptsCiqNOtB3RKUToSX5hkPD45fJDY4057XkkcsQRuGsU8y9rgm37i-Kiyd5Z_iy6pJXrwi9XnAgGL118lIV790GZ7xe5o3DvPV3Px74C0ABsfL9lW86D4t_qClJ6wSQksKNd7rnUImIvHW0vxLswST7dfUngevzKt4kv48VTub4951XdUHjb45Uurf7xFYSCizAGtGqr5GYDFrVk-mNrzFH5bXt06PJfxe9E5ujIE5Uq1Az6vqEOO0E1mWmXqdTPluAxcjmgktkoNLHZnnU-BsYuFaTWW5NU3aS-RgJKXYs9O6Dc1-2SITl-H_wtdGNSj31fj72UkaAbkpA1mionK-8bOIkSpYgKCyTC42oHh1Fw4SLXPyLxBj1w6F32LSLjpse5dmKymj4fJPezCMdi709uIiVT7XOm4LZBCzFOxS6-UNxgE57dBFpcWkcqNO3p00biYqH5d5bzvK3bwd-4j0KyKlqPYTProSnd3P6ROZuuJaLoLD_Or5-L_dUjawHz-DFlzmckYaf8l3XdzPM4JSsE8CEDZl0NCN0AidXt_wjbr6k9JsO7cnLB226AjxhcyuxZOlhgkIn7EbxpVXx-O2mXkcXF8PixCP0k5brtriLeF1MAdspgd_S-LKQeGVr7-mk"
    );

    expect(await verifyReceipt({ ...receipt, signature, public: privateKey.n }))
      .to.be.true;
  });
});
