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
import { expect } from "chai";

import { testArweaveJWK } from "../../tests/test_helpers";
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
    const privateKey = testArweaveJWK;
    const signedReceipt = await signReceipt(receipt, privateKey);
    const { signature, public: pubKey } = signedReceipt;

    expect(signature).to.equal(
      "Lcfe5H7ACKjduuVc3OrZ0gFeILGKuSjnZ6wXq0p5mCcoma2xoqeCUASg1JqAgDA-5o3adzqzXV1uiE6uIEKVXbuLCN6zFLS6qLvtzC8wNquBV1OqVylWhkavXQ-JZY8zJQQoemA_UKLe_HQpyyHc16GkD-uUZaXj3MpF4oSBiWcmOPHVc0WVN3PTFRmWOZGmtwNiGEyPpelhrVW8q5n8r5ve6Ao2snJOIVG4QpDVzBDoU1TBnolEGNyq60XQhQsW8Ck8g7KcYwJj3jFKIujzZeC-ba8oVPac1_HI0roaMxJPGr3fKqoh1w5XHrTwkFoWX0JQWaB5LfhEl1PfyC-UEkix0Pir2z8D9D0FtVD60JUQAVx2sGBIy5KgfGBWPtZY5qyhh7BsQn-dtP9zL07f2f68oDa9vlf0IZokmr7Ad_psmLY5-LZ1QWXpqU5H8eB9cpSRAEyOv8OeFohHYPntpdmcveeAr8l2sQwvI3fn3FZ9cFNz8b7oxCU1mBSUeqJW3d6qWi8bqPyf_xSfKcIOGX7RFXn563567RjJD_pWRA63vUDw2fMt05QywNZG12igLZ_lrX-rFygYQkN68_zQgIyLS9eaXI6ZYa_RKAp4sUgpjlc8WAr2H3dxUMeeeVitfusQjhOmNwyh8GqjfCsRUJIrWiNenCuep5PIDGIgGxw"
    );
    expect(pubKey).to.equal(
      "qREovbmD6oxgHYNCzOeTei07lSz0-YLcjnvgSDzqptsCiqNOtB3RKUToSX5hkPD45fJDY4057XkkcsQRuGsU8y9rgm37i-Kiyd5Z_iy6pJXrwi9XnAgGL118lIV790GZ7xe5o3DvPV3Px74C0ABsfL9lW86D4t_qClJ6wSQksKNd7rnUImIvHW0vxLswST7dfUngevzKt4kv48VTub4951XdUHjb45Uurf7xFYSCizAGtGqr5GYDFrVk-mNrzFH5bXt06PJfxe9E5ujIE5Uq1Az6vqEOO0E1mWmXqdTPluAxcjmgktkoNLHZnnU-BsYuFaTWW5NU3aS-RgJKXYs9O6Dc1-2SITl-H_wtdGNSj31fj72UkaAbkpA1mionK-8bOIkSpYgKCyTC42oHh1Fw4SLXPyLxBj1w6F32LSLjpse5dmKymj4fJPezCMdi709uIiVT7XOm4LZBCzFOxS6-UNxgE57dBFpcWkcqNO3p00biYqH5d5bzvK3bwd-4j0KyKlqPYTProSnd3P6ROZuuJaLoLD_Or5-L_dUjawHz-DFlzmckYaf8l3XdzPM4JSsE8CEDZl0NCN0AidXt_wjbr6k9JsO7cnLB226AjxhcyuxZOlhgkIn7EbxpVXx-O2mXkcXF8PixCP0k5brtriLeF1MAdspgd_S-LKQeGVr7-mk"
    );

    expect(await verifyReceipt({ ...receipt, signature, public: privateKey.n }))
      .to.be.true;
  });
});
