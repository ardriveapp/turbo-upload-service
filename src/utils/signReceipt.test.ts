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
      "iU_S6uuG1OD8k0XqMGOmbcKfysDckEMUy4R9-ODPXiQjKXuT4lTngRFBFKO5NQT1iIfqSDKcbTRL6gJowM_L7bBQZRGkojzXD0PNNU2F0bNNJ80VtUktHifGTXbCgz5kiFciL19n0P3nX6ZfXnOn-H8ALZzRJV69apdvwqitpNKxLMPyc-QA0QBxmC3CKPz_7fy2Qg0QHr5g_ZT2Of-YJ_RsZTEoc3g1fgzsEmMBPOsx4XtPrhV6llnA3pncngzHbPdFvypdWiO8Bvr0EWmazNsoanuwK5uKJ_ROIGXW_dBBGN8Vrfv5U6dJnhJVn5IE7JFlixpFTluF_ICRzbUq2pk_re6jEtW1H3ItH2iN0UeFUw1uDbq3HJW6lDc8aOwDwDspJI11KEI6uCz5QmQy2V8DvRknoqcxmuihF6XmmJIZgTVeo6LNufEis9kFxqtc3Dh_gn8z0cDXKEKFycudckmcHP7vkWD68uSssMMJIdVgwvPZss06svfRnI-E33j3MrQI9FzMIv-7Df8iYATyeyldM1v3gexG0kQm0AMG1_8_SLqwu2QlqzM41mrK5vNmQxOVdIQSOPWPvzbF-YGRwpCjlveRBuARGC9JNC4UipvDYri2gRWuBx2uDL7dmVFv1gRll3dYNMYaMULHYngtrrCynB3Cyfhh7cyPlwuNlk0"
    );
    expect(pubKey).to.equal(
      "qREovbmD6oxgHYNCzOeTei07lSz0-YLcjnvgSDzqptsCiqNOtB3RKUToSX5hkPD45fJDY4057XkkcsQRuGsU8y9rgm37i-Kiyd5Z_iy6pJXrwi9XnAgGL118lIV790GZ7xe5o3DvPV3Px74C0ABsfL9lW86D4t_qClJ6wSQksKNd7rnUImIvHW0vxLswST7dfUngevzKt4kv48VTub4951XdUHjb45Uurf7xFYSCizAGtGqr5GYDFrVk-mNrzFH5bXt06PJfxe9E5ujIE5Uq1Az6vqEOO0E1mWmXqdTPluAxcjmgktkoNLHZnnU-BsYuFaTWW5NU3aS-RgJKXYs9O6Dc1-2SITl-H_wtdGNSj31fj72UkaAbkpA1mionK-8bOIkSpYgKCyTC42oHh1Fw4SLXPyLxBj1w6F32LSLjpse5dmKymj4fJPezCMdi709uIiVT7XOm4LZBCzFOxS6-UNxgE57dBFpcWkcqNO3p00biYqH5d5bzvK3bwd-4j0KyKlqPYTProSnd3P6ROZuuJaLoLD_Or5-L_dUjawHz-DFlzmckYaf8l3XdzPM4JSsE8CEDZl0NCN0AidXt_wjbr6k9JsO7cnLB226AjxhcyuxZOlhgkIn7EbxpVXx-O2mXkcXF8PixCP0k5brtriLeF1MAdspgd_S-LKQeGVr7-mk"
    );

    expect(await verifyReceipt({ ...receipt, signature, public: privateKey.n }))
      .to.be.true;
  });
});
