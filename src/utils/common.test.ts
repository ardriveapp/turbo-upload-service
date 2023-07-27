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

import { filterKeysFromObject } from "./common";

describe("filterKeysFromObject function", () => {
  it("filters top level keys from objects as expected", () => {
    const testObject = {
      ["Test Key 1"]: { "Test Key 1": 42 },
      testKey2: ["string", 22],
      testKeyThree: "two words",
    };

    const filteredResult = filterKeysFromObject(testObject, [
      "Test Key 1",
      "testKeyThree",
    ]);

    expect(filteredResult).to.deep.equal({
      testKey2: ["string", 22],
    });
  });
});
