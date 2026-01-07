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
import { Readable } from "stream";

import { readExactly } from "./streamUtils";

describe("readExactly paused-state behavior", () => {
  it("preserves paused state and returns leftover bytes to the stream", async () => {
    const combined = Buffer.from("abcdefghijklmnopqrstuvwxyz");
    const stream = Readable.from([combined]);
    // Ensure paused
    stream.pause();

    // Read 5 bytes
    const { bytes, rest } = await readExactly(stream, 5);
    expect(bytes.toString()).to.equal("abcde");

    // Consume the rest and verify leftover from clean rest stream
    const chunks: Buffer[] = [];
    for await (const chunk of rest) {
      chunks.push(chunk as Buffer);
    }
    const restBuf = Buffer.concat(chunks);
    expect(restBuf.toString()).to.equal("fghijklmnopqrstuvwxyz");
  });
});
