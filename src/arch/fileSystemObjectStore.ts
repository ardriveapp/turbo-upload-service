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
import { createReadStream, existsSync, mkdirSync, statSync } from "fs";
import { writeFile } from "fs/promises";
import { Readable } from "stream";

import { TransactionId } from "../types/types";
import { cleanUpTempFile } from "../utils/common";
import { ObjectStore } from "./objectStore";

const localDirectories = [
  "temp",
  "temp/bundle",
  "temp/raw-data-item",
  "temp/header",
  "temp/bundle-payload",
  "temp/data",
];
export class FileSystemObjectStore implements ObjectStore {
  constructor() {
    // create the directories if they don't exist
    for (const dir of localDirectories) {
      if (!existsSync(dir)) {
        mkdirSync(dir);
      }
    }
  }

  public async putObject(Key: string, fileReadStream: Readable) {
    await writeFile(`temp/${Key}`, fileReadStream);
  }

  public async getObject(Key: string, Range?: string): Promise<Readable> {
    const range = Range?.split("=")[1].split("-");
    const start = range?.[0] ?? undefined;
    const end = range?.[1] ?? undefined;

    return createReadStream(`temp/${Key}`, {
      start: start !== undefined ? +start : start,
      end: end !== undefined ? +end : end,
    });
  }

  public getObjectByteCount(Key: string): Promise<number> {
    return Promise.resolve(statSync(`temp/${Key}`).size);
  }

  public async removeObject(dataItemTxId: TransactionId) {
    return cleanUpTempFile(`temp/${dataItemTxId}`);
  }
}
