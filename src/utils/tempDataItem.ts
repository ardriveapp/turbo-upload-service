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
import { randomUUID } from "crypto";
import {
  PathLike,
  ReadStream,
  WriteStream,
  createReadStream,
  createWriteStream,
} from "fs";
import { rename, unlink } from "fs/promises";

import logger from "../logger";
import { cleanUpTempFile } from "./common";

export class TempDataItem {
  private readonly tempDataItemPath = `temp/${Date.now()}.${randomUUID()}`;

  public get writeStream(): WriteStream {
    return createWriteStream(this.tempDataItemPath);
  }

  public get readStream(): ReadStream {
    return createReadStream(this.tempDataItemPath);
  }

  public async renameTo(objectStoreKeyPath: PathLike) {
    return rename(this.tempDataItemPath, objectStoreKeyPath);
  }

  public async unlink(): Promise<void> {
    return unlink(this.tempDataItemPath).catch((error: unknown) => {
      logger.error(error);
    });
  }

  public cleanUp(): void {
    cleanUpTempFile(this.tempDataItemPath);
  }
}
