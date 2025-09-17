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
import { Readable } from "stream";

import logger from "../logger";

export const streamToBuffer = async (
  stream: Readable,
  data_size = 0,
  encoding?: BufferEncoding
): Promise<Buffer> => {
  let offset = 0;
  logger.debug(
    `Converting stream of expected data size ${data_size} to Buffer...`
  );

  if (stream.isPaused()) {
    stream.resume();
  }

  let buffer: Buffer = Buffer.alloc(data_size, undefined, encoding);
  return new Promise((resolve, reject) => {
    stream.on("data", (chunk: Buffer) => {
      if (data_size > 0) {
        // Faster than concat, if we already know buffer size
        buffer.set(chunk, offset);
        offset += chunk.byteLength;
      } else {
        buffer = Buffer.concat([buffer, chunk]);
      }
    });

    stream.on("error", (err) => {
      logger.error(
        `[encoding] streamToBuffer error: ${JSON.stringify(err, null, 2)}`
      );
      reject(err);
    });

    stream.on("end", () => {
      resolve(buffer);
    });
  });
};

// Presumes that the buffer is preallocated to receive the stream data
export const streamIntoBufferAtOffset = async (
  stream: Readable,
  buffer: Buffer,
  offset = 0
): Promise<void> => {
  return new Promise((resolve, reject) => {
    stream.on("data", (chunk: Buffer) => {
      buffer.set(chunk, offset);
      offset += chunk.byteLength;
    });

    stream.on("error", (err) => {
      logger.error(
        `[encoding] streamIntoBufferAtOffset error: ${JSON.stringify(
          err,
          null,
          2
        )}`
      );
      reject(err);
    });

    stream.on("end", () => {
      resolve();
    });
  });
};
