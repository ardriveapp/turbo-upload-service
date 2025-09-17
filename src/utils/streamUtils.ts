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

// A utility function that ensures a stream is cleaned up after use regardless of success or failure
export async function useAndCleanupReadable<T>(
  streamFactory: () => Promise<Readable>,
  useStream: (stream: Readable) => Promise<T>
): Promise<T> {
  const stream = await streamFactory();
  try {
    // Await useStream so that the finally block does not run immediately
    return await useStream(stream);
  } finally {
    stream.destroy();
  }
}

export async function drainStream(readable: Readable): Promise<void> {
  readable.resume(); // Ensure the stream is flowing
  return new Promise((resolve, reject) => {
    readable.on("data", () => {
      // do nothing, just drain chunks
    });
    readable.once("end", resolve);
    readable.once("error", reject);
    readable.once("close", resolve);
  });
}

export function waitForStreamToEnd(stream: Readable): Promise<void> {
  return new Promise((resolve, reject) => {
    let cleanup: (() => void) | undefined = undefined;
    const onError = (err: Error) => {
      cleanup?.();
      reject(err);
    };
    const onEnd = () => {
      cleanup?.();
      resolve();
    };
    const onClose = () => {
      cleanup?.();
      reject(new Error("Stream closed before end"));
    };
    cleanup = () => {
      stream.off("error", onError);
      stream.off("end", onEnd);
      stream.off("close", onClose);
    };

    stream.once("error", onError);
    stream.once("end", onEnd);
    stream.once("close", onClose);
  });
}
