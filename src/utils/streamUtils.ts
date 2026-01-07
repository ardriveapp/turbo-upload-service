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
import { PassThrough, Readable, once } from "stream";
import winston from "winston";

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

export async function drainStream(
  readable: Readable,
  logger: winston.Logger | undefined = undefined
): Promise<void> {
  return new Promise((resolve) => {
    let finished = false;
    const onData = () => {
      // discard chunks
    };
    const done = () => {
      if (finished) return;
      finished = true;
      readable.off("data", onData);
      resolve();
    };
    readable.on("data", onData);
    readable.once("end", done);
    readable.once("error", done); // resolve on error to avoid unhandled rejection in fire-and-forget usage
    readable.once("close", done);
    try {
      readable.resume(); // ensure the stream is flowing
    } catch (error) {
      logger?.error("Error while resuming stream in drainStream", error);
      done();
    }
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

/**
 * A tiny in-memory byte queue. Helpful when you need to coalesce a few small
 * buffers across boundaries without reallocating large intermediate buffers.
 */
export class ByteQueue {
  private queue: Buffer[] = [];
  private _length = 0;

  get length(): number {
    return this._length;
  }

  push(buf: Buffer) {
    if (!buf || buf.length === 0) return;
    this.queue.push(buf);
    this._length += buf.length;
  }

  /**
   * Consume up to n bytes from the front of the queue and return them in a
   * single Buffer. If fewer than n bytes are available, returns all available.
   */
  consume(n: number): Buffer {
    if (n <= 0 || this._length === 0) return Buffer.alloc(0);
    const toRead = Math.min(n, this._length);
    const out = Buffer.allocUnsafe(toRead);
    let offset = 0;
    while (offset < toRead) {
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const head = this.queue[0]!;
      const remaining = toRead - offset;
      if (head.length <= remaining) {
        head.copy(out, offset);
        offset += head.length;
        this.queue.shift();
        this._length -= head.length;
      } else {
        head.copy(out, offset, 0, remaining);
        this.queue[0] = head.subarray(remaining);
        offset += remaining;
        this._length -= remaining;
      }
    }
    return out;
  }

  /** Remove exactly n bytes and return a new Buffer (copy). Throws if fewer available. */
  shiftExactly(n: number): Buffer {
    if (n === 0) return Buffer.alloc(0);
    if (n > this._length) throw new Error("EOF");
    const out = Buffer.allocUnsafe(n);
    let off = 0;
    while (off < n) {
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      const head = this.queue[0]!;
      const take = Math.min(head.length, n - off);
      head.copy(out, off, 0, take);
      if (take === head.length) this.queue.shift();
      else this.queue[0] = head.subarray(take);
      off += take;
      this._length -= take;
    }
    return out;
  }

  /** Drain remaining queued bytes without copying (as slices) */
  drainSlices(): Buffer[] {
    const out = this.queue;
    this.queue = [];
    this._length = 0;
    return out;
  }
}

/**
 * A minimal lookahead reader that preserves a stream's paused/flowing state
 * and provides a convenience to read exactly N bytes.
 */
export class LookaheadReader {
  private q = new ByteQueue();
  private ended = false;
  private error: Error | null = null;
  private waiters: Array<() => void> = [];

  constructor(private readonly stream: Readable) {
    this.stream.pause();
    this.attach();
  }

  private onReadable = () => {
    let c: Buffer | null;
    while ((c = this.stream.read() as Buffer | null) !== null) {
      this.q.push(c);
    }
    this.resolve();
  };
  private onEnd = () => {
    this.ended = true;
    this.resolve();
  };
  private onError = (e: Error) => {
    this.error = e;
    this.resolve();
  };

  private attach() {
    this.stream.on("readable", this.onReadable);
    this.stream.once("end", this.onEnd);
    this.stream.once("error", this.onError);
  }
  private detach() {
    this.stream.removeListener("readable", this.onReadable);
    this.stream.removeListener("end", this.onEnd);
    this.stream.removeListener("error", this.onError);
  }
  private resolve() {
    for (const w of this.waiters.splice(0)) w();
  }

  private async ensure(n: number) {
    while (this.q.length < n && !this.ended && !this.error) {
      await new Promise<void>((r) => this.waiters.push(r));
    }
    if (this.error) throw this.error;
    if (this.q.length < n && this.ended) throw new Error("EOF");
  }

  async readExactly(n: number): Promise<Buffer> {
    if (n <= 0) return Buffer.alloc(0);
    await this.ensure(n);
    return this.q.shiftExactly(n);
  }

  releaseToRest(): Readable {
    this.detach();
    const rest = new PassThrough();
    for (const s of this.q.drainSlices()) rest.write(s);
    this.stream.pipe(rest, { end: true });
    return rest;
  }
}

/**
 * Ensure a stream is paused for lookahead parsing during the provided async
 * callback. The stream's prior paused/flowing state is restored afterwards.
 */
export async function withLookahead<T>(
  src: Readable,
  parseFn: (r: LookaheadReader) => Promise<T>
): Promise<{ result: T; rest: Readable }> {
  const r = new LookaheadReader(src);
  const result = await parseFn(r);
  const rest = r.releaseToRest();
  return { result, rest };
}

/**
 * Create a Readable that yields exactly `n` bytes from `source`, then ends.
 * Any overflow bytes from the last chunk are unshifted back onto `source` so
 * subsequent consumers see a consistent stream starting at the next byte.
 *
 * - Preserves and restores the source stream's paused/flowing state.
 * - Handles backpressure by using the consumer's demand on the returned stream.
 */
export function splitReadable(
  source: Readable,
  n: number
): { head: Readable; tail: Readable } {
  const head = new Readable({
    read() {
      // We push from source 'data' events.
    },
  });
  const tail = new Readable({
    read() {
      // We will push into this from source as well.
    },
  });

  if (n <= 0) {
    queueMicrotask(() => head.push(null));
    // Forward all source data to tail
    const wasPausedZero = source.isPaused();
    /* eslint-disable prefer-const */
    let onDataZero: (chunk: Buffer) => void;
    let onEndZero: () => void;
    let onErrorZero: (err: Error) => void;
    /* eslint-enable prefer-const */
    const cleanupZero = () => {
      source.off("data", onDataZero);
      source.off("end", onEndZero);
      source.off("error", onErrorZero);
      if (wasPausedZero) {
        try {
          source.pause();
        } catch {
          // ignore
        }
      }
    };
    onDataZero = (chunk: Buffer) => tail.push(chunk);
    onEndZero = () => {
      cleanupZero();
      tail.push(null);
    };
    onErrorZero = (err: Error) => {
      cleanupZero();
      head.destroy(err);
      tail.destroy(err);
    };
    source.on("data", onDataZero);
    source.once("end", onEndZero);
    source.once("error", onErrorZero);
    if (wasPausedZero) source.resume();
    return { head, tail };
  }

  let remaining = n;
  const wasPaused = source.isPaused();

  /* eslint-disable prefer-const */
  let onDataFn: (chunk: Buffer) => void;
  let onEndFn: () => void;
  let onErrorFn: (err: Error) => void;
  /* eslint-enable prefer-const */

  function cleanup() {
    source.off("data", onDataFn);
    source.off("end", onEndFn);
    source.off("error", onErrorFn);
    if (wasPaused) {
      try {
        source.pause();
      } catch {
        // ignore
      }
    }
  }

  function endHead() {
    head.push(null);
  }

  onDataFn = function onData(chunk: Buffer) {
    if (remaining > 0) {
      if (chunk.length <= remaining) {
        remaining -= chunk.length;
        head.push(chunk);
        if (remaining === 0) {
          endHead();
        }
      } else {
        // Split the chunk between head and tail
        head.push(chunk.subarray(0, remaining));
        tail.push(chunk.subarray(remaining));
        remaining = 0;
        endHead();
      }
    } else {
      // After head is satisfied, forward everything to tail
      tail.push(chunk);
    }
  };

  onEndFn = function onEnd() {
    cleanup();
    if (remaining > 0) {
      // Source ended early; close head regardless
      endHead();
    }
    tail.push(null);
  };

  onErrorFn = function onError(err: Error) {
    cleanup();
    head.destroy(err);
    tail.destroy(err);
  };

  source.on("data", onDataFn);
  source.once("end", onEndFn);
  source.once("error", onErrorFn);
  if (wasPaused) source.resume();

  return { head, tail };
}

/**
 * Read exactly `n` bytes from the stream, returning a clean Readable stream that will
 * lead with any leftover bytes that were consumed beyond `n` from the input stream
 */
export async function readExactly(
  stream: Readable,
  n: number
): Promise<{ bytes: Buffer; rest: Readable }> {
  // New behavior: return bytes and a clean rest stream
  const { result, rest } = await withLookahead(stream, async (reader) => {
    const bytes = await reader.readExactly(n);
    return bytes;
  });
  return { bytes: result, rest };
}

export async function waitForStreamEndOrError(stream: Readable): Promise<void> {
  if (stream.readableEnded || stream.destroyed) return;
  await Promise.race([once(stream, "end"), once(stream, "error")]);
}
