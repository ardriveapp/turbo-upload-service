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
export class CircularBuffer {
  private writeOffset = 0;
  private readIndex = 0;
  private _remainingCapacity: number;
  private buffer: Buffer;

  constructor(
    readonly maxCapacity: number,
    backingBuffer?: {
      buffer: Buffer;
      usedCapacity?: number;
    }
  ) {
    if (maxCapacity < 1 || !Number.isInteger(maxCapacity)) {
      throw new Error("maxCapacity must be a positive integer number!");
    }
    if (backingBuffer) {
      if (backingBuffer.buffer.byteLength < maxCapacity) {
        throw new Error(
          "Backing buffer must be at least as large as maxCapacity!"
        );
      }
      if (backingBuffer.usedCapacity) {
        if (
          backingBuffer.usedCapacity < 0 ||
          !Number.isInteger(backingBuffer.usedCapacity)
        ) {
          throw new Error(
            "usedCapacity must be zero or a positive integer number!"
          );
        }
        if (backingBuffer.usedCapacity > maxCapacity) {
          throw new Error("usedCapacity can't be larger than maxCapacity!");
        }
      }
    }

    this.buffer = backingBuffer?.buffer ?? Buffer.alloc(maxCapacity);
    this._remainingCapacity = maxCapacity - (backingBuffer?.usedCapacity ?? 0);
  }

  get remainingCapacity(): number {
    return this._remainingCapacity;
  }

  get usedCapacity(): number {
    return this.maxCapacity - this._remainingCapacity;
  }

  toString(): string {
    const bytesWritten = this.usedCapacity;
    if (bytesWritten === 0) {
      return "";
    }

    const outputBuffer = Buffer.alloc(bytesWritten);

    // Either the src buffer can be read from contiguously or two reads will have to be performed
    if (this.maxCapacity - this.readIndex >= bytesWritten) {
      outputBuffer.set(
        this.buffer.slice(this.readIndex, this.readIndex + bytesWritten),
        0
      );
    } else {
      const firstReadNumBytes = this.maxCapacity - this.readIndex;
      const secondReadNumBytes = bytesWritten - firstReadNumBytes;
      outputBuffer.set(
        this.buffer.slice(this.readIndex, this.readIndex + firstReadNumBytes),
        0
      );
      outputBuffer.set(
        this.buffer.slice(0, secondReadNumBytes),
        firstReadNumBytes
      );
    }

    return outputBuffer.toString();
  }

  rawBuffer(): Buffer {
    return this.buffer.slice();
  }

  writeFrom({
    srcBuffer,
    srcOffset = 0,
    numBytes,
  }: {
    srcBuffer: Buffer;
    srcOffset?: number;
    numBytes?: number;
  }) {
    numBytes = numBytes ?? srcBuffer.length - srcOffset;
    if (numBytes > this._remainingCapacity) {
      throw new Error("CircularBuffer overflow");
    }

    if (numBytes < 1 || !Number.isInteger(numBytes)) {
      throw new Error("numBytes must be a positive integer!");
    }

    // Either all the bytes can be written contiguously OR two separate writes must be performed
    if (this.maxCapacity - this.writeOffset >= numBytes) {
      this.buffer.set(
        srcBuffer.slice(srcOffset, srcOffset + numBytes),
        this.writeOffset
      );
      this.writeOffset += numBytes;

      // If we've written to the very end of the buffer, set the offset back to 0
      if (this.writeOffset === this.maxCapacity) {
        this.writeOffset = 0;
      }
    } else {
      const firstWriteNumBytes = this.maxCapacity - this.writeOffset;
      const secondWriteNumBytes = numBytes - firstWriteNumBytes;
      this.buffer.set(
        srcBuffer.slice(srcOffset, srcOffset + firstWriteNumBytes),
        this.writeOffset
      );
      this.buffer.set(
        srcBuffer.slice(
          srcOffset + firstWriteNumBytes,
          srcOffset + firstWriteNumBytes + secondWriteNumBytes
        ),
        0
      );
      this.writeOffset = secondWriteNumBytes;
    }
    this._remainingCapacity -= numBytes;
  }

  readInto({
    destBuffer,
    destOffset,
    numBytes,
  }: {
    destBuffer: Buffer;
    destOffset?: number;
    numBytes: number;
  }) {
    if (numBytes > this.usedCapacity) {
      throw new Error("CircularBuffer underflow!");
    }

    if (numBytes < 1 || !Number.isInteger(numBytes)) {
      throw new Error("numBytes must be a positive integer!");
    }

    // Either the src buffer can be read from contiguously or two reads will have to be performed
    if (this.maxCapacity - this.readIndex >= numBytes) {
      destBuffer.set(
        this.buffer.slice(this.readIndex, this.readIndex + numBytes),
        destOffset
      );

      this.readIndex += numBytes;

      // Return to index 0 if we read to the very end of the buffer
      if (this.readIndex === this.maxCapacity) {
        this.readIndex = 0;
      }
    } else {
      const firstReadNumBytes = this.maxCapacity - this.readIndex;
      const secondReadNumBytes = numBytes - firstReadNumBytes;
      destBuffer.set(
        this.buffer.slice(this.readIndex, this.readIndex + firstReadNumBytes),
        destOffset
      );
      destBuffer.set(
        this.buffer.slice(0, secondReadNumBytes),
        (destOffset ?? 0) + firstReadNumBytes
      );
      this.readIndex = secondReadNumBytes;
    }
    this._remainingCapacity += numBytes;
  }

  shift(numBytes: number): Buffer {
    if (numBytes > this.usedCapacity) {
      throw new Error("CircularBuffer underflow!");
    }

    if (numBytes < 1 || !Number.isInteger(numBytes)) {
      throw new Error("numBytes must be a positive integer!");
    }

    let destBuffer: Buffer;

    // Either the src buffer can be sliced from contiguously or two reads will have to be performed into a new Buffer
    if (this.maxCapacity - this.readIndex >= numBytes) {
      destBuffer = this.buffer.slice(this.readIndex, this.readIndex + numBytes);
      this.readIndex += numBytes;

      // Return to index 0 if we read to the very end of the buffer
      if (this.readIndex === this.maxCapacity) {
        this.readIndex = 0;
      }
    } else {
      destBuffer = Buffer.alloc(numBytes);
      const firstReadNumBytes = this.maxCapacity - this.readIndex;
      const secondReadNumBytes = numBytes - firstReadNumBytes;
      destBuffer.set(
        this.buffer.slice(this.readIndex, this.readIndex + firstReadNumBytes),
        0
      );
      destBuffer.set(
        this.buffer.slice(0, secondReadNumBytes),
        firstReadNumBytes
      );
      this.readIndex = secondReadNumBytes;
    }
    this._remainingCapacity += numBytes;
    return destBuffer;
  }

  unshift({
    srcBuffer,
    srcOffset = 0,
    numBytes,
  }: {
    srcBuffer: Buffer;
    srcOffset?: number;
    numBytes?: number;
  }) {
    numBytes = numBytes ?? srcBuffer.length - srcOffset;
    if (numBytes > this._remainingCapacity) {
      throw new Error("CircularBuffer overflow");
    }

    if (numBytes < 1 || !Number.isInteger(numBytes)) {
      throw new Error("numBytes must be a positive integer!");
    }

    // Either all the bytes can be written contiguously OR two separate writes must be performed
    if (this.readIndex - numBytes >= 0) {
      this.buffer.set(
        srcBuffer.slice(srcOffset, srcOffset + numBytes),
        this.readIndex - numBytes
      );
      this.readIndex -= numBytes;
    } else if (this.readIndex === 0) {
      this.buffer.set(
        srcBuffer.slice(srcOffset, srcOffset + numBytes),
        this.maxCapacity - numBytes
      );
      this.readIndex = this.maxCapacity - numBytes;
    } else {
      const firstWriteNumBytes = this.readIndex;
      const secondWriteNumBytes = numBytes - firstWriteNumBytes;
      this.buffer.set(
        srcBuffer.slice(numBytes - firstWriteNumBytes, numBytes),
        0
      );
      this.buffer.set(
        srcBuffer.slice(0, secondWriteNumBytes),
        this.maxCapacity - secondWriteNumBytes
      );
      this.readIndex = this.maxCapacity - firstWriteNumBytes;
    }
    this._remainingCapacity -= numBytes;
  }
}
