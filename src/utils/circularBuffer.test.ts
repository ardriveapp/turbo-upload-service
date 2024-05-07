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

import { CircularBuffer } from "./circularBuffer";

describe("CircularBuffer class", () => {
  describe("constructor", () => {
    it("creates a CircularBuffer with the expected capacity", () => {
      const buffer = new CircularBuffer(1);
      expect(buffer.remainingCapacity).to.equal(1);
      expect(buffer.maxCapacity).to.equal(1);
    });

    it("throws an exception when maxCapacity is 0", () => {
      expect(() => {
        new CircularBuffer(0);
      }).to.throw;
    });

    it("throws an exception when maxCapacity is not an integer", () => {
      expect(() => {
        new CircularBuffer(1.1);
      }).to.throw;
    });

    it("throws an exception when maxCapacity exceeds the size of the provided backing buffer", () => {
      expect(() => {
        new CircularBuffer(2, { buffer: Buffer.alloc(1) });
      }).to.throw;
    });

    it("throws an exception when usedCapacity exceeds maxCapacity", () => {
      expect(() => {
        new CircularBuffer(2, { buffer: Buffer.alloc(2), usedCapacity: 3 });
      }).to.throw;
    });
  });

  describe("toString function", () => {
    it("returns an empty string when nothing has been written", () => {
      expect(new CircularBuffer(1).toString()).to.equal("");
    });

    it("returns the contiguous data written to the buffer within the ring boundary", () => {
      const buffer = new CircularBuffer(4);
      buffer.writeFrom({
        srcBuffer: Buffer.from("bar"),
      });
      expect(buffer.toString()).to.equal("bar");
    });

    it("returns the contiguous data written to the buffer across the ring boundary", () => {
      const buffer = new CircularBuffer(3);
      buffer.writeFrom({
        srcBuffer: Buffer.from("bar"),
      });
      buffer.readInto({
        destBuffer: Buffer.alloc(2),
        numBytes: 2,
      });
      buffer.writeFrom({
        srcBuffer: Buffer.from("fo"),
      });
      expect(buffer.toString()).to.equal("rfo");
    });
  });

  describe("rawBuffer function", () => {
    it("returns an empty Buffer when nothing has been written", () => {
      expect(new CircularBuffer(1).rawBuffer()).to.deep.equal(Buffer.alloc(1));
    });

    it("returns the data in the order it was written when written within the ring boundary", () => {
      const buffer = new CircularBuffer(3);
      buffer.writeFrom({
        srcBuffer: Buffer.from("bar"),
      });
      expect(buffer.rawBuffer().toString()).to.equal("bar");
    });

    it("returns the data in the linear order of the buffer when it was written across the ring boundary", () => {
      const buffer = new CircularBuffer(3);
      buffer.writeFrom({
        srcBuffer: Buffer.from("bar"),
      });
      buffer.readInto({
        destBuffer: Buffer.alloc(2),
        numBytes: 2,
      });
      buffer.writeFrom({
        srcBuffer: Buffer.from("fo"),
      });
      expect(buffer.rawBuffer().toString()).to.equal("for");
    });
  });

  describe("usedCapacity function", () => {
    it("returns 0 when nothing has been written", () => {
      expect(new CircularBuffer(1).usedCapacity).to.equal(0);
    });

    it("returns 1 when a byte has been written", () => {
      const buffer = new CircularBuffer(1);
      buffer.writeFrom({
        srcBuffer: Buffer.from("x"),
      });
      expect(buffer.usedCapacity).to.equal(1);
    });
  });

  describe("writeFrom function", () => {
    it("throws when anything but a positive integer number of bytes are specified for writing", () => {
      const buffer = new CircularBuffer(3);
      const srcBuffer = Buffer.from("01");
      expect(() => {
        buffer.writeFrom({
          srcBuffer,
          numBytes: 0,
        });
      }).to.throw;
      expect(() => {
        buffer.writeFrom({
          srcBuffer,
          numBytes: 1.1,
        });
      }).to.throw;
    });

    describe("with maxCapacity available", () => {
      let buffer: CircularBuffer;
      const maxCapacity = 3;
      beforeEach(() => {
        buffer = new CircularBuffer(maxCapacity);
      });

      it("can write fewer bytes than maxCapacity", () => {
        const srcBuffer = Buffer.from("01");
        buffer.writeFrom({
          srcBuffer,
        });
        expect(buffer.remainingCapacity).to.equal(1);
        expect(buffer.maxCapacity).to.equal(maxCapacity);
      });

      it("can write as many bytes as maxCapacity", () => {
        const srcBuffer = Buffer.from("012");
        buffer.writeFrom({
          srcBuffer,
        });
        expect(buffer.remainingCapacity).to.equal(0);
        expect(buffer.maxCapacity).to.equal(maxCapacity);
      });

      it("can write multiple times up to the maxCapacity", () => {
        buffer.writeFrom({
          srcBuffer: Buffer.from("01"),
        });
        expect(buffer.remainingCapacity).to.equal(1);
        buffer.writeFrom({
          srcBuffer: Buffer.from("2"),
        });
        expect(buffer.remainingCapacity).to.equal(0);
        expect(buffer.maxCapacity).to.equal(maxCapacity);
      });

      it("supports writing from a source buffer offset", () => {
        buffer.writeFrom({
          srcBuffer: Buffer.from("012"),
          srcOffset: 1,
        });
        expect(buffer.remainingCapacity).to.equal(1);
      });

      it("supports writing fewer than all of the bytes from a source buffer", () => {
        buffer.writeFrom({
          srcBuffer: Buffer.from("012"),
          numBytes: 2,
        });
        expect(buffer.remainingCapacity).to.equal(1);
      });

      it("supports writing fewer than all of the bytes from an offset in a source buffer", () => {
        buffer.writeFrom({
          srcBuffer: Buffer.from("012"),
          srcOffset: 1,
          numBytes: 1,
        });
        expect(buffer.remainingCapacity).to.equal(2);
      });

      describe("when writing around the ring", () => {
        it("writes to the start of the buffer when last write ended at end of the buffer", () => {
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          const zeroesBuffer = Buffer.alloc(buffer.maxCapacity, "0");
          buffer.writeFrom({
            srcBuffer: zeroesBuffer,
          });
          expect(buffer.remainingCapacity).to.equal(0);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          expect(buffer.toString()).to.equal("000");
          const dumpBuffer = Buffer.alloc(2);
          buffer.readInto({
            destBuffer: dumpBuffer,
            numBytes: dumpBuffer.byteLength,
          });
          expect(buffer.remainingCapacity).to.equal(dumpBuffer.byteLength);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          const srcBuffer = Buffer.from("ab");
          buffer.writeFrom({
            srcBuffer,
          });
          expect(buffer.remainingCapacity).to.equal(0);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("ab0");
          expect(buffer.toString()).to.equal("0ab");
        });

        it("writes to then end of the buffer and then back around to the beginning of the buffer when there is capacity", () => {
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          const onesBuffer = Buffer.alloc(buffer.maxCapacity - 1, "1");
          buffer.writeFrom({
            srcBuffer: onesBuffer,
          });
          expect(buffer.remainingCapacity).to.equal(1);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          expect(buffer.toString()).to.equal("11");
          const dumpBuffer = Buffer.alloc(1);
          buffer.readInto({
            destBuffer: dumpBuffer,
            numBytes: dumpBuffer.byteLength,
          });
          expect(dumpBuffer.toString()).to.equal("1");
          expect(buffer.remainingCapacity).to.equal(1 + dumpBuffer.byteLength);
          expect(buffer.toString()).to.equal("1");
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          const srcBuffer = Buffer.from("ab");
          buffer.writeFrom({
            srcBuffer,
          });
          expect(buffer.remainingCapacity).to.equal(0);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("b1a");
          expect(buffer.toString()).to.equal("1ab");
        });

        it("should throw when writing more data than is available in the buffer", () => {
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          const onesBuffer = Buffer.alloc(buffer.maxCapacity - 1, "1");
          buffer.writeFrom({
            srcBuffer: onesBuffer,
          });
          expect(buffer.remainingCapacity).to.equal(1);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          expect(buffer.toString()).to.equal("11");
          const dumpBuffer = Buffer.alloc(1);
          buffer.readInto({
            destBuffer: dumpBuffer,
            numBytes: dumpBuffer.byteLength,
          });
          expect(dumpBuffer.toString()).to.equal("1");
          expect(buffer.remainingCapacity).to.equal(1 + dumpBuffer.byteLength);
          expect(buffer.toString()).to.equal("1");
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          const srcBuffer = Buffer.from("abc");
          expect(() => {
            buffer.writeFrom({
              srcBuffer,
            });
          }).to.throw;
        });
      });
    });
  });

  describe("readInto function", () => {
    it("throws when no bytes have been written", () => {
      const buffer = new CircularBuffer(1);
      expect(() => {
        buffer.readInto({
          destBuffer: Buffer.alloc(1),
          numBytes: 1,
        });
      }).to.throw;
    });

    describe("with bytes available for reading", () => {
      let buffer: CircularBuffer;
      const maxCapacity = 3;
      beforeEach(() => {
        buffer = new CircularBuffer(maxCapacity);
        buffer.writeFrom({
          srcBuffer: Buffer.from("012"),
        });
      });

      it("throws when more data is requested than has been written to the buffer", () => {
        expect(() => {
          const destBuffer = Buffer.alloc(4);
          buffer.readInto({
            destBuffer,
            numBytes: 4,
          });
        }).to.throw;
      });

      it("throws when anything other than a positive integer number of bytes are requested", () => {
        expect(() => {
          const destBuffer = Buffer.alloc(2);
          buffer.readInto({
            destBuffer,
            numBytes: 0,
          });
        }).to.throw;
        expect(() => {
          const destBuffer = Buffer.alloc(4);
          buffer.readInto({
            destBuffer,
            numBytes: 1.1,
          });
        }).to.throw;
      });

      it("throws when more bytes are requested than maxCapacity", () => {
        expect(() => {
          buffer.readInto({
            destBuffer: Buffer.alloc(maxCapacity + 1),
            numBytes: maxCapacity + 1,
          });
        }).to.throw;
      });

      it("can read all available written bytes into a destination buffer", () => {
        const destBuffer = Buffer.alloc(maxCapacity);
        buffer.readInto({
          destBuffer: destBuffer,
          numBytes: maxCapacity,
        });
        expect(buffer.remainingCapacity).to.equal(maxCapacity);
        expect(buffer.maxCapacity).to.equal(maxCapacity);
        expect(destBuffer.toString()).to.equal("012");
      });

      it("can read fewer than the available written bytes into a destination buffer", () => {
        const destBuffer = Buffer.alloc(maxCapacity - 1);
        buffer.readInto({
          destBuffer: destBuffer,
          numBytes: destBuffer.byteLength,
        });
        expect(buffer.remainingCapacity).to.equal(destBuffer.byteLength);
        expect(buffer.maxCapacity).to.equal(maxCapacity);
        expect(destBuffer.toString()).to.equal("01");
      });

      it("can read into a destination buffer at an offset", () => {
        const destBuffer = Buffer.alloc(maxCapacity);
        destBuffer.set(Buffer.from("abc"));
        buffer.readInto({
          destBuffer: destBuffer,
          destOffset: 1,
          numBytes: 1,
        });
        expect(buffer.remainingCapacity).to.equal(1);
        expect(buffer.maxCapacity).to.equal(maxCapacity);
        expect(destBuffer.toString()).to.equal("a0c");
      });

      describe("when reading around the ring", () => {
        beforeEach(() => {
          const dumpBuffer = Buffer.alloc(maxCapacity - 1);
          buffer.readInto({
            destBuffer: dumpBuffer,
            numBytes: dumpBuffer.byteLength,
          });
          buffer.writeFrom({
            srcBuffer: Buffer.from("a"),
          });
          expect(buffer.remainingCapacity).to.equal(1);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("a12");
          expect(buffer.toString()).to.equal("2a");
        });

        it("reads max capacity bytes to end of buffer then performs next read from start of buffer", () => {
          let destBuffer = Buffer.alloc(maxCapacity - 1);
          buffer.readInto({
            destBuffer: destBuffer,
            numBytes: destBuffer.byteLength,
          });
          expect(destBuffer.toString()).to.equal("2a");
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("a12");
          expect(buffer.toString()).to.equal("");
          buffer.writeFrom({
            srcBuffer: Buffer.from("-"),
          });
          destBuffer = Buffer.alloc(1);
          buffer.readInto({
            destBuffer: destBuffer,
            numBytes: destBuffer.byteLength,
          });
          expect(destBuffer.toString()).to.equal("-");
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("a-2");
          expect(buffer.toString()).to.equal("");
        });

        it("can read bytes around the edge of the ring", () => {
          const destBuffer = Buffer.alloc(2);
          buffer.readInto({
            destBuffer: destBuffer,
            numBytes: destBuffer.byteLength,
          });
          expect(destBuffer.toString()).to.equal("2a");
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("a12");
          expect(buffer.toString()).to.equal("");
        });

        it("should throw when reading more bytes than are written", () => {
          expect(() => {
            const destBuffer = Buffer.alloc(3);
            buffer.readInto({
              destBuffer: destBuffer,
              numBytes: destBuffer.byteLength,
            });
          }).to.throw();
        });
      });
    });
  });

  describe("shift function", () => {
    it("throws when no data has been written to the buffer", () => {
      expect(() => {
        new CircularBuffer(1).shift(1);
      }).to.throw;
    });

    describe("when data has been written to the buffer", () => {
      let buffer: CircularBuffer;
      const maxCapacity = 3;

      beforeEach(() => {
        // Use a backing buffer here for test coverage help
        buffer = new CircularBuffer(maxCapacity, {
          buffer: Buffer.alloc(maxCapacity + 1),
        });
        expect(buffer.maxCapacity).to.equal(maxCapacity);
        buffer.writeFrom({
          srcBuffer: Buffer.from("ab"),
        });
      });

      it("throws when more data is requested than has been written to the buffer", () => {
        expect(() => {
          buffer.shift(3);
        }).to.throw;
      });

      it("throws when anything other than a positive integer number of bytes are requested", () => {
        expect(() => {
          buffer.shift(0);
        }).to.throw;
        expect(() => {
          buffer.shift(1.1);
        }).to.throw;
      });

      it("returns the expected buffer and restores capacity to the buffer when fewer than all written bytes are requested", () => {
        expect(buffer.usedCapacity).to.equal(2);
        const returnedBuffer = buffer.shift(1);
        expect(buffer.usedCapacity).to.equal(1);
        expect(buffer.maxCapacity).to.equal(3);
        expect(returnedBuffer.toString()).to.equal("a");
      });

      it("returns the expected buffer and restores capacity to the buffer when all written bytes are requested", () => {
        expect(buffer.usedCapacity).to.equal(2);
        const returnedBuffer = buffer.shift(2);
        expect(buffer.usedCapacity).to.equal(0);
        expect(buffer.maxCapacity).to.equal(3);
        expect(returnedBuffer.toString()).to.equal("ab");
      });

      describe("when dequeuing around the ring", () => {
        beforeEach(() => {
          buffer = new CircularBuffer(maxCapacity);
          buffer.writeFrom({
            srcBuffer: Buffer.from("012"),
          });
          const dumpBuffer = Buffer.alloc(maxCapacity - 1);
          buffer.readInto({
            destBuffer: dumpBuffer,
            numBytes: dumpBuffer.byteLength,
          });
          buffer.writeFrom({
            srcBuffer: Buffer.from("a"),
          });
          expect(buffer.remainingCapacity).to.equal(1);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("a12");
          expect(buffer.toString()).to.equal("2a");
        });

        it("reads max capacity bytes to end of buffer then performs next read from start of buffer", () => {
          let returnedBuffer = buffer.shift(maxCapacity - 1);
          expect(returnedBuffer.toString()).to.equal("2a");
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("a12");
          expect(buffer.toString()).to.equal("");
          buffer.writeFrom({
            srcBuffer: Buffer.from("-"),
          });
          returnedBuffer = buffer.shift(1);
          expect(returnedBuffer.toString()).to.equal("-");
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("a-2");
          expect(buffer.toString()).to.equal("");
        });

        it("can read bytes around the edge of the ring", () => {
          const returnedBuffer = buffer.shift(2);
          expect(returnedBuffer.toString()).to.equal("2a");
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("a12");
          expect(buffer.toString()).to.equal("");
        });

        it("should throw when reading more bytes than are written", () => {
          expect(() => {
            buffer.shift(3);
          }).to.throw();
        });
      });
    });
  });

  describe("unshift function", () => {
    it("throws when anything but a positive integer number of bytes are specified for writing", () => {
      const buffer = new CircularBuffer(3);
      const srcBuffer = Buffer.from("01");
      expect(() => {
        buffer.writeFrom({
          srcBuffer,
          numBytes: 0,
        });
      }).to.throw;
      expect(() => {
        buffer.writeFrom({
          srcBuffer,
          numBytes: 1.1,
        });
      }).to.throw;
    });

    describe("with maxCapacity available", () => {
      let buffer: CircularBuffer;
      const maxCapacity = 3;
      beforeEach(() => {
        buffer = new CircularBuffer(maxCapacity);
      });

      it("can write fewer bytes than maxCapacity", () => {
        const srcBuffer = Buffer.from("01");
        buffer.unshift({
          srcBuffer,
        });
        expect(buffer.remainingCapacity).to.equal(1);
        const expectedRawBuffer = Buffer.alloc(maxCapacity);
        expectedRawBuffer.set(Buffer.from("01"), 1);
        expect(buffer.rawBuffer()).to.deep.equal(expectedRawBuffer);
        expect(buffer.toString()).to.equal("01");
        expect(buffer.maxCapacity).to.equal(maxCapacity);
      });

      it("can write as many bytes as maxCapacity", () => {
        const srcBuffer = Buffer.from("012");
        buffer.unshift({
          srcBuffer,
        });
        expect(buffer.remainingCapacity).to.equal(0);
        expect(buffer.rawBuffer().toString()).to.equal("012");
        expect(buffer.toString()).to.equal("012");
        expect(buffer.maxCapacity).to.equal(maxCapacity);
      });

      it("can write multiple times up to the maxCapacity", () => {
        buffer.unshift({
          srcBuffer: Buffer.from("2"),
        });
        expect(buffer.remainingCapacity).to.equal(2);
        buffer.unshift({
          srcBuffer: Buffer.from("01"),
        });
        expect(buffer.remainingCapacity).to.equal(0);
        expect(buffer.rawBuffer().toString()).to.equal("012");
        expect(buffer.toString()).to.equal("012");
        expect(buffer.maxCapacity).to.equal(maxCapacity);
      });

      it("supports writing from a source buffer offset", () => {
        buffer.unshift({
          srcBuffer: Buffer.from("012"),
          srcOffset: 1,
        });
        const expectedRawBuffer = Buffer.alloc(maxCapacity);
        expectedRawBuffer.set(Buffer.from("12"), 1);
        expect(buffer.rawBuffer()).to.deep.equal(expectedRawBuffer);
        expect(buffer.toString()).to.equal("12");
        expect(buffer.remainingCapacity).to.equal(1);
      });

      it("supports writing fewer than all of the bytes from a source buffer", () => {
        buffer.unshift({
          srcBuffer: Buffer.from("012"),
          numBytes: 2,
        });
        const expectedRawBuffer = Buffer.alloc(maxCapacity);
        expectedRawBuffer.set(Buffer.from("01"), 1);
        expect(buffer.rawBuffer()).to.deep.equal(expectedRawBuffer);
        expect(buffer.toString()).to.equal("01");
        expect(buffer.remainingCapacity).to.equal(1);
      });

      it("supports writing fewer than all of the bytes from an offset in a source buffer", () => {
        buffer.unshift({
          srcBuffer: Buffer.from("012"),
          srcOffset: 1,
          numBytes: 1,
        });
        const expectedRawBuffer = Buffer.alloc(maxCapacity);
        expectedRawBuffer.set(Buffer.from("1"), 2);
        expect(buffer.rawBuffer()).to.deep.equal(expectedRawBuffer);
        expect(buffer.toString()).to.equal("1");
        expect(buffer.remainingCapacity).to.equal(2);
      });

      describe("when unshifting around the ring", () => {
        it("unshifts onto the end of the buffer when nothing is yet written", () => {
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          expect(buffer.rawBuffer().toString()).to.equal("\u0000\u0000\u0000");
          expect(buffer.toString()).to.equal("");
          buffer.unshift({ srcBuffer: Buffer.from("12") });
          expect(buffer.remainingCapacity).to.equal(1);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          expect(buffer.toString()).to.equal("12");
          expect(buffer.rawBuffer().toString()).to.equal("\u000012");
        });

        it("unshifts onto the end of the buffer when data was written but not yet read", () => {
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          buffer.writeFrom({ srcBuffer: Buffer.from("0") });
          expect(buffer.rawBuffer().toString()).to.equal("0\u0000\u0000");
          expect(buffer.toString()).to.equal("0");
          buffer.unshift({ srcBuffer: Buffer.from("12") });
          expect(buffer.remainingCapacity).to.equal(0);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          expect(buffer.toString()).to.equal("120");
          expect(buffer.rawBuffer().toString()).to.equal("012");
        });

        it("unshifts from the beginning of the buffer to the read index and unshifts the remainder into the end of the buffer when there is capacity", () => {
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          buffer.writeFrom({
            srcBuffer: Buffer.from("ab"),
          });
          buffer.shift(1);
          // Shifted data will still be in the buffer but can now be overwritten
          expect(buffer.rawBuffer().toString()).to.equal("ab\u0000");
          expect(buffer.remainingCapacity).to.equal(2);
          buffer.unshift({ srcBuffer: Buffer.from("12") });
          expect(buffer.remainingCapacity).to.equal(0);
          expect(buffer.maxCapacity).to.equal(maxCapacity);
          expect(buffer.toString()).to.equal("12b");
          expect(buffer.rawBuffer().toString()).to.equal("2b1");
        });

        it("should throw when unshifting more data than is available in the buffer", () => {
          expect(buffer.remainingCapacity).to.equal(maxCapacity);
          const onesBuffer = Buffer.alloc(buffer.maxCapacity, "1");
          buffer.unshift({
            srcBuffer: onesBuffer,
          });
          const srcBuffer = Buffer.from("a");
          expect(() => {
            buffer.unshift({
              srcBuffer,
            });
          }).to.throw;
        });
      });
    });
  });
});
