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
import {
  byteArrayToLong,
  deepHash,
  deserializeTags,
  indexToType,
} from "arbundles";
import { stringToBuffer } from "arweave/node/lib/utils";
import { PathLike, createReadStream, read as fsRead, promises } from "fs";
import { EventEmitter, PassThrough, Readable } from "stream";
import { promisify } from "util";
import winston from "winston";

import logger from "../logger";
import { CircularBuffer } from "../utils/circularBuffer";
import { tapStream } from "../utils/common";
import FileDataItem, { dataItemTagsByteLimit, readFileMode } from "./dataItem";

const read = promisify(fsRead);

interface SigInfo {
  signatureLength: number;
  pubkeyLength: number;
  name: string;
}
const fiveMiB = 1024 * 1024 * 5;

const arweaveSigInfo = {
  signatureLength: 512,
  pubkeyLength: 512,
  name: "arweave",
};

export const signatureTypeInfo: Record<number, SigInfo> = {
  [1]: arweaveSigInfo,
  [2]: {
    signatureLength: 64,
    pubkeyLength: 32,
    name: "ed25519",
  },
  [3]: {
    signatureLength: 65,
    pubkeyLength: 65,
    name: "ethereum",
  },
  [4]: {
    signatureLength: 64,
    pubkeyLength: 32,
    name: "solana",
  },
  [5]: {
    signatureLength: 64,
    pubkeyLength: 32,
    name: "injectedAptos",
  },
  [6]: {
    signatureLength: 2052, // max 32 64 byte signatures, +4 for 32-bit bitmap
    pubkeyLength: 1025, // max 32 32 byte keys, +1 for 8-bit threshold value
    name: "multiAptos",
  },
};

/**
 * Creates an EventEmitter that emits DataItem fields as they pass through a readable stream.
 * For a health data item, the events are emitted in the following order with their respective
 * callback argument types, optionality, and emission frequency:
 * - "signatureType" (number, required, once)
 * - "signature" (Buffer, required, once)
 * - "owner" (Buffer, required, once)
 * - "targetFlag" (number, required, once)
 * - "targetBytes" (Buffer, optional, once)
 * - "anchorFlag" (number, required, once)
 * - "anchorBytes" (Buffer, optional, once)
 * - "numTags" (number, required, once)
 * - "numTagsBytes" (number, required, once)
 * - "tagsBytes" (Buffer, optional, once)
 * - "data" (Buffer, required, one or many times)
 * - "payloadStream" (Readable [NOTE: must call .resume() to get stream flowing], required, once)
 * - "payloadSize" (number, required, once)
 * - "isValid" (boolean, required, once)
 */
export function createVerifiedDataItemStream(
  inputStream: Readable,
  logger?: winston.Logger,
  payloadStreamHighWaterMark = fiveMiB
): EventEmitter {
  const emitter = new EventEmitter();
  const searchBuffer = new CircularBuffer(dataItemTagsByteLimit); // This is the most we'll ever read into the buffer
  let currentOffset = 0;

  let emittedData = false;

  let parsedNumTagsBytes: number | undefined;
  let talliedPayloadSize = 0;
  let byteQueue: CircularBuffer;
  let haveTarget = false;
  let haveAnchor = false;
  let nextEventToParse: { name: string; length: () => number } | undefined;
  let lastParsingError: Error | undefined;

  // Variables needing parsing for signature check
  let signatureType: number | undefined;
  let signatureBytes: Buffer | undefined;
  let ownerBytes: Buffer | undefined;
  let targetBytes: Buffer | undefined;
  let anchorBytes: Buffer | undefined;
  let tagsBytes: Buffer | undefined;
  let payloadStream: PassThrough | undefined;

  const offsetQueue: { name: string; length: () => number }[] = [
    { name: "signatureType", length: () => 2 },
    {
      name: "signature",
      length: () => {
        if (!signatureType) {
          lastParsingError = new Error("signatureType never parsed!");
          emitter.emit("error", lastParsingError);
          return arweaveSigInfo.signatureLength;
        }
        const sigCfg = signatureTypeInfo[signatureType];
        if (!sigCfg) {
          lastParsingError = new Error(
            `Signature with value ${signatureType} not supported!`
          );
          emitter.emit("error", lastParsingError);
          return arweaveSigInfo.signatureLength;
        }
        return sigCfg.signatureLength;
      },
    },
    {
      name: "owner",
      length: () => {
        if (!signatureType) {
          lastParsingError = new Error("signatureType never parsed!");
          emitter.emit("error", lastParsingError);
          return arweaveSigInfo.pubkeyLength;
        }
        const sigCfg = signatureTypeInfo[signatureType];
        if (!sigCfg) {
          lastParsingError = new Error(
            `Signature with value ${signatureType} not supported!`
          );
          emitter.emit("error", lastParsingError);
          return arweaveSigInfo.pubkeyLength;
        }
        return sigCfg.pubkeyLength;
      },
    },
    { name: "targetFlag", length: () => 1 },
    { name: "target", length: () => (haveTarget ? 32 : 0) },
    { name: "anchorFlag", length: () => 1 },
    { name: "anchor", length: () => (haveAnchor ? 32 : 0) },
    { name: "numTags", length: () => 8 },
    { name: "numTagsBytes", length: () => 8 },
    {
      name: "tagsBytes",
      length: () => {
        if (parsedNumTagsBytes === undefined) {
          lastParsingError = new Error("numTagBytes never parsed!");
          emitter.emit("error", lastParsingError);
          return 0;
        }
        return parsedNumTagsBytes;
      },
    },
    { name: "data", length: () => Number.POSITIVE_INFINITY }, // Just emit data chunks until we're done
  ];

  const parseDataItemStream = (chunk: Buffer) => {
    if (lastParsingError) {
      logger?.warn(`Skipping chunk due to parsing error.`, lastParsingError);
      return;
    }

    // In the event that the search buffer can't hold the whole chunk, we'll read into it incrementally.
    let chunkOffset = 0;
    do {
      // Keep parsing the same event or else parse a potential next event
      nextEventToParse ??= offsetQueue.shift();
      if (nextEventToParse?.length() === 0) {
        logger?.debug(`Skipping 0-length event ${offsetQueue[0].name}`);
      }
    } while (
      nextEventToParse?.length() === 0 // skip any events with 0 expected bytes incoming
    );

    if (nextEventToParse) {
      logger?.debug(
        `Parsing ${nextEventToParse.name}. Progress: ${
          searchBuffer.usedCapacity
        } of ${nextEventToParse.length()} expected bytes`
      );

      // Since data is at the end of the data item and unbounded in size, we just emit chunks immediately
      if (nextEventToParse.name === "data") {
        logger?.debug(
          `Emitting ${chunk.byteLength} bytes of data item payload data.`
        );
        emitter.emit(nextEventToParse.name, chunk);
        currentOffset += chunk.byteLength;
        return;
      }

      // BEST CASE - we're not searching for bytes and can event straight from chunk data
      // NEXT BEST - we're searching for bytes and there's enough in the chunk to do one or more events
      // NEXT BEST - we're searching for bytes and there's NOT enough in the chunk to do one or more events
      // NEXT BEST - we're NOT searching for bytes and there's NOT enough in the chunk to do one or more events
      const useChunkAsBuffer =
        searchBuffer.usedCapacity === 0 &&
        chunk.byteLength > nextEventToParse.length();

      if (useChunkAsBuffer) {
        logger?.debug(`Using incoming chunk as event emission buffer`);
      } else {
        const numBytesToAppend = Math.min(
          chunk.byteLength - chunkOffset,
          searchBuffer.remainingCapacity
        );
        logger?.debug(
          `Adding ${numBytesToAppend} bytes of incoming ${chunk.byteLength} byte chunk to the search buffer with remaining capacity ${searchBuffer.remainingCapacity} bytes...`
        );
        // Append the incoming chunk to the search buffer
        searchBuffer.writeFrom({
          srcBuffer: chunk,
          numBytes: numBytesToAppend,
          srcOffset: chunkOffset,
        });
        chunkOffset += numBytesToAppend;
      }

      byteQueue = useChunkAsBuffer
        ? new CircularBuffer(chunk.byteLength, {
            buffer: chunkOffset ? chunk.slice(chunkOffset) : chunk,
            usedCapacity: chunk.byteLength,
          })
        : searchBuffer;

      // EMIT AS MANY EVENTS AS POSSIBLE FROM THE CHUNK, THEN CACHE THE REST IN SEARCH BUFFER
      while (
        byteQueue.usedCapacity >= nextEventToParse.length() &&
        !lastParsingError
      ) {
        logger?.debug(
          `Emitting event '${nextEventToParse.name}' at offset ${currentOffset}`
        );
        const eventBuffer = byteQueue.shift(nextEventToParse.length());
        emitter.emit(nextEventToParse.name, eventBuffer);
        currentOffset += nextEventToParse.length();

        // Skip any 0 size events
        while (offsetQueue[0] && offsetQueue[0].length() === 0) {
          logger?.debug(`Skipping 0-length event ${offsetQueue[0].name}`);
          offsetQueue.shift();
        }

        // Emit the next event in the queue if possible
        nextEventToParse = offsetQueue.shift();
        if (!nextEventToParse) {
          logger?.error(
            `UNEXPECTED! SHOULD HAVE AT LEAST BEEN A DATA EVENT TO PARSE!`
          );
          break;
        }

        if (lastParsingError) {
          logger?.warn(
            `Skipping event emissions due to parsing error.`,
            lastParsingError
          );
          return;
        }

        if (nextEventToParse.length() <= byteQueue.usedCapacity) {
          logger?.debug(`Have enough bytes to emit the next event`);
          // Event gets emitted in the next loop iteration
        } else {
          // If we're not on the "data" event, preserve remaining parsed bytes for next input stream on("data") event
          if (nextEventToParse.name !== "data") {
            logger?.debug(
              `Remaining ${
                byteQueue.usedCapacity
              } bytes not enough for next event ${
                nextEventToParse.name
              } with size ${nextEventToParse.length()}`
            );

            if (useChunkAsBuffer) {
              logger?.debug(
                `Stashing remaining ${byteQueue.usedCapacity} bytes of incoming buffer in search buffer`
              );
              // Stash the rest of whatever's left in the buffer into the searchBuffer for next iteration
              searchBuffer.writeFrom({
                srcBuffer: byteQueue.shift(byteQueue.usedCapacity),
              });
            } else if (chunkOffset > 0) {
              logger?.debug(
                `ATTEMPTING TO SAVE THE REMAINING ${
                  chunk.byteLength - chunkOffset
                } CHUNK BYTES INTO SEARCH BUFFER WITH REMAINING CAPACITY ${
                  searchBuffer.remainingCapacity
                } BYTES...`
              );
              searchBuffer.writeFrom({
                srcBuffer: chunk,
                srcOffset: chunkOffset,
              });
            } else {
              // Nothing to do since remaining bytes are already in the searchBuffer
            }
          } else {
            // Just emit data now since that's the only event we have left to process
            if (byteQueue.usedCapacity) {
              currentOffset += byteQueue.usedCapacity;
              logger?.debug(
                `Emitting ${byteQueue.usedCapacity} remaining searchBuffer bytes of data item payload data.`
              );
              emitter.emit("data", byteQueue.shift(byteQueue.usedCapacity));
            }

            // Also emit any remaining data in the chunk
            if (chunkOffset > 0) {
              const remainingChunkBytes = chunk.byteLength - chunkOffset;
              logger?.debug(
                `Emitting ${remainingChunkBytes} remaining chunk bytes of data item payload data.`
              );
              emitter.emit(nextEventToParse.name, chunk.slice(chunkOffset));
              currentOffset += remainingChunkBytes;
            }
          }
          break;
        }
      }
      logger?.debug(`No longer have enough bytes to emit the next event.`);
    }

    currentOffset += chunk.byteLength;
  };

  let timeoutId: NodeJS.Timeout;
  inputStream.on("data", (chunk) => {
    clearTimeout(timeoutId);
    timeoutId = setTimeout(() => {
      logger?.error("Data item chunk not received within 3 seconds.");
    }, 3000);
    parseDataItemStream(chunk);
  });

  inputStream.once("close", () => {
    clearTimeout(timeoutId);
    logger?.debug("Input stream closed");
    payloadStream?.end();
  });

  inputStream.once("end", () => {
    clearTimeout(timeoutId);
    logger?.debug("Input stream ended");

    if (tagsBytes !== undefined && !emittedData) {
      logger?.info(
        "Zero length data item payload found! Emitting empty data buffer..."
      );
      emitter.emit("data", Buffer.alloc(0));
    }

    payloadStream?.end();
  });

  inputStream.on("pause", () => {
    clearTimeout(timeoutId);
  });

  inputStream.once("error", (error) => {
    clearTimeout(timeoutId);

    // Propagate error to the payload stream if possible
    payloadStream?.emit(
      "error",
      new Error(
        `Input stream encountered error: ${error.message ?? "Unknown error"}`
      )
    );
    logger?.error("Ending payload stream due to error...", error);
    payloadStream?.end();

    emitter.emit("error", error);
  });

  let errorsCaught = 0;
  inputStream.on("error", (error) => {
    errorsCaught++;
    logger?.error(`Input stream encountered error # ${errorsCaught}`, error);
  });

  // Stop consuming data from the input stream once we encounter an error
  emitter.once("error", () => {
    inputStream.removeListener("data", parseDataItemStream);
  });

  emitter.once("signatureType", (bufferedSignatureType: Buffer) => {
    signatureType = byteArrayToLong(bufferedSignatureType);
  });

  emitter.once("signature", (bufferedSignature: Buffer) => {
    signatureBytes = Buffer.from(bufferedSignature);
  });

  emitter.once("owner", (bufferedOwner: Buffer) => {
    ownerBytes = Buffer.from(bufferedOwner);
  });

  emitter.once("target", (bufferedTarget: Buffer) => {
    targetBytes = Buffer.from(bufferedTarget);
  });

  emitter.once("anchor", (bufferedAnchor: Buffer) => {
    anchorBytes = Buffer.from(bufferedAnchor);
  });

  emitter.once("tagsBytes", (bufferedTags: Buffer) => {
    tagsBytes = Buffer.from(bufferedTags);
  });

  emitter.on("data", async (bufferedData: Buffer) => {
    emittedData = true;

    if (!payloadStream) {
      logger?.debug("CREATING PAYLOAD STREAM");
      payloadStream = new PassThrough({
        highWaterMark: payloadStreamHighWaterMark,
      });

      payloadStream.on("error", (err) => {
        logger?.error("Payload stream has received an error...", err);
        payloadStream?.end();
      });
      emitter.emit("payloadStream", payloadStream);
    }

    if (!payloadStream.write(bufferedData)) {
      inputStream.pause();
      payloadStream?.once("drain", () => {
        inputStream.resume();
      });
    }
  });

  emitter.once("targetFlag", (bufferedTargetFlag: Buffer) => {
    const targetFlag = bufferedTargetFlag[0];
    haveTarget = targetFlag === 1;
  });

  emitter.once("anchorFlag", (bufferedAnchorFlag: Buffer) => {
    const anchorFlag = bufferedAnchorFlag[0];
    haveAnchor = anchorFlag === 1;
  });

  emitter.once("numTagsBytes", (bufferedNumTagBytes: Buffer) => {
    const numTagsBytes = byteArrayToLong(bufferedNumTagBytes);
    if (numTagsBytes > dataItemTagsByteLimit) {
      lastParsingError = new Error(
        `Data item total tags size must not exceed ${dataItemTagsByteLimit} bytes! Parser expected ${numTagsBytes} bytes!`
      );
      emitter.emit("error", lastParsingError);
    }
    logger?.debug(
      `Got numTagsBytes emission. Tags should be ${numTagsBytes} bytes`
    );
    parsedNumTagsBytes = numTagsBytes;
    if (parsedNumTagsBytes === 0) {
      emitter.emit("tagsBytes", Buffer.alloc(0));
    }
  });

  emitter.once("payloadStream", async (emittedPayloadStream: Readable) => {
    // We should now be able to start verifying the data item
    if (
      !(
        signatureType &&
        signatureBytes &&
        ownerBytes &&
        (tagsBytes || parsedNumTagsBytes === 0)
      )
    ) {
      logger?.error(
        "Some data necessary for validation was NOT successfully parsed!",
        {
          signatureType,
          signatureBytesExists: signatureBytes !== undefined,
          ownerBytesExists: ownerBytes !== undefined,
          tagsBytesExists: tagsBytes !== undefined,
          parsedNumTagsBytes,
        }
      );
      emitter.emit("isValid", false);
      return;
    }

    // HACK: Pause payloadStream while this listener is attached to prevent
    // data from starting to flow past any other would-be payloadStream listeners.
    logger?.debug("Pausing emitted payload stream");
    emittedPayloadStream.pause();
    const deepHashStream = tapStream({
      readable: emittedPayloadStream,
      logger: logger?.child({ context: "deepHashStream" }),
    });
    deepHashStream.on("data", (chunk: Buffer) => {
      talliedPayloadSize += chunk.length;
    });
    deepHashStream.on("end", () => {
      if (!lastParsingError) {
        emitter.emit("payloadSize", talliedPayloadSize);
      }
    });

    logger?.debug("Starting deep hashing...");
    try {
      const signatureData = await deepHash([
        stringToBuffer("dataitem"),
        stringToBuffer("1"),
        stringToBuffer(`${signatureType}`),
        ownerBytes,
        targetBytes ?? Buffer.alloc(0),
        anchorBytes ?? Buffer.alloc(0),
        tagsBytes ?? Buffer.alloc(0),
        deepHashStream,
      ]);

      const signer = indexToType[signatureType];
      const isValid = await signer.verify(
        ownerBytes,
        signatureData,
        signatureBytes
      );
      logger?.debug(`IS VALID EMITTING ${isValid}`);
      emitter.emit("isValid", isValid);
    } catch (error) {
      logger?.debug("Failed to deepHash stream", error);
      emitter.emit("isValid", false);
    }
  });

  return emitter;
}

export async function verifyArweaveDataItemFromPath(
  filename: PathLike
): Promise<boolean> {
  return verifyDataItem(new FileDataItem(filename));
}

export async function verifyDataItem(item: FileDataItem): Promise<boolean> {
  const filename = item.filename;
  const handle = await promises.open(filename, readFileMode);
  const sigType = await item.signatureType();
  const tagsStart = await item.getTagsStart();

  const numberOfTags = await read(
    handle.fd,
    Buffer.allocUnsafe(8),
    0,
    8,
    tagsStart
  ).then((r) => byteArrayToLong(r.buffer));
  const numberOfTagsBytes = await read(
    handle.fd,
    Buffer.allocUnsafe(8),
    0,
    8,
    tagsStart + 8
  ).then((r) => byteArrayToLong(r.buffer));
  if (numberOfTagsBytes > dataItemTagsByteLimit) {
    await handle.close();
    logger.error("Data Item Tags are Too Large");
    return false;
  }

  const tagsBytes = await read(
    handle.fd,
    Buffer.allocUnsafe(numberOfTagsBytes),
    0,
    numberOfTagsBytes,
    tagsStart + 16
  ).then((r) => r.buffer);
  if (numberOfTags > 0) {
    try {
      deserializeTags(tagsBytes);
    } catch (e) {
      await handle.close();
      logger.error("Data Item Tags Could Not Be Parsed");
      return false;
    }
  }
  const owner = await item.rawOwner();

  const signatureData = await deepHash([
    stringToBuffer("dataitem"),
    stringToBuffer("1"),
    stringToBuffer(sigType.toString()),
    owner,
    await item.rawTarget(),
    await item.rawAnchor(),
    await item.rawTags(),
    createReadStream(filename, {
      start: await item.dataStart(),
    }),
  ]);
  const sig = await item.rawSignature();

  const signer = indexToType[sigType];
  if (!(await signer.verify(owner, signatureData, sig))) {
    logger.error("Data Item Signature Could Not be Verified");
    await handle.close();
    return false;
  }

  await handle.close();

  return true;
}
