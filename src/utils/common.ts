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
import { Tag } from "arbundles";
import { PathLike, existsSync, rmSync } from "fs";
import { PassThrough, Readable } from "stream";
import winston from "winston";

import { SignatureConfig } from "../bundles/verifyDataItem";
import {
  dedicatedBundleTypes,
  defaultPremiumFeatureType,
  kyveDedicatedBundlesPremiumFeatureType,
  octetStreamContentType,
  premiumPaidFeatureTypes,
  rePostDataItemThresholdNumberOfBlocks,
  warpDedicatedBundlesPremiumFeatureType,
} from "../constants";
import defaultLogger from "../logger";
import { KoaContext } from "../server";
import { JWKInterface } from "../types/jwkTypes";
import { ByteCount } from "../types/types";

export function isTestEnv(): boolean {
  return process.env.NODE_ENV === "test";
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function filterKeysFromObject<T = any>(
  object: Record<string, T>,
  excludedKeys: string[]
): Record<string, T> {
  const entries = Object.entries(object);
  const filteredEntries = entries.filter(
    ([key]) => !excludedKeys.includes(key)
  );
  return Object.fromEntries(filteredEntries);
}

export function errorResponse(
  ctx: KoaContext,
  {
    status = 400,
    errorMessage,
    error,
  }: { errorMessage: string; status?: number; error?: unknown }
) {
  const logger = ctx.state.logger ?? defaultLogger;
  logger.debug("Pausing request stream to return error response");
  ctx.req.pause(); // pause the stream to prevent unnecessary consumption of additional bytes
  ctx.status = status;
  ctx.res.statusMessage = errorMessage;
  /**
   * Emit an error to the input stream to ensure that it and tapped streams prepare for shutdown
   * NOTE: an input stream may initially cause the error - so this would re-emit, if uncaught exceptions are thrown we may want to validate that this error was not already emitted by the stream
   */
  ctx.req.emit(
    "error",
    error ?? new Error(`${errorMessage ?? "Unknown error"}`)
  );
  logger.error(errorMessage, error);
}

export function cleanUpTempFile(path: PathLike) {
  if (existsSync(path)) {
    rmSync(path);
  }
}

const fiveMiB = 1024 * 1024 * 5;
export function tapStream({
  readable,
  writableHighWaterMark = fiveMiB,
  context = "tappedStream",
  logger = defaultLogger.child({ context }),
  onError = (error: Error) => {
    logger?.error("Tapped stream encountered error!", error);
  },
}: {
  readable: Readable;
  writableHighWaterMark?: number;
  logger?: winston.Logger;
  context?: string;
  onError?: (error: Error) => void;
  // TODO: add destroyOnError handler that
}): PassThrough {
  const passThrough = new PassThrough({ writableHighWaterMark });
  passThrough.setMaxListeners(Infinity); // Suppress leak warnings related to backpressure and drain listeners
  readable.on("data", (chunk: Buffer) => {
    if (!passThrough.write(chunk)) {
      logger?.debug(
        "PassThrough stream overflowing. Pausing readable stream..."
      );
      readable.pause(); // stops the input stream from pushing data to the passthrough while it's trying to catch up by processing its enqueued bytes
      passThrough.once("drain", () => {
        logger?.debug(
          "PassThrough stream drained. Resuming readable stream..."
        );
        readable.resume();
      });
    }
  });
  readable.once("end", () => {
    logger?.debug("Readable stream ended. Closing pass through stream...");
    passThrough.end();
  });
  readable.once("error", (error) => {
    // intentionally cleanup any pass through streams relying on the original input stream
    passThrough.destroy(error);
  });
  if (onError) {
    passThrough.on("error", onError);
  }
  return passThrough;
}

export function getPublicKeyFromJwk(jwk: JWKInterface): string {
  return jwk.n;
}

export function payloadContentTypeFromDecodedTags(tags: Tag[]): string {
  return (
    tags.filter((tag) => tag.name.toLowerCase() === "content-type").shift()
      ?.value || octetStreamContentType
  );
}

export function getPremiumFeatureType(
  ownerPublicAddress: string,
  tags: Tag[],
  signatureType: SignatureConfig
): string {
  if (signatureType === SignatureConfig.KYVE) {
    return kyveDedicatedBundlesPremiumFeatureType;
  }

  for (const premiumFeatureType of premiumPaidFeatureTypes) {
    const { allowedWallets } = dedicatedBundleTypes[premiumFeatureType];
    if (allowedWallets.includes(ownerPublicAddress)) {
      if (premiumFeatureType !== warpDedicatedBundlesPremiumFeatureType) {
        return premiumFeatureType;
      }

      // TODO: Include optional `mustHaveOneOfTags` (or similar) on `dedicatedBundleTypes` to
      // allow for more tag checks from other clients.  For now, only Warp needs this check
      const hasSmartWeaveActionTag = tags
        .filter((t) => t.name === "App-Name")
        .some((t) => t.value === "SmartWeaveAction");
      const hasWarpSequencerTag = tags
        .filter((t) => t.name === "Sequencer")
        .some((t) => t.value === "Warp");

      if (hasSmartWeaveActionTag || hasWarpSequencerTag) {
        return warpDedicatedBundlesPremiumFeatureType;
      }
    }
  }

  return defaultPremiumFeatureType;
}

export type NonZeroPositiveInteger = number;
export function isNonZeroPositiveInteger(
  num: number
): num is NonZeroPositiveInteger {
  return Number.isInteger(num) && num > 0;
}
export function* generateArrayChunks<T>(
  arr: T[],
  chunkSize: NonZeroPositiveInteger
): Generator<T[], void> {
  if (!isNonZeroPositiveInteger(chunkSize)) {
    throw new Error("chunkSize must be a non-negative integer");
  }
  for (let i = 0; i < arr.length; i += chunkSize) {
    yield arr.slice(i, i + chunkSize);
  }
}

export function getByteCountBasedRePackThresholdBlockCount(
  payloadSize: ByteCount
): number {
  const oneMiB = 1024 * 1024;
  const oneGiB = 1024 * oneMiB;

  const fiveHundredMiB = 500 * oneMiB;
  if (payloadSize <= fiveHundredMiB) {
    return rePostDataItemThresholdNumberOfBlocks;
  }

  const twoGiB = 2 * oneGiB;
  if (payloadSize <= twoGiB) {
    return rePostDataItemThresholdNumberOfBlocks * 1.5;
  }

  const fiveGiB = 5 * oneGiB;
  if (payloadSize <= fiveGiB) {
    return rePostDataItemThresholdNumberOfBlocks * 2;
  }

  const tenGiB = 10 * oneGiB;
  if (payloadSize <= tenGiB) {
    return rePostDataItemThresholdNumberOfBlocks * 3;
  }

  const twentyGiB = 20 * oneGiB;
  if (payloadSize <= twentyGiB) {
    return rePostDataItemThresholdNumberOfBlocks * 4;
  }

  return rePostDataItemThresholdNumberOfBlocks * 5;
}

export function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
