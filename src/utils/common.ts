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
import { Tag } from "@dha-team/arbundles";
import { PathLike, existsSync, rmSync } from "fs";
import { PassThrough, Readable } from "stream";
import winston from "winston";

import {
  aoDedicatedBundlesPremiumFeatureType,
  arDriveDedicatedBundlesPremiumFeatureType,
  arioDedicatedBundlesPremiumFeatureType,
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
import {
  ByteCount,
  ParsedDataItemHeader,
  PayloadInfo,
  SignatureConfig,
  TransactionId,
} from "../types/types";

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
  }: { errorMessage?: string; status?: number; error?: unknown }
) {
  const logger = ctx.state.logger ?? defaultLogger;
  logger.debug("Pausing request stream to return error response");
  ctx.req.pause(); // pause the stream to prevent unnecessary consumption of additional bytes
  ctx.status = status;
  errorMessage =
    errorMessage ?? (error instanceof Error ? error.message : "Unknown error");
  ctx.res.statusMessage = errorMessage;

  /**
   * Emit an error to the input stream to ensure that it and tapped streams prepare for shutdown
   * NOTE: an input stream may initially cause the error - so this would re-emit, if uncaught exceptions are thrown we may want to validate that this error was not already emitted by the stream
   */
  ctx.req.emit("error", error ?? new Error(`${errorMessage}`));
  logger.debug(errorMessage ?? error);
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
    logger?.debug("Tapped stream encountered error!", error);
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
    (
      tags.filter((tag) => tag.name.toLowerCase() === "content-type").shift()
        ?.value || octetStreamContentType
    )
      // Truncate to 255 characters to avoid DB errors
      .slice(0, 255)
  );
}

export function getPremiumFeatureType(
  ownerPublicAddress: string,
  tags: Tag[],
  signatureType: SignatureConfig,
  nestedDataItemHeaders: ParsedDataItemHeader[],
  targetPublicAddress?: string | undefined
): string {
  if (signatureType === SignatureConfig.KYVE) {
    return kyveDedicatedBundlesPremiumFeatureType;
  }

  for (const premiumFeatureType of premiumPaidFeatureTypes) {
    const { allowedWallets } = dedicatedBundleTypes[premiumFeatureType];
    if (allowedWallets.includes(ownerPublicAddress)) {
      if (premiumFeatureType === aoDedicatedBundlesPremiumFeatureType) {
        const arioProcesses =
          dedicatedBundleTypes[arioDedicatedBundlesPremiumFeatureType]
            .allowedProcesses ?? [];
        if (
          targetPublicAddress !== undefined &&
          arioProcesses.includes(targetPublicAddress)
        ) {
          // If the sender is AO and the target is an AR.IO Network process, we pack into AR.IO dedicated bundles
          return arioDedicatedBundlesPremiumFeatureType;
        }

        const hasArioProcessTag = (tags: Tag[]) =>
          tags
            .filter((t) => t.name === "Process" || t.name === "From-Process")
            .some((t) => arioProcesses.includes(t.value));

        if (hasArioProcessTag(tags)) {
          // If the sender is AO and the tags include an AR.IO Network process, we pack into AR.IO dedicated bundles
          return arioDedicatedBundlesPremiumFeatureType;
        }

        // When the sender is AO and the upload has nested data item headers, check for AR.IO Tags
        if (nestedDataItemHeaders.length > 0) {
          const containsNestedArioTagOrTarget = nestedDataItemHeaders.some(
            (header) =>
              hasArioProcessTag(header.tags ?? []) ||
              (header.target !== undefined &&
                arioProcesses.includes(header.target))
          );
          defaultLogger.debug("Unpacked nested headers for AO BDI", {
            hasArioProcessTagInNested: containsNestedArioTagOrTarget,
            nestedDataItemHeaders,
          });
          if (containsNestedArioTagOrTarget) {
            // TODO: move this to debug log
            defaultLogger.info(
              "AO BDI upload with AR.IO process tag detected, using AR.IO dedicated bundles",
              {
                nestedDataItemHeaders,
                ownerPublicAddress,
                targetPublicAddress,
                tags,
                signatureType,
              }
            );
            return arioDedicatedBundlesPremiumFeatureType;
          }
        }
      }

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

  const hasAppNameArDriveTag = tags
    .filter((t) => t.name === "App-Name")
    .some((t) => t.value.startsWith("ArDrive"));
  if (hasAppNameArDriveTag) {
    return arDriveDedicatedBundlesPremiumFeatureType;
  }

  if (
    targetPublicAddress !== undefined &&
    dedicatedBundleTypes[
      arioDedicatedBundlesPremiumFeatureType
    ].allowedWallets.includes(targetPublicAddress) &&
    tags.some((t) => t.name === "Action" && t.value === "Eval")
  ) {
    // If the target is an AR.IO Network process and the action is Eval, we pack into AR.IO dedicated bundles
    return arioDedicatedBundlesPremiumFeatureType;
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

export function deserializePayloadInfo(metadataString: string): PayloadInfo {
  // Don't use split() because the payloadContentType might contain semicolons
  const lastSemicolonIndex = metadataString.lastIndexOf(";");
  if (lastSemicolonIndex === -1) {
    throw new Error(
      `Invalid metadata string: ${metadataString} (no semicolon found)`
    );
  }

  const payloadContentType = metadataString.substring(0, lastSemicolonIndex);
  const payloadDataStartStr = metadataString.substring(lastSemicolonIndex + 1);
  return {
    payloadContentType,
    payloadDataStart: parseInt(payloadDataStartStr),
  };
}

export function shouldSampleIn(samplingRate: number): boolean {
  if (samplingRate >= 1) {
    return true;
  }
  if (samplingRate > 0) {
    return Math.random() <= samplingRate;
  }
  return false;
}

export function minifyNestedDataItemInfo({
  parentDataItemId,
  parentPayloadDataStart,
  startOffsetInRawParent,
  rawContentLength,
  payloadContentType,
  payloadDataStart,
}: {
  parentDataItemId: TransactionId;
  parentPayloadDataStart: number;
  startOffsetInRawParent: number;
  rawContentLength: number;
  payloadContentType: string;
  payloadDataStart: number;
}) {
  return {
    pid: parentDataItemId,
    ppds: parentPayloadDataStart,
    sorp: startOffsetInRawParent,
    rcl: rawContentLength,
    pct: payloadContentType,
    pds: payloadDataStart,
  };
}

/**
 * Extracts the error code from an error object if it exists and is a string,
 * otherwise returns "unknown".
 *
 * @param error - The error object to extract the code from
 * @returns The error code as a string, or "unknown" if not found or not a string
 */
export function getErrorCodeFromErrorObject(error: unknown): string {
  return typeof error === "object" &&
    error &&
    "code" in error &&
    typeof error.code === "string"
    ? error.code
    : "unknown";
}
