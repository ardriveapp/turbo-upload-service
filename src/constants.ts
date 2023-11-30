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
import { PublicArweaveAddress } from "./types/types";

export const port = process.env.PORT ? +process.env.PORT : 3000;

export const receiptVersion = "0.1.0";

export const deadlineHeightIncrement = 200;

// Wallets added via environment var as a comma separated list // cspell:disable
// e.g: ALLOW_LISTED_ADDRESSES="QWERTYUIOP,ASDFGHJKL,ZXCVBNM" // cspell:enable
const injectedAllowListAddresses = process.env.ALLOW_LISTED_ADDRESSES
  ? process.env.ALLOW_LISTED_ADDRESSES.split(",")
  : [];

export const allowListPublicAddresses: PublicArweaveAddress[] =
  injectedAllowListAddresses;

export const migrateOnStartup = process.env.MIGRATE_ON_STARTUP === "true";

const oneGiB = 1_073_741_824;
const twoGiB = oneGiB * 2;
const fourGiB = oneGiB * 4;
const oneKiB = 1024;

export const maxDataItemLimit = 1_000;

/** Target max size for bundle packing. If data item is larger than this, it will bundle by itself */
export const maxBundleSize = process.env.MAX_BUNDLE_SIZE
  ? +process.env.MAX_BUNDLE_SIZE
  : twoGiB;

/** Max allowed data item limit on data post ingest */
export const maxDataItemSize = process.env.MAX_DATA_ITEM_SIZE
  ? +process.env.MAX_DATA_ITEM_SIZE
  : fourGiB;

export const freeArfsDataAllowLimit = +(
  process.env.FREE_UPLOAD_LIMIT ?? oneKiB * 505
); // Extra to account for the header sizes

export const allowArFSData = process.env.ALLOW_ARFS_DATA === "true";
export const gatewayUrl = new URL(
  process.env.ARWEAVE_GATEWAY || "https://arweave.net:443"
);

export const dataCaches = process.env.DATA_CACHES?.split(",") ?? [
  "arweave.net",
];
export const fastFinalityIndexes = process.env.FAST_FINALITY_INDEXES?.split(
  ","
) ?? ["arweave.net"];

/**
 * Error delay for the first failed request for a transaction header post or chunk upload
 * Subsequent requests will delay longer with an exponential back off strategy
 */
export const INITIAL_ERROR_DELAY = 500; // 500ms

/**
 *  These are errors from the `/chunk` endpoint on an Arweave
 *  node that we should never try to continue on
 */
export const FATAL_CHUNK_UPLOAD_ERRORS = [
  "invalid_json",
  "chunk_too_big",
  "data_path_too_big",
  "offset_too_big",
  "data_size_too_big",
  "chunk_proof_ratio_not_attractive",
  "invalid_proof",
];

export const txPermanentThreshold = 50;
export const txWellSeededThreshold = 30;
export const txConfirmationThreshold = 1;

export const dropTxThresholdNumberOfBlocks = 50;
export const defaultMaxConcurrentChunks = 32;

export const testPrivateRouteSecret = "test-secret";

export const octetStreamContentType = "application/octet-stream";

export const failedReasons = {
  failedToPost: "failed_to_post",
  notFound: "not_found",
} as const;

export const msPerMinute = 60_000;

export const signatureTypeLength = 2;
export const emptyTargetLength = 1;
export const targetLength = 33;
export const emptyAnchorLength = 1;
export const anchorLength = 33;
