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
import * as fs from "fs";

import { PublicArweaveAddress } from "./types/types";

export const port = process.env.PORT ? +process.env.PORT : 3000;

export const receiptVersion = "0.2.0";

export const deadlineHeightIncrement = 200;

// Wallets added via environment var as a comma separated list // cspell:disable
// e.g: ALLOW_LISTED_ADDRESSES="QWERTYUIOP,ASDFGHJKL,ZXCVBNM" // cspell:enable
const injectedAllowListAddresses = process.env.ALLOW_LISTED_ADDRESSES
  ? process.env.ALLOW_LISTED_ADDRESSES.split(",")
  : [];

export const allowListPublicAddresses: PublicArweaveAddress[] =
  injectedAllowListAddresses;

export const migrateOnStartup = process.env.MIGRATE_ON_STARTUP === "true";

export const otelSampleRate = process.env.OTEL_SAMPLE_RATE
  ? +process.env.OTEL_SAMPLE_RATE
  : 200;

const oneGiB = 1_073_741_824;
const twoGiB = oneGiB * 2;
const fourGiB = oneGiB * 4;
const oneKiB = 1024;

export const maxDataItemsPerBundle = process.env.MAX_DATA_ITEM_LIMIT
  ? +process.env.MAX_DATA_ITEM_LIMIT
  : 10_000;

/** Target max size for bundle packing. If data item is larger than this, it will bundle by itself */
export const maxBundleDataItemsByteCount = process.env.MAX_BUNDLE_SIZE
  ? +process.env.MAX_BUNDLE_SIZE
  : twoGiB;

/** Max allowed data item limit on data post ingest */
export const maxSingleDataItemByteCount = process.env.MAX_DATA_ITEM_SIZE
  ? +process.env.MAX_DATA_ITEM_SIZE
  : fourGiB;

export const tickArnsContractEnabled =
  process.env.TICK_ARNS_REGISTRY_ENABLED === "true";
export const arnsContractTxId = process.env.ARNS_CONTRACT_TX_ID;

export const freeUploadLimitBytes = +(
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

export const txPermanentThreshold = 18;
export const txWellSeededThreshold = 30;
export const txConfirmationThreshold = 1;

export const dropBundleTxThresholdNumberOfBlocks = 50;
export const rePostDataItemThresholdNumberOfBlocks = 125;
export const retryLimitForFailedDataItems = 10;

const txIdLength = 43;
export const failedBundleCSVColumnLength = (txIdLength + 1) * 20; // Allow up to 20 failed bundles in the schema

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

export const defaultPremiumFeatureType = "default";

export const warpWalletAddresses = process.env.WARP_ADDRESSES?.split(",") ?? [
  // cspell:disable
  "jnioZFibZSCcV8o-HkBXYPYEYNib4tqfexP0kCBXX_M",
];
export const redstoneOracleAddresses =
  process.env.REDSTONE_ORACLE_ADDRESSES?.split(",") ?? [
    "I-5rWUehEv-MjdK9gFw09RxfSLQX9DIHxG614Wf8qo0", // cspell:enable
  ];

export const firstBatchAddresses = process.env.FIRST_BATCH_ADDRESSES?.split(
  "," // cspell:disable
) ?? ["8NyeR4GiwbneFMNfCNz2Q84Xbd2ks9QrlAD85QabQrw"]; // cspell:enable

export const aoAddresses = process.env.AO_ADDRESSES?.split(
  "," // cspell:disable
) ?? ["fcoN_xJeisVsPXA-trzVAuIiqO3ydLQxM-L4XbrQKzY"]; // cspell:enable

export const skipOpticalPostAddresses: string[] =
  process.env.SKIP_OPTICAL_POST_ADDRESSES?.split(",") ??
  redstoneOracleAddresses;

export const warpDedicatedBundlesPremiumFeatureType = "warp_dedicated_bundles";
export const redstoneOracleDedicatedBundlesPremiumFeatureType =
  "redstone_oracle_dedicated_bundles";
export const firstBatchDedicatedBundlesPremiumFeatureType =
  "first_batch_dedicated_bundles";
export const aoDedicatedBundlesPremiumFeatureType = "ao_dedicated_bundles";

export const premiumPaidFeatureTypes = [
  warpDedicatedBundlesPremiumFeatureType,
  redstoneOracleDedicatedBundlesPremiumFeatureType,
  firstBatchDedicatedBundlesPremiumFeatureType,
  aoDedicatedBundlesPremiumFeatureType,
] as const;
export type PremiumPaidFeatureType = (typeof premiumPaidFeatureTypes)[number];

export const allFeatureTypes = [
  ...premiumPaidFeatureTypes,
  defaultPremiumFeatureType,
] as const;
export type PremiumFeatureType = (typeof allFeatureTypes)[number];

export const dedicatedBundleTypes: Record<
  PremiumPaidFeatureType,
  {
    allowedWallets: string[];
    bundlerAppName?: string;
  }
> = {
  [warpDedicatedBundlesPremiumFeatureType]: {
    allowedWallets: warpWalletAddresses,
    bundlerAppName: "Warp",
  },

  [redstoneOracleDedicatedBundlesPremiumFeatureType]: {
    allowedWallets: redstoneOracleAddresses,
    bundlerAppName: "Redstone",
  },

  [firstBatchDedicatedBundlesPremiumFeatureType]: {
    allowedWallets: firstBatchAddresses,
    bundlerAppName: "FirstBatch",
  },

  [aoDedicatedBundlesPremiumFeatureType]: {
    allowedWallets: aoAddresses,
    bundlerAppName: "AO",
  },
} as const;

/**
 * This is the limit of `signature` on `new_data_item` and `planned_data_item`
 * If this value needs to be changed, a migration will be required to update the column type
 */
export const maxSignatureLength = 2055; // 2052 is MULTIAPTOS signature length

export const batchingSize = 100;

export const payloadDataStartS3MetaDataTag = "payload-data-start";
export const payloadContentTypeS3MetaDataTag = "payload-content-type";

export const defaultOverdueThresholdMs = +(
  (process.env.OVERDUE_DATA_ITEM_THRESHOLD_MS ?? 5 * 60 * 1000) // 5 minutes
);

export const blocklistedAddresses =
  process.env.BLOCKLISTED_ADDRESSES?.split(",") ?? [];

// allows providing a local JWK for testing purposes
export const turboLocalJwk = process.env.TURBO_JWK_FILE
  ? JSON.parse(fs.readFileSync(process.env.TURBO_JWK_FILE, "utf-8"))
  : undefined;

export const allowListedSignatureTypes = process.env
  .ALLOW_LISTED_SIGNATURE_TYPES
  ? new Set(process.env.ALLOW_LISTED_SIGNATURE_TYPES.split(",").map((s) => +s))
  : new Set([]);
