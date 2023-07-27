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
import pLimit from "p-limit";
import { Readable } from "stream";

import { ArweaveGateway, Gateway } from "../arch/arweaveGateway";
import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { ObjectStore } from "../arch/objectStore";
import { PricingService } from "../arch/pricing";
import { createQueueHandler, enqueue } from "../arch/queues";
import { ArweaveInterface } from "../arweaveJs";
import {
  assembleBundleHeader,
  bundleHeaderInfoFromBuffer,
  totalBundleSizeFromHeaderInfo,
} from "../bundles/assembleBundleHeader";
import { rawIdFromRawSignature } from "../bundles/rawIdFromRawSignature";
import { signatureTypeInfo } from "../bundles/verifyDataItem";
import { gatewayUrl } from "../constants";
import logger from "../logger";
import { PlanId } from "../types/dbTypes";
import { JWKInterface } from "../types/jwkTypes";
import { W } from "../types/winston";
import { getArweaveWallet } from "../utils/getArweaveWallet";
import {
  assembleBundlePayload,
  getBundlePayload,
  getRawSignatureOfDataItem,
  getS3ObjectStore,
  getSignatureTypeOfDataItem,
  putBundleHeader,
  putBundlePayload,
  putBundleTx,
} from "../utils/objectStoreUtils";
import { streamToBuffer } from "../utils/streamToBuffer";

// eslint-disable-next-line @typescript-eslint/no-var-requires
const { version } = require("../../package.json");
const PARALLEL_LIMIT = 10;
interface PrepareBundleJobInjectableArch {
  database?: Database;
  objectStore?: ObjectStore;
  jwk?: JWKInterface;
  pricing?: PricingService;
  gateway?: Gateway;
  arweave?: ArweaveInterface;
}

/**
 * Uses next bundle plan from to prepare a Bundle Transaction for posting
 *
 * - Gets planned Data Items from DB
 * - Assembles Bundle Header onto Object Store
 * - Assembles and signs Bundle tx onto ObjectStore
 * - Inserts NewBundle into DB for Post job
 */
export async function prepareBundleHandler(
  planId: PlanId,
  {
    database = new PostgresDatabase(),
    objectStore = getS3ObjectStore(),
    jwk,
    gateway = new ArweaveGateway({
      endpoint: gatewayUrl,
    }),
    pricing = new PricingService(gateway),
    arweave = new ArweaveInterface(),
  }: PrepareBundleJobInjectableArch
): Promise<void> {
  logger.child({ job: "prepare-bundle-job" });

  if (!jwk) {
    jwk = await getArweaveWallet();
  }

  const dbDataItems = await database.getPlannedDataItemsForPlanId(planId);

  logger.info(`Preparing data items.`, {
    planId,
    dataItems: dbDataItems,
  });

  // Assemble bundle header
  // This could be done in plan job -- or another specific bundleHeader job
  const parallelLimit = pLimit(PARALLEL_LIMIT);
  const dataItemRawIdsAndByteCounts = await Promise.all(
    dbDataItems.map(({ byteCount, dataItemId, signatureType }) => {
      return parallelLimit(async () => {
        logger.info("Getting raw signature of data item.", {
          dataItemId,
          planId,
        });
        const sigType =
          signatureType ??
          (await getSignatureTypeOfDataItem(objectStore, dataItemId));
        const dataItemRawSignature = await getRawSignatureOfDataItem(
          objectStore,
          dataItemId,
          sigType
        );
        logger.info("Retrieved raw signature of data item.", {
          dataItemId,
          planId,
        });
        const dataItemRawId = await rawIdFromRawSignature(
          dataItemRawSignature,
          signatureTypeInfo[sigType].signatureLength
        );
        logger.info("Parsed data item raw id.", {
          dataItemRawId,
          dataItemId,
          planId,
        });
        return { dataItemRawId, byteCount };
      });
    })
  );
  logger.info("Assembling bundle header.", {
    planId,
  });
  const bundleHeaderReadable = await assembleBundleHeader(
    dataItemRawIdsAndByteCounts
  );
  const bundleHeaderBuffer = await streamToBuffer(bundleHeaderReadable);
  logger.info("Caching bundle header.", {
    planId,
  });
  await putBundleHeader(objectStore, planId, Readable.from(bundleHeaderBuffer));
  // Bundle header end
  logger.info("Successfully cached bundle header.", {
    planId,
  });

  // Call pricing service to determine reward and tip settings for bundle
  const txAttributes = await pricing.getTxAttributesForDataItems(dbDataItems);

  // Assemble bundle transaction
  const dataItemCount = dbDataItems.length;
  const totalDataItemsSize = dbDataItems.reduce(
    (acc, dataItem) => acc + dataItem.byteCount,
    0
  );
  logger.info("Assembling bundle transaction.", {
    planId,
    dataItemCount,
    totalDataItemsSize,
  });

  const totalPayloadSize = totalBundleSizeFromHeaderInfo(
    bundleHeaderInfoFromBuffer(bundleHeaderBuffer)
  );
  logger.info("Caching bundle payload.", {
    planId,
    payloadSize: totalPayloadSize,
  });
  await putBundlePayload(
    objectStore,
    planId,
    await assembleBundlePayload(objectStore, bundleHeaderBuffer)
    // HACK: Attempting to remove totalPayloadSize to appease AWS V3 SDK
    // totalPayloadSize
  );
  const headerByteCount = bundleHeaderBuffer.byteLength;

  const bundleTx = await arweave.createTransactionFromPayloadStream(
    await getBundlePayload(
      objectStore,
      planId,
      headerByteCount,
      totalPayloadSize
    ),
    txAttributes,
    jwk
  );

  logger.info("Successfully assembled bundle transaction.", {
    planId,
    txId: bundleTx.id,
  });
  bundleTx.addTag("Bundle-Format", "binary");
  bundleTx.addTag("Bundle-Version", "2.0.0");

  bundleTx.addTag("App-Name", process.env.APP_NAME ?? "ArDrive Turbo");
  bundleTx.addTag("App-Version", version);

  // Mint $U
  bundleTx.addTag("App-Name", "SmartWeaveAction");
  bundleTx.addTag("App-Version", "0.3.0");
  bundleTx.addTag("Input", '{ "function": "mint" }');
  bundleTx.addTag("Contract", "KTzTXT_ANmF84fWEKHzWURD1LWd9QaFR9yfYUwH2Lxw");

  await arweave.signTx(bundleTx, jwk);

  logger.info("Successfully signed bundle transaction.", {
    planId,
    txId: bundleTx.id,
  });
  bundleTx.data = new Uint8Array(0);
  const serializedBundleTx = JSON.stringify(bundleTx.toJSON());
  const bundleTxBuffer = Buffer.from(serializedBundleTx);

  logger.info("Updating object-store with bundled transaction.", {
    planId,
    txId: bundleTx.id,
  });

  await putBundleTx(objectStore, bundleTx.id, Readable.from(bundleTxBuffer));

  await database.insertNewBundle({
    planId,
    bundleId: bundleTx.id,
    reward: W(bundleTx.reward),
    payloadByteCount: totalPayloadSize,
    headerByteCount,
    transactionByteCount: bundleTxBuffer.byteLength,
  });

  await enqueue("post-bundle", { planId });

  logger.info("Successfully updated object-store with bundled transaction.", {
    planId,
    txId: bundleTx.id,
  });
}
export const handler = createQueueHandler(
  "prepare-bundle",
  (message: { planId: PlanId }) => prepareBundleHandler(message.planId, {}),
  {
    before: async () => {
      logger.info("Prepare bundle job has been triggered.");
    },
  }
);
