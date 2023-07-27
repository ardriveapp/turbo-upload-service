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
import { ArweaveGateway, Gateway } from "../arch/arweaveGateway";
import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { ObjectStore } from "../arch/objectStore";
import { createQueueHandler, enqueue } from "../arch/queues";
import { gatewayUrl } from "../constants";
import logger from "../logger";
import { PlanId } from "../types/dbTypes";
import { Winston } from "../types/winston";
import { ownerToAddress } from "../utils/base64";
import { getBundleTx, getS3ObjectStore } from "../utils/objectStoreUtils";

interface PostBundleJobInjectableArch {
  database?: Database;
  objectStore?: ObjectStore;
  gateway?: Gateway;
}

export async function postBundleHandler(
  planId: PlanId,
  {
    database = new PostgresDatabase(),
    objectStore = getS3ObjectStore(),
    gateway = new ArweaveGateway({
      endpoint: gatewayUrl,
    }),
  }: PostBundleJobInjectableArch
) {
  logger.child({ job: "post-bundle-job" });

  const dbNextBundle = await database.getNextBundleToPostByPlanId(planId);
  const { bundleId, transactionByteCount } = dbNextBundle;

  logger.info(`Posting bundle.`, {
    // Log entire NewBundle from database (includes planId and bundleId)
    bundle: dbNextBundle,
  });
  const bundleTx = await getBundleTx(
    objectStore,
    bundleId,
    transactionByteCount
  );

  logger.info(`Bundle Transaction details.`, { planId, bundleTx });

  try {
    const transactionPostResponseData = await gateway.postBundleTx(bundleTx);
    logger.info("Successfully posted bundle for transaction.", {
      planId,
      bundleId,
      response: transactionPostResponseData,
    });

    await database.insertPostedBundle(bundleId);
    await enqueue("seed-bundle", { planId });
  } catch (error) {
    logger.error("Post Bundle Job has failed!", {
      planId,
      bundleId,
      error,
    });

    const balance = await gateway.getBalanceForWallet(
      ownerToAddress(bundleTx.owner)
    );

    if (new Winston(bundleTx.reward).isGreaterThan(balance)) {
      // During an error to post, if the wallet the signed the bundle does not have enough balance
      // for the reward, we will throw an error so this job will go to DLQ and alert us
      throw Error(
        `Wallet does not have enough balance for this bundle post! Current Balance: ${balance}, Reward for Bundle: ${bundleTx.reward}`
      );
    }

    // For other failure reasons, insert as a failed to post bundle without throwing error
    // The planned_data_items in the bundle will be demoted to new_data_items
    return database.insertFailedToPostBundle(bundleId);
  }
}
export const handler = createQueueHandler(
  "post-bundle",
  (message) => postBundleHandler(message.planId, {}),
  {
    before: async () => {
      logger.info("Post bundle job has been triggered.");
    },
  }
);
