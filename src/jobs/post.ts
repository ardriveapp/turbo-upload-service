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
import { defaultArchitecture } from "../arch/architecture";
import { ArweaveGateway, Gateway } from "../arch/arweaveGateway";
import { Database } from "../arch/db/database";
import { PostgresDatabase } from "../arch/db/postgres";
import { ObjectStore } from "../arch/objectStore";
import { PaymentService, TurboPaymentService } from "../arch/payment";
import { createQueueHandler, enqueue } from "../arch/queues";
import { gatewayUrl } from "../constants";
import defaultLogger from "../logger";
import { MetricRegistry } from "../metricRegistry";
import { PlanId } from "../types/dbTypes";
import { Winston } from "../types/winston";
import { ownerToNormalizedB64Address } from "../utils/base64";
import { getBundleTx, getS3ObjectStore } from "../utils/objectStoreUtils";

interface PostBundleJobInjectableArch {
  database?: Database;
  objectStore?: ObjectStore;
  arweaveGateway?: Gateway;
  paymentService?: PaymentService;
}

export async function postBundleHandler(
  planId: PlanId,
  {
    database = new PostgresDatabase(),
    objectStore = getS3ObjectStore(),
    arweaveGateway = new ArweaveGateway({
      endpoint: gatewayUrl,
    }),
    paymentService = new TurboPaymentService(),
  }: PostBundleJobInjectableArch,
  logger = defaultLogger.child({ job: "post-bundle-job", planId })
) {
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

  logger.debug(`Bundle Transaction details.`, { planId, bundleTx });

  try {
    // post bundle, throw error on failure
    const transactionPostResponseData = await arweaveGateway.postBundleTx(
      bundleTx
    );

    // fetch AR rate - but don't throw on failure
    const usdToArRate = await paymentService
      .getFiatToARConversionRate("usd")
      .catch((err) => {
        MetricRegistry.usdToArRateFail.inc();
        logger.error("Failed to fetch USD/AR rate", {
          err,
        });
        return undefined;
      });

    logger.info("Successfully posted bundle for transaction.", {
      bundleId,
      response: transactionPostResponseData,
      usdToArRate,
    });

    await database.insertPostedBundle({ bundleId, usdToArRate });
    await enqueue("seed-bundle", { planId });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Unknown error.";
    logger.error("Post Bundle Job has failed!", {
      bundleId,
      error: message,
    });

    const balance = await arweaveGateway.getBalanceForWallet(
      ownerToNormalizedB64Address(bundleTx.owner)
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
    // We also do not care about the USD/AR rate if posting fails, so we do not pass it in
    return database.updateNewBundleToFailedToPost(planId, bundleId);
  }
}
export const handler = createQueueHandler(
  "post-bundle",
  (message) => postBundleHandler(message.planId, defaultArchitecture),
  {
    before: async () => {
      defaultLogger.debug("Post bundle job has been triggered.");
    },
  }
);
