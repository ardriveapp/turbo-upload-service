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
import { ReadThroughPromiseCache } from "@ardrive/ardrive-promise-cache";
import { GetParameterCommand } from "@aws-sdk/client-ssm";
import { SQSEvent } from "aws-lambda";
import CircuitBreaker from "opossum";
import winston from "winston";

import { ssmClient } from "../arch/ssmClient";
import logger from "../logger";
import {
  BreakerSource,
  MetricRegistry,
  setUpCircuitBreakerListenerMetrics,
} from "../metricRegistry";
import { fromB64Url, toB64Url } from "../utils/base64";
import { getOpticalPubKey } from "../utils/getArweaveWallet";
import { RetryHttpClient, createRetryHttpClient } from "../utils/httpClient";
import { SignedDataItemHeader } from "../utils/opticalUtils";

// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
const primaryOpticalUrl = process.env.OPTICAL_BRIDGE_URL!;
if (!primaryOpticalUrl) {
  logger.warn("OPTICAL_BRIDGE_URL is not set.");
}

const arDriveGatewayAdminKeyCache = new ReadThroughPromiseCache<string, string>(
  {
    cacheParams: {
      cacheCapacity: 1,
      cacheTTLMillis: 900_000, // 15 minutes
    },
    readThroughFunction: async (ssmParamName) => {
      const result = await ssmClient.send(
        new GetParameterCommand({
          Name: "/upload-service/admin-keys/" + ssmParamName,
        })
      );
      if (!result.Parameter?.Value) {
        throw new Error("No admin key found in SSM");
      }
      return result.Parameter.Value;
    },
  }
);

const stringToB64 = (str: string) => toB64Url(Buffer.from(str));
const b64UrlStrings = {
  "App-Name": stringToB64("App-Name"),
  ArDrive: stringToB64("ArDrive"),
  "Data-Protocol": stringToB64("Data-Protocol"),
  ao: stringToB64("ao"),
  Nonce: stringToB64("Nonce"),
  Type: stringToB64("Type"),
  "Scheduler-Location": stringToB64("Scheduler-Location"),
  Checkpoint: stringToB64("Checkpoint"),
  Process: stringToB64("Process"),
  Module: stringToB64("Module"),
  Assignment: stringToB64("Assignment"),
  "0": stringToB64("0"),
};

const canaryOpticalUrl = process.env.CANARY_OPTICAL_BRIDGE_URL;
const canaryOpticalSampleRate = Number.parseInt(
  process.env.CANARY_OPTICAL_SAMPLE_RATE ?? "0"
);

/** These don't need to succeed */
const optionalOpticalUrls =
  process.env.OPTIONAL_OPTICAL_BRIDGE_URLS?.split(",");

const arDriveGatewayOpticalUrlAndApiKeyPairs =
  process.env.ARDRIVE_GATEWAY_OPTICAL_URLS?.split(",")
    ?.map((pair) => {
      // eslint-disable-next-line prefer-const
      let [url, ssmParamName] = pair.split("|");
      return { url, ssmParamName };
    })
    ?.filter(({ url }) => !!url) ?? [];

let cachedAxios: RetryHttpClient | undefined = undefined;

export const opticalPostHandler = async ({
  stringifiedDataItemHeaders,
  logger,
}: {
  stringifiedDataItemHeaders: string[];
  logger: winston.Logger;
}) => {
  // Convert the stringified headers back into objects for nested bundle inspection
  const dataItemHeaders = stringifiedDataItemHeaders.map(
    (headerString) => JSON.parse(headerString) as SignedDataItemHeader
  );

  const dataItemStringifiedHeadersToSendToPrimaryOptical = dataItemHeaders
    .filter(({ tags }) => {
      const dataProtocol = tags.find(
        (tag) => tag.name === b64UrlStrings["Data-Protocol"]
      )?.value;
      const type = tags.find(
        (tag) => tag.name === b64UrlStrings["Type"]
      )?.value;
      const nonce = tags.find(
        (tag) => tag.name === b64UrlStrings["Nonce"]
      )?.value;
      const isAOMsg = dataProtocol === b64UrlStrings["ao"];
      const isLowPriorityAOMessage =
        isAOMsg &&
        type !== b64UrlStrings["Scheduler-Location"] &&
        type !== b64UrlStrings["Checkpoint"] &&
        type !== b64UrlStrings["Process"] &&
        type !== b64UrlStrings["Module"] &&
        !(type === b64UrlStrings["Assignment"] && nonce === b64UrlStrings["0"]);

      return !isLowPriorityAOMessage;
    })
    .map((header) => JSON.stringify(header));

  const dataItemStringifiedHeadersToSendToArDriveOptical = dataItemHeaders
    .filter(({ tags }) => {
      // Find tags that where the "App-Name" starts with "ArDrive"
      const appName = tags.find(
        (tag) => tag.name === b64UrlStrings["App-Name"]
      )?.value;
      if (!appName) {
        return false;
      }
      const decodedAppName = fromB64Url(appName).toString("utf-8");
      return decodedAppName.startsWith("ArDrive");
    })
    .map((header) => JSON.stringify(header));

  const dataItemIds = dataItemHeaders.map((header) => header.id);
  const childLogger = logger.child({ dataItemIds });

  // Create a JSON array string out of the stringified headers
  const arDrivePostBody = `[${dataItemStringifiedHeadersToSendToArDriveOptical.join(
    ","
  )}]`;
  const optionalPostBody = `[${stringifiedDataItemHeaders.join(",")}]`;
  const primaryPostBody = `[${dataItemStringifiedHeadersToSendToPrimaryOptical.join(
    ","
  )}]`;
  const opticalPubKey = await getOpticalPubKey();

  childLogger.debug(`Posting to optical bridge...`, {
    numPrimaryOpticalItems:
      dataItemStringifiedHeadersToSendToPrimaryOptical.length,
    numOptionalOpticalItems: optionalOpticalUrls
      ? dataItemStringifiedHeadersToSendToArDriveOptical.length
      : 0,
    numArDriveOpticalItems:
      dataItemStringifiedHeadersToSendToArDriveOptical.length,
  });

  /** This one must succeed for the job to succeed */
  const primaryOpticalUrl = process.env.OPTICAL_BRIDGE_URL;
  if (!primaryOpticalUrl) {
    throw Error("OPTICAL_BRIDGE_URL is not set.");
  }

  const headers: Record<string, string> = {
    "x-bundlr-public-key": opticalPubKey,
    "Content-Type": "application/json",
  };
  if (process.env.AR_IO_ADMIN_KEY !== undefined) {
    headers["Authorization"] = `Bearer ${process.env.AR_IO_ADMIN_KEY}`;
  }

  const getAxios = () => {
    cachedAxios ??= createRetryHttpClient({
      maxTries: 4,
      config: {
        headers,
      },
    });
    return cachedAxios;
  };

  try {
    for (const optionalUrl of optionalOpticalUrls ?? []) {
      void breakerForOpticalUrl(optionalUrl)
        .fire(async () => {
          return getAxios().post(optionalUrl, optionalPostBody);
        })
        .then(() => {
          childLogger.debug(`Successfully posted to optional optical bridge`);
        })
        .catch((error) => {
          childLogger.error(
            `Failed to post to optional optical bridge: ${error.message}`,
            {
              optionalUrl,
            }
          );
          // TODO: make this choice part of configuration
          MetricRegistry.goldskyOpticalFailure.inc();
        });
    }

    if (dataItemStringifiedHeadersToSendToArDriveOptical.length !== 0) {
      for (const {
        url,
        ssmParamName,
      } of arDriveGatewayOpticalUrlAndApiKeyPairs) {
        const apiKey = ssmParamName
          ? await arDriveGatewayAdminKeyCache.get(ssmParamName).catch((e) => {
              childLogger.error(
                `Failed to get admin key for ${ssmParamName}: ${e.message}`
              );
              return undefined;
            })
          : undefined;
        void breakerForOpticalUrl(url)
          .fire(async () => {
            return getAxios().post(url, arDrivePostBody, {
              headers: apiKey ? { Authorization: `Bearer ${apiKey}` } : {},
            });
          })
          .then(() => {
            childLogger.debug(
              `Successfully posted to ardrive gateway optical bridge`
            );
          })
          .catch((error) => {
            childLogger.error(
              `Failed to post to ardrive gateway optical bridge: ${error.message}`
            );
            // TODO: Make this choice part of configuration
            MetricRegistry.ardriveGatewayOpticalFailure.inc();
          });
      }
    } else {
      childLogger.debug(
        `No data items to send to ardrive gateway optical bridge. Skipping.`
      );
    }

    if (dataItemStringifiedHeadersToSendToPrimaryOptical.length === 0) {
      childLogger.debug(
        `No data items to send to primary optical bridge. Skipping.`
      );
      return;
    }

    const { status, statusText } = await breakerForOpticalUrl(
      primaryOpticalUrl
    ).fire(async () => {
      return getAxios().post(primaryOpticalUrl, primaryPostBody);
    });

    if (status < 200 || status >= 300) {
      throw Error(
        `Failed to post to primary optical bridge: ${status} ${statusText}`
      );
    }

    childLogger.debug(
      `Successfully posted to primary and ${
        optionalOpticalUrls?.length ?? 0
      } optional optical bridges.`,
      {
        status,
        statusText,
      }
    );
  } catch (error) {
    childLogger.error("Failed to post to optical bridge!", {
      error: error instanceof Error ? error.message : error,
    });
    MetricRegistry.legacyGatewayOpticalFailure.inc();
    throw Error(
      `Failed to post to optical bridge with error: ${
        error instanceof Error ? error.message : "Unknown error"
      }`
    );
  }

  if (
    dataItemHeaders.length !==
    dataItemStringifiedHeadersToSendToPrimaryOptical.length
  ) {
    childLogger.info(
      "Some data items were filtered out and not sent to primary optical bridge.",
      {
        numDataItemsFiltered:
          dataItemHeaders.length -
          dataItemStringifiedHeadersToSendToPrimaryOptical.length,
      }
    );
  }

  if (
    canaryOpticalUrl &&
    !Number.isNaN(canaryOpticalSampleRate) &&
    canaryOpticalSampleRate > 0
  ) {
    const diceRoll = Math.random();
    if (diceRoll < canaryOpticalSampleRate) {
      try {
        await getAxios().post(canaryOpticalUrl, primaryPostBody);
        childLogger.debug(`Successfully posted to canary optical bridge.`);
      } catch (error) {
        childLogger.error("Failed to post to canary optical bridge!", {
          error: error instanceof Error ? error.message : error,
        });
      }
    }
  }
};

// Lambda version with batched records
export const handler = async (sqsEvent: SQSEvent) => {
  const childLogger = logger.child({ job: "optical-post-job" });
  childLogger.info("Optical post lambda handler has been triggered.");
  childLogger.debug("Optical post sqsEvent:", sqsEvent);
  return opticalPostHandler({
    stringifiedDataItemHeaders: sqsEvent.Records.map((record) => record.body),
    logger: childLogger,
  });
};

// A helper type that will allow us to pass around closures involving CacheService activities
type OpticalTask<T> = () => Promise<T>;

// In the future we may have multiple cache services, so we use a WeakMap to store
// the circuit breaker for each service. WeakMap allows for object keys.
type URLString = string;
const opticalBreakers = new Map<
  URLString,
  {
    fire<T>(task: OpticalTask<T>): Promise<T>;
    breaker: CircuitBreaker<[OpticalTask<unknown>], unknown>;
  }
>();

// TODO: Move this mapping to configuration
function breakerNameForUrl(url: URLString): BreakerSource {
  if (url.includes("goldsky")) {
    return "optical_goldsky";
  }
  if (url.includes("ardrive")) {
    return "optical_ardriveGateway";
  }
  if (url === primaryOpticalUrl) {
    return "optical_legacyGateway";
  }
  return "unknown";
}

function breakerForOpticalUrl(url: URLString): {
  fire<T>(task: OpticalTask<T>): Promise<T>;
  breaker: CircuitBreaker<[OpticalTask<unknown>], unknown>;
} {
  const existing = opticalBreakers.get(url);
  if (existing) return existing;

  // Use a rest parameter to indicate that the argument is a tuple
  const breaker = new CircuitBreaker<[OpticalTask<unknown>], unknown>(
    async (...args: [OpticalTask<unknown>]) => {
      const [task] = args;
      return task();
    },
    {
      timeout: process.env.NODE_ENV === "local" ? 7777 : 3000,
      errorThresholdPercentage: 50,
      resetTimeout: 30_000,
    }
  );

  setUpCircuitBreakerListenerMetrics(breakerNameForUrl(url), breaker, logger);
  breaker.on("timeout", () =>
    logger.error("Optical circuit breaker command timed out")
  );

  // This wrapper accomplishes two important things:
  // 1. It allows us to get type-safe returns for the task function passed to fire()
  // 2. It provides access to the breaker itself for external use cases
  const wrapper = {
    fire<T>(task: OpticalTask<T>): Promise<T> {
      return breaker.fire(task) as Promise<T>;
    },
    breaker,
  };

  opticalBreakers.set(url, wrapper);
  return wrapper;
}
