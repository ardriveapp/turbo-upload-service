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
import { SQSEvent } from "aws-lambda";
import { AxiosInstance } from "axios";
import CircuitBreaker from "opossum";
import winston from "winston";

import { createAxiosInstance } from "../arch/axiosClient";
import logger from "../logger";
import { getOpticalPubKey } from "../utils/getArweaveWallet";
import {
  SignedDataItemHeader,
  encodeTagsForOptical,
  getNestedDataItemHeaders,
  signDataItemHeader,
} from "../utils/opticalUtils";

/** These don't need to succeed */
const optionalOpticalUrls =
  process.env.OPTIONAL_OPTICAL_BRIDGE_URLS?.split(",");

let optionalCircuitBreakers: CircuitBreaker[] = [];
if (optionalOpticalUrls) {
  optionalCircuitBreakers = optionalOpticalUrls.map((url) => {
    return new CircuitBreaker(
      async (axios: AxiosInstance, postBody: unknown) => {
        return axios.post(url, postBody);
      },
      {
        timeout: 3_000, // If our function takes longer than 3 seconds, trigger a failure
        errorThresholdPercentage: 50, // When 50% of requests fail, trip the circuit
        resetTimeout: 30_000, // After 30 seconds, try again.
      }
    );
  });
}

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

  const dataItemIds = dataItemHeaders.map((header) => header.id);
  let childLogger = logger.child({ dataItemIds });

  // If any BDIs are detected, unpack them 1 nested level deep
  // and include the nested data items in the optical post
  // TODO: Filter this further for ArDrive data
  const nestedStringifiedHeaders = await Promise.all(
    (
      await getNestedDataItemHeaders({
        potentialBDIHeaders: dataItemHeaders,
        logger: childLogger,
      })
    ).map(async (nestedHeader) => {
      const opticalNestedHeader = await signDataItemHeader(
        encodeTagsForOptical(nestedHeader)
      );
      return JSON.stringify(opticalNestedHeader);
    })
  );
  const nestedDataItemIds = nestedStringifiedHeaders.map(
    (headerString) => (JSON.parse(headerString) as SignedDataItemHeader).id
  );
  childLogger = childLogger.child({ nestedDataItemIds });
  stringifiedDataItemHeaders = stringifiedDataItemHeaders.concat(
    nestedStringifiedHeaders
  );

  // Create a JSON array string out of the stringified headers
  const postBody = `[${stringifiedDataItemHeaders.join(",")}]`;
  const opticalPubKey = await getOpticalPubKey();

  childLogger.debug(`Posting to optical bridge...`);

  /** This one must succeed for the job to succeed */
  const primaryOpticalUrl = process.env.OPTICAL_BRIDGE_URL;
  if (!primaryOpticalUrl) {
    throw Error("OPTICAL_BRIDGE_URL is not set.");
  }

  const axios = createAxiosInstance({
    retries: 3,
    config: {
      validateStatus: () => true,
      headers: {
        "x-bundlr-public-key": opticalPubKey,
        "Content-Type": "application/json",
      },
    },
  });

  try {
    for (const circuitBreaker of optionalCircuitBreakers) {
      circuitBreaker
        .fire(axios, postBody)
        .then(() => {
          childLogger.debug(`Successfully posted to optional optical bridge`);
        })
        .catch((error) => {
          childLogger.error(
            `Failed to post to optional optical bridge: ${error.message}`
          );
        });
    }
    const { status, statusText } = await axios.post(
      primaryOpticalUrl,
      postBody
    );

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
    throw Error(
      `Failed to post to optical bridge with error: ${
        error instanceof Error ? error.message : "Unknown error"
      }`
    );
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
