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
import { SQSEvent } from "aws-lambda";
import axios from "axios";
import winston from "winston";

import logger from "../logger";
import { getOpticalPubKey } from "../utils/getArweaveWallet";
import {
  SignedDataItemHeader,
  encodeTagsForOptical,
  getNestedDataItemHeaders,
  signDataItemHeader,
} from "../utils/opticalUtils";

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
  try {
    const { status, statusText } = await axios.post(
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      process.env.OPTICAL_BRIDGE_URL!,
      postBody,
      {
        headers: {
          "x-bundlr-public-key": opticalPubKey,
        },
      }
    );
    childLogger.info("Successfully posted to optical bridge.", {
      status,
      statusText,
    });
  } catch (error) {
    childLogger.error("Failed to post to optical bridge!", error);
    throw error;
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
