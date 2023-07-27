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

import logger from "../logger";
import { getOpticalPubKey } from "../utils/getArweaveWallet";
import {
  SignedDataItemHeader,
  encodeTagsForOptical,
  getNestedDataItemHeaders,
  signDataItemHeader,
} from "../utils/opticalUtils";

export const handler = async (event: SQSEvent) => {
  let childLogger = logger.child({ job: "optical-post-job" });
  childLogger.info("Optical post job has been triggered.", event);

  let stringifiedDataItemHeaders = event.Records.map((record) => record.body);

  // Convert the stringified headers back into objects for nested bundle inspection
  const dataItemHeaders = stringifiedDataItemHeaders.map(
    (headerString) => JSON.parse(headerString) as SignedDataItemHeader
  );

  const dataItemIds = dataItemHeaders.map((header) => header.id);
  childLogger = childLogger.child({ dataItemIds });

  // If any BDIs are detected, unpack them 1 nested level deep
  // and include the nested data items in the optical post

  const nestedStringifiedHeaders = await Promise.all(
    (
      await getNestedDataItemHeaders({
        potentialBDIHeaders: dataItemHeaders,
        logger: childLogger,
      })
    ).map(async (nestedHeader) => {
      const opticalizedNestedHeader = await signDataItemHeader(
        encodeTagsForOptical(nestedHeader)
      );
      return JSON.stringify(opticalizedNestedHeader);
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

  childLogger.info(`Posting to optical bridge...`);
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
