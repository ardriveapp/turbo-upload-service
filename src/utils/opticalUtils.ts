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
import { ArweaveSigner, Tag, deepHash, processStream } from "arbundles";
import { stringToBuffer } from "arweave/node/lib/utils";
import pLimit from "p-limit";
import winston from "winston";

import { ObjectStore } from "../arch/objectStore";
import { octetStreamContentType } from "../constants";
import { ParsedDataItemHeader } from "../types/types";
import { fromB64Url, ownerToAddress, toB64Url } from "./base64";
import { getOpticalWallet } from "./getArweaveWallet";
import { getDataItemData, getS3ObjectStore } from "./objectStoreUtils";

export type DataItemHeader = {
  id: string;
  owner: string; // The raw public key
  owner_address: string; // The base64url encoded sha256 hash of the owner string

  signature: string;
  target: string; // Empty string if unspecified
  content_type: string;
  tags: Tag[];
  data_size: number;
};

export type SignedDataItemHeader = DataItemHeader & {
  bundlr_signature: string;
};
export function encodeTagsForOptical(
  dataItemHeader: DataItemHeader
): DataItemHeader {
  const { tags } = dataItemHeader;
  const encodedTags = tags.map((tag) => {
    return {
      name: toB64Url(Buffer.from(tag.name)),
      value: toB64Url(Buffer.from(tag.value)),
    };
  });
  return {
    ...dataItemHeader,
    tags: encodedTags,
  };
}

export function decodeOpticalizedTags(
  dataItemHeader: DataItemHeader
): DataItemHeader {
  const { tags } = dataItemHeader;
  const decodedTags = tags.map((tag) => {
    return {
      name: fromB64Url(tag.name).toString(),
      value: fromB64Url(tag.value).toString(),
    };
  });
  return {
    ...dataItemHeader,
    tags: decodedTags,
  };
}

export async function signDataItemHeader(
  dataItemHeader: DataItemHeader
): Promise<SignedDataItemHeader> {
  const jwk = await getOpticalWallet();
  const arweaveSigner = new ArweaveSigner(jwk);
  const message = await deepHash([stringToBuffer(dataItemHeader.id)]);
  const bundlr_signature = Buffer.from(
    // Signer types are exported incorrectly from arbundles - we need to await this
    await arweaveSigner.sign(message)
  ).toString("base64url");
  return {
    ...dataItemHeader,
    bundlr_signature,
  };
}

export async function getNestedDataItemHeaders({
  potentialBDIHeaders,
  objectStore = getS3ObjectStore(),
  logger,
}: {
  potentialBDIHeaders: DataItemHeader[];
  objectStore?: ObjectStore;
  logger: winston.Logger;
}): Promise<DataItemHeader[]> {
  const decodedDataItemHeaders = potentialBDIHeaders.map(decodeOpticalizedTags);
  const headersRequiringUnpacking = decodedDataItemHeaders.filter(
    filterForNestedBundles
  );

  // Keep the parallelization of this work capped at sensible limits
  const bdiParallelLimit = pLimit(10);
  const bdiDataItemsIds = headersRequiringUnpacking.map((header) => header.id);
  const nestedHeadersPromises = bdiDataItemsIds.map((bdiDataItemId) => {
    return bdiParallelLimit(async () => {
      // Fetch the data item
      const dataItemReadable = await getDataItemData(
        objectStore,
        bdiDataItemId
      );

      logger.info("Processing BDI stream...", {
        bdiDataItemId,
      });

      // Process it as a bundle and get all the data item info
      const parsedDataItemHeaders = (await processStream(
        dataItemReadable
      )) as ParsedDataItemHeader[];

      logger.info("Finished processing BDI stream.", {
        bdiDataItemId,
        parsedDataItemHeaders,
      });

      // Return the encoded, signed, and serialized headers for all the nested data items
      const dataItemParallelLimit = pLimit(10);
      const nestedDataItemHeaders = await Promise.all(
        parsedDataItemHeaders.map((parsedDataItemHeader) => {
          return dataItemParallelLimit(async () => {
            const decodedTags = parsedDataItemHeader.tags;

            // Get content type from tag if possible
            const contentType =
              decodedTags
                .filter((tag) => tag.name.toLowerCase() === "content-type")
                .shift()?.value || octetStreamContentType;

            return {
              id: parsedDataItemHeader.id,
              signature: parsedDataItemHeader.signature,
              owner: parsedDataItemHeader.owner,
              owner_address: ownerToAddress(parsedDataItemHeader.owner),
              target: parsedDataItemHeader.target ?? "",
              content_type: contentType,
              data_size: parsedDataItemHeader.dataSize,
              tags: decodedTags,
            };
          });
        })
      );

      return nestedDataItemHeaders;
    });
  });

  return (await Promise.all(nestedHeadersPromises)).flat(1);
}

export function filterForNestedBundles(decodedHeader: DataItemHeader): boolean {
  return containsAns104Tags(decodedHeader.tags);
}

export function containsAns104Tags(tags: Tag[]) {
  const hasBundleFormatHeader = tags.some(
    (tag) => tag.name === "Bundle-Format" && tag.value === "binary"
  );
  return (
    hasBundleFormatHeader &&
    tags.some((tag) => tag.name === "Bundle-Version" && tag.value === "2.0.0")
  );
}
