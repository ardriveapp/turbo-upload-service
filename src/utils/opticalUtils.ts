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
import { ArweaveSigner, Tag, deepHash } from "@dha-team/arbundles";
import { stringToBuffer } from "arweave/node/lib/utils";

import { fromB64Url, toB64Url } from "./base64";
import { getOpticalWallet } from "./getArweaveWallet";

export type DataItemHeader = {
  id: string;
  owner: string; // The raw public key
  owner_address: string; // The base64url encoded sha256 hash of the owner string - TODO: VERIFY
  signature: string;
  target: string; // Empty string if unspecified
  content_type: string;
  tags: Tag[];
  data_size: number;
};

export type SignedDataItemHeader = DataItemHeader & {
  bundlr_signature: string; // TODO: Update optical bridge to use bundler_signature
};

export type DatedSignedDataItemHeader = SignedDataItemHeader & {
  uploaded_at: number;
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
