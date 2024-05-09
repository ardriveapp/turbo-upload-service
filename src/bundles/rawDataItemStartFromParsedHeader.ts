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
import { serializeTags } from "arbundles";

import { ParsedDataItemHeader } from "../types/types";
import { sigNameToSigInfo } from "./verifyDataItem";

export function rawDataItemStartFromParsedHeader({
  anchor,
  dataOffset,
  tags,
  sigName,
  target,
}: ParsedDataItemHeader): number {
  const { pubkeyLength, signatureLength } = sigNameToSigInfo[sigName];

  const signatureTypeLength = 2;
  const targetLength = target ? 33 : 1;
  const anchorLength = anchor ? 33 : 1;
  const tagsLength = 16 + serializeTags(tags).byteLength;

  return (
    dataOffset -
    signatureTypeLength -
    signatureLength -
    pubkeyLength -
    targetLength -
    anchorLength -
    tagsLength
  );
}
