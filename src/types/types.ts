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
import { Tag } from "arbundles";
import { CreateTransactionInterface } from "arweave/node/common";

export type Base64String = string;
export type PublicArweaveAddress = Base64String;
export type TransactionId = Base64String;
export type DataItemId = TransactionId;

export type ByteCount = number;

export * from "./winston";

export type TxAttributes = Partial<CreateTransactionInterface>;

// A local type describing the outputs from arbundles's processStream function
export type ParsedDataItemHeader = {
  id: Base64String;
  sigName: string;
  signature: Base64String;
  target: Base64String;
  anchor: Base64String;
  owner: Base64String;
  tags: Tag[];
  dataOffset: number;
  dataSize: number;
};
