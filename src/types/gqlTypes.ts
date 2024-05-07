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

export interface GQLPageInfoInterface {
  hasNextPage: boolean;
}

// Used to determine who the owner/submitter of a transaction.
export interface GQLOwnerInterface {
  address: string;
  key: string;
}

// Payment-related data, like the mining fee, the amount paid for the transaction, who received the AR ("0" for data-only transactions), as well as the address that initially sent the AR.
// This is also reused for the Quantity object for each GQLNodeInterface
export interface GQLAmountInterface {
  winston: string;
  ar: string;
}

// Relates to the data of the underlying transaction, like its size and content type.
export interface GQLMetaDataInterface {
  size: number;
  type: string;
}

// Used to access the tags embedded in a given Arweave transaction. You can retrieve both the tag name and the value as an array.
export interface GQLTagInterface {
  name: string;
  value: string;
}

// Used to build the GQL query
export interface GQLQueryTagInterface {
  name: string;
  value: string | string[];
}

// Details specific to a transaction's block. Used to retrieve its block number, mining date, block hash, and the previous block hash.
export interface GQLBlockInterface {
  id: string;
  timestamp: number;
  height: number;
  previous: string;
}

// the full Graphql structure that can be returned for a given item in a query
export interface GQLNodeInterface {
  id: string;
  anchor: string;
  signature: string;
  recipient: string;
  owner: GQLOwnerInterface;
  fee: GQLAmountInterface;
  quantity: GQLAmountInterface; // reuse the amount interface since the values are the same
  data: GQLMetaDataInterface;
  tags: GQLTagInterface[];
  block: GQLBlockInterface | null;
  bundledIn: {
    id: string;
    timestamp: number;
  } | null;
}

// The array of objects returned by a graphql query, including cursor which is used for result pagination.
// There are three components to pagination queries.
// First, when retrieving the GraphQL object, always make sure to retrieve the cursor. The cursor is used in queries to traverse to the next page.
// Second, specify the amount of elements to output by using the "first" key. When "first" is 5, the result set will include 5 transactions.
// And finally, specify the "after" string (i.e. the "cursor" from the previous page) to fetch the subsequent page.
export interface GQLEdgeInterface {
  cursor: string;
  node: GQLNodeInterface;
}

// The object structure returned by any graphql query
export interface GQLTransactionsResultInterface {
  pageInfo: GQLPageInfoInterface;
  edges: GQLEdgeInterface[];
}

export default interface GQLResultInterface {
  data: {
    transactions: GQLTransactionsResultInterface;
  };
}
