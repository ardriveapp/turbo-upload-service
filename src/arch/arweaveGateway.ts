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
import { ReadThroughPromiseCache } from "@ardrive/ardrive-promise-cache";
import Transaction from "arweave/node/lib/transaction";
import axios, { AxiosInstance, AxiosResponse } from "axios";

import { gatewayUrl, msPerMinute } from "../constants";
import logger from "../logger";
import GQLResultInterface from "../types/gqlTypes";
import {
  ConfirmedTransactionStatus,
  TransactionStatus,
  isConfirmedTransactionStatus,
} from "../types/txStatus";
import { ByteCount, PublicArweaveAddress, TransactionId } from "../types/types";
import { W, Winston } from "../types/winston";
import {
  ExponentialBackoffRetryStrategy,
  RetryStrategy,
} from "./retryStrategy";

interface GatewayAPIConstParams {
  endpoint?: URL;
  retryStrategy?: RetryStrategy<AxiosResponse>;
  axiosInstance?: AxiosInstance;
}

const currentBlockInfoCacheKey = "currentBlockInfo";

export interface Gateway {
  getWinstonPriceForByteCount(
    byteCount: ByteCount,
    target?: PublicArweaveAddress
  ): Promise<Winston>;

  postToEndpoint<T = unknown>(
    endpoint: string,
    data?: unknown
  ): Promise<AxiosResponse<T>>;

  postBundleTx(bundleTx: Transaction): Promise<Transaction>;

  getTransactionStatus(
    transactionId: TransactionId
  ): Promise<TransactionStatus>;

  getBlockHash(): Promise<string>;
  getBlockHeightForTxAnchor(txAnchor: string): Promise<number>;
  getCurrentBlockHeight(): Promise<number>;
  getBalanceForWallet(wallet: PublicArweaveAddress): Promise<Winston>;
  getDataItemsFromGQL(dataItemIds: TransactionId[]): Promise<
    {
      id: TransactionId;
      blockHeight?: number;
      bundledIn?: TransactionId;
    }[]
  >;
}

export class ArweaveGateway implements Gateway {
  private endpoint: URL;
  private retryStrategy: RetryStrategy<AxiosResponse>;
  private axiosInstance: AxiosInstance;

  constructor({
    endpoint = gatewayUrl,
    retryStrategy = new ExponentialBackoffRetryStrategy({}),
    axiosInstance = axios.create({ validateStatus: undefined }),
  }: GatewayAPIConstParams) {
    this.endpoint = endpoint;
    this.retryStrategy = retryStrategy;
    this.axiosInstance = axiosInstance;
  }

  public async postToEndpoint<D, T>(
    endpoint: string,
    data?: D
  ): Promise<AxiosResponse<T>> {
    return this.retryStrategy.sendRequest(() =>
      this.axiosInstance.post(`${this.endpoint.href}${endpoint}`, data)
    );
  }

  public async getWinstonPriceForByteCount(
    byteCount: ByteCount,
    target?: PublicArweaveAddress
  ): Promise<Winston> {
    return W(
      +(
        await this.retryStrategy.sendRequest(() =>
          this.axiosInstance.get<string>(
            `${this.endpoint.href}price/${byteCount}${
              target ? `/${target}` : ""
            }`
          )
        )
      ).data
    );
  }

  public async getTransactionStatus(
    transactionId: TransactionId
  ): Promise<TransactionStatus> {
    logger.debug("Getting transaction status...", { transactionId });
    const statusResponse =
      await new ExponentialBackoffRetryStrategy<AxiosResponse>({
        validStatusCodes: [200, 202, 404],
      }).sendRequest(() =>
        this.axiosInstance.get<ConfirmedTransactionStatus>(
          `${this.endpoint.href}tx/${transactionId}/status`
        )
      );

    if (statusResponse.data) {
      if (statusResponse.status === 404) {
        logger.debug("Transaction not found...", { transactionId });
        return { status: "not found" };
      }
      if (statusResponse.data === "Pending") {
        logger.debug("Transaction is pending...", { transactionId });
        return {
          status: "pending",
        };
      }
      if (isConfirmedTransactionStatus(statusResponse.data)) {
        return { status: "found", transactionStatus: statusResponse.data };
      }

      logger.error("Unknown status shape returned!", {
        transactionId,
        status: statusResponse.data,
      });
    }

    logger.error("Unable to derive transaction status from response!", {
      transactionId,
      response: statusResponse,
    });

    return { status: "not found" };
  }

  public async postBundleTx(bundleTx: Transaction): Promise<Transaction> {
    logger.debug("Posting bundle tx id.", {
      txId: bundleTx.id,
    });
    const response = await this.postToEndpoint<Transaction, Transaction>(
      "tx",
      bundleTx
    );
    return response.data;
  }

  public async getBlockHash(): Promise<string> {
    return (
      await this.retryStrategy.sendRequest(() =>
        this.axiosInstance.get<string>(`${this.endpoint.href}tx_anchor`)
      )
    ).data;
  }

  private gqlPageSize = 100;
  public async getDataItemsFromGQL(dataItemIds: TransactionId[]): Promise<
    {
      id: TransactionId;
      blockHeight?: number;
      bundledIn?: TransactionId;
    }[]
  > {
    if (dataItemIds.length > this.gqlPageSize) {
      // TODO: Can support pagination from if needed here, but for now we are batching for breaking up db inserts
      throw Error(
        `Cannot query more than ${this.gqlPageSize} data items at a time. This method expects pre-batching of data item ids`
      );
    }

    try {
      logger.debug("Checking if data items can be found on GQL...", {
        dataItemIds,
      });

      const dataItems = await this.axiosInstance
        .post<GQLResultInterface>(this.endpoint.href + "graphql", {
          query: `
                query {
                  transactions(ids: [${dataItemIds.map(
                    (id) => `"${id}"`
                  )}] first: ${this.gqlPageSize}) {
                    edges {
                      node {
                        id
                        block {
                          height
                        }
                        bundledIn {
                          id
                        }
                      }
                    }
                  }
                }`,
        })
        .then((statusResponse) => {
          if (statusResponse?.data?.data?.transactions?.edges?.length > 0) {
            return statusResponse.data.data.transactions.edges.map((edge) => ({
              id: edge.node.id,
              blockHeight: edge.node.block?.height,
              bundledIn: edge.node.bundledIn?.id,
            }));
          }
          return [];
        });

      return dataItems;
    } catch (error) {
      logger.error("Error querying transaction on GQL", error);
      throw error;
    }
  }

  public async getBlockHeightForTxAnchor(txAnchor: string): Promise<number> {
    try {
      const statusResponse = await this.axiosInstance.post(
        this.endpoint.href + "graphql",
        {
          query: `
          query {
            blocks(ids: ["${txAnchor}"]) {
              edges {
                node {
                  id
                  height
                }
              }
            }
          }
          
          `,
        }
      );

      if (statusResponse?.data?.data?.blocks?.edges[0]) {
        const height = statusResponse.data.data.blocks.edges[0].node.height;

        logger.debug("Successfully fetched block height for tx_anchor", {
          height,
          txAnchor,
        });
        return height;
      } else {
        throw Error("Could not fetch tx anchor");
      }
    } catch (error) {
      logger.error("Error getting block height for tx anchor", error);
      throw error;
    }
  }

  private currentBlockInfoCache = new ReadThroughPromiseCache<
    string,
    { blockHeight: number; timestamp: number }
  >({
    cacheParams: {
      cacheCapacity: 1,
      cacheTTL: msPerMinute,
    },
    readThroughFunction: async () => {
      return this.getCurrentBlockInfoInternal();
    },
  });

  public async getCurrentBlockHeight(): Promise<number> {
    return (await this.currentBlockInfoCache.get(currentBlockInfoCacheKey))
      .blockHeight;
  }

  public async getCurrentBlockTimestamp(): Promise<number> {
    return (await this.currentBlockInfoCache.get(currentBlockInfoCacheKey))
      .timestamp;
  }

  private async getCurrentBlockInfoInternal(): Promise<{
    blockHeight: number;
    timestamp: number;
  }> {
    try {
      const statusResponse = await this.axiosInstance.post(
        this.endpoint.href + "graphql",
        {
          query: `
          query {
            blocks(first: 1) {
              edges {
                node {
                  id
                  height
                  timestamp
                }
              }
            }
          }
          
          `,
        }
      );
      const edge = statusResponse?.data?.data?.blocks?.edges[0];
      const blockHeight = edge?.node?.height;
      const timestamp = edge?.node?.timestamp;
      if (blockHeight && timestamp) {
        logger.debug("Successfully fetched current block info", {
          blockHeight,
        });
        return {
          blockHeight,
          timestamp,
        };
      } else {
        throw Error("Could not fetch block info");
      }
    } catch (error) {
      logger.error("Error getting current block info", error);
      throw error;
    }
  }

  public async getBalanceForWallet(
    wallet: PublicArweaveAddress
  ): Promise<Winston> {
    const res = await this.retryStrategy.sendRequest(() =>
      this.axiosInstance.get<string>(`${this.endpoint}wallet/${wallet}/balance`)
    );
    return new Winston(res.data);
  }
}
