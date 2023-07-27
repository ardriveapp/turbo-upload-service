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
import Transaction from "arweave/node/lib/transaction";
import axios, { AxiosInstance, AxiosResponse } from "axios";

import { gatewayUrl } from "../constants";
import logger from "../logger";
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

export abstract class Gateway {
  public abstract getWinstonPriceForByteCount(
    byteCount: ByteCount,
    target?: PublicArweaveAddress
  ): Promise<Winston>;

  public abstract postToEndpoint<T = unknown>(
    endpoint: string,
    data?: unknown
  ): Promise<AxiosResponse<T>>;

  public abstract postBundleTx(bundleTx: Transaction): Promise<Transaction>;

  public abstract getTransactionStatus(
    transactionId: TransactionId
  ): Promise<TransactionStatus>;

  public abstract getBlockHash(): Promise<string>;
  public abstract isTransactionQueryableOnGQL(
    transactionId: TransactionId
  ): Promise<boolean>;
  public abstract getBlockHeightForTxAnchor(txAnchor: string): Promise<number>;
  public abstract getCurrentBlockHeight(): Promise<number>;
  public abstract getBalanceForWallet(
    wallet: PublicArweaveAddress
  ): Promise<Winston>;
}

export class ArweaveGateway extends Gateway {
  private endpoint: URL;
  private retryStrategy: RetryStrategy<AxiosResponse>;
  private axiosInstance: AxiosInstance;

  constructor({
    endpoint = gatewayUrl,
    retryStrategy = new ExponentialBackoffRetryStrategy({}),
    axiosInstance = axios.create({ validateStatus: undefined }),
  }: GatewayAPIConstParams = {}) {
    super();
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
    logger.info("Getting transaction status...", { transactionId });
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
        logger.info("Transaction not found...", { transactionId });
        return { status: "not found" };
      }
      if (statusResponse.data === "Pending") {
        logger.info("Transaction is pending...", { transactionId });
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
    logger.info("Posting bundle tx id.", {
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

  public async isTransactionQueryableOnGQL(
    transactionId: TransactionId
  ): Promise<boolean> {
    try {
      logger.info("Checking if data item can be found on GQL...", {
        transactionId,
      });

      const statusResponse = await this.axiosInstance.post(
        this.endpoint.href + "graphql",
        {
          query: `
          query {
            transactions(ids: ["${transactionId}"]) {
              edges {
                node {
                  id
                }
              }
            }
          }
          
          `,
        }
      );
      if (statusResponse?.data?.data?.transactions?.edges?.length > 0) {
        return true;
      } else return false;
    } catch (error) {
      logger.error("Error querying transaction on GQL", error);
      return false;
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

        logger.info("Successfully fetched block height for tx_anchor", {
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

  public async getCurrentBlockHeight(): Promise<number> {
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
                }
              }
            }
          }
          
          `,
        }
      );
      const edge = statusResponse?.data?.data?.blocks?.edges[0];
      const blockHeight: number = edge?.node?.height;
      if (blockHeight) {
        logger.info("Successfully fetched current block height", {
          blockHeight,
        });
        return blockHeight;
      } else {
        throw Error("Could not fetch block height");
      }
    } catch (error) {
      logger.error("Error getting current block height", error);
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
