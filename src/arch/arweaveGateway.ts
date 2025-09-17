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
import { ReadThroughPromiseCache } from "@ardrive/ardrive-promise-cache";
import Transaction from "arweave/node/lib/transaction";
import axios, { AxiosInstance, AxiosResponse } from "axios";

import { gatewayUrl, msPerMinute } from "../constants";
import logger from "../logger";
import { MetricRegistry } from "../metricRegistry";
import {
  ConfirmedTransactionStatus,
  TransactionStatus,
  isConfirmedTransactionStatus,
} from "../types/txStatus";
import { ByteCount, PublicArweaveAddress, TransactionId } from "../types/types";
import { W, Winston } from "../types/winston";
import { getHttpAgents } from "./axiosClient";
import {
  ExponentialBackoffRetryStrategy,
  RetryStrategy,
} from "./retryStrategy";

interface GatewayAPIConstParams {
  endpoint?: URL;
  retryStrategy?: RetryStrategy<AxiosResponse>;
  axiosInstance?: AxiosInstance;
}

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
  postBundleTxToAdminQueue(bundleTxId: TransactionId): Promise<void>;
}

export const currentBlockInfoCache = new ReadThroughPromiseCache<
  string, // cache key is the gateway endpoint URL
  { blockHeight: number; timestamp: number },
  { axiosInstance: AxiosInstance; endpointHref: string }
>({
  cacheParams: {
    cacheCapacity: 1,
    cacheTTLMillis: msPerMinute,
  },
  readThroughFunction: async (_, { axiosInstance, endpointHref }) => {
    return getCurrentBlockInfoInternal({ axiosInstance, endpointHref });
  },
  metricsConfig: {
    cacheName: "curr_block_cache",
    registry: MetricRegistry.getInstance().getRegistry(),
    labels: {
      env: process.env.NODE_ENV ?? "local",
    },
  },
});

export class ArweaveGateway implements Gateway {
  private endpoint: URL;
  private retryStrategy: RetryStrategy<AxiosResponse>;
  private axiosInstance: AxiosInstance;

  constructor({
    endpoint = gatewayUrl,
    retryStrategy = new ExponentialBackoffRetryStrategy({}),
    axiosInstance = axios.create({
      ...getHttpAgents(),
    }), // defaults to throwing errors for status codes >400
  }: GatewayAPIConstParams = {}) {
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
          `${this.endpoint.href}tx/${transactionId}/status`,
          { validateStatus: () => true }
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

  public async getBlockHeightForTxAnchor(txAnchor: string): Promise<number> {
    try {
      const statusResponse = await this.retryStrategy.sendRequest(() =>
        this.axiosInstance.post(this.endpoint.href + "graphql", {
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
        })
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
      logger.error("Error getting block height for tx anchor", {
        txAnchor,
        error: error instanceof Error ? error.message : "Unknown error",
      });
      throw error;
    }
  }

  public async getCurrentBlockHeight(): Promise<number> {
    return (
      await currentBlockInfoCache.get(this.endpoint.href, {
        axiosInstance: this.axiosInstance,
        endpointHref: this.endpoint.href,
      })
    ).blockHeight;
  }

  public async getCurrentBlockTimestamp(): Promise<number> {
    return (
      await currentBlockInfoCache.get(this.endpoint.href, {
        axiosInstance: this.axiosInstance,
        endpointHref: this.endpoint.href,
      })
    ).timestamp;
  }

  public async getBalanceForWallet(
    wallet: PublicArweaveAddress
  ): Promise<Winston> {
    const res = await this.retryStrategy.sendRequest(() =>
      this.axiosInstance.get<string>(`${this.endpoint}wallet/${wallet}/balance`)
    );
    return new Winston(res.data);
  }

  /** Optionally posts a prepared bundle to the ar.io gateway's priority bundle queue if an admin key exists */
  public async postBundleTxToAdminQueue(
    bundleTxId: TransactionId
  ): Promise<void> {
    if (process.env.AR_IO_ADMIN_KEY !== undefined) {
      logger.debug("Posting bundle to admin queue...", { bundleTxId });
      try {
        await this.retryStrategy.sendRequest(() =>
          this.axiosInstance.post(
            `${this.endpoint.href}ar-io/admin/queue-bundle`,
            {
              id: bundleTxId,
            },
            {
              headers: {
                Authorization: `Bearer ${process.env.AR_IO_ADMIN_KEY}`,
              },
            }
          )
        );
      } catch (error) {
        logger.error("Error posting bundle to admin queue", {
          bundleTxId,
          error: error instanceof Error ? error.message : "Unknown error",
        });
      }
    }
  }
}

async function getCurrentBlockInfoInternal({
  axiosInstance,
  endpointHref,
}: {
  axiosInstance: AxiosInstance;
  endpointHref: string;
}): Promise<{
  blockHeight: number;
  timestamp: number;
}> {
  try {
    const result = await Promise.any([
      getCurrentBlockInfoViaGraphQL({ axiosInstance, endpointHref }),
      getCurrentBlockInfoViaNodeProxy({ axiosInstance, endpointHref }),
    ]);
    return result;
  } catch (_) {
    const errMsg = "Error getting current block info from all sources!";
    logger.error(errMsg);
    throw new Error(errMsg);
  }
}

async function getCurrentBlockInfoViaGraphQL({
  axiosInstance,
  endpointHref,
}: {
  axiosInstance: AxiosInstance;
  endpointHref: string;
}): Promise<{
  blockHeight: number;
  timestamp: number;
}> {
  const retryStrategy = new ExponentialBackoffRetryStrategy<AxiosResponse>({
    validStatusCodes: [200, 202], // only success on these codes
  });
  let blockHeight, timestamp;
  try {
    const statusResponse = await retryStrategy
      .sendRequest(() =>
        axiosInstance.post(endpointHref + "graphql", {
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
        })
      )
      // catch errors thrown by retry logic - which would be anything not a 200 or 202 - swallow them so we can fallback below
      .catch((error) => {
        logger.debug(error);
        return undefined;
      });

    // success from gql - use the response to get block info
    if (statusResponse) {
      const edge = statusResponse.data?.data?.blocks?.edges[0];
      blockHeight = edge?.node?.height;
      timestamp = edge?.node?.timestamp;
      logger.debug("Successfully fetched current block info from GQL", {
        blockHeight,
        timestamp,
      });
      return {
        blockHeight,
        timestamp,
      };
    }
  } catch (error) {
    logger.error("Error getting current block info via gql", {
      error: error instanceof Error ? error.message : "Unknown error",
    });
    throw error;
  }

  throw Error("Failed to fetch block info via gql");
}

async function getCurrentBlockInfoViaNodeProxy({
  axiosInstance,
  endpointHref,
}: {
  axiosInstance: AxiosInstance;
  endpointHref: string;
}): Promise<{
  blockHeight: number;
  timestamp: number;
}> {
  const retryStrategy = new ExponentialBackoffRetryStrategy<AxiosResponse>({
    validStatusCodes: [200, 202], // only success on these codes
  });

  // try and fetch from /block/current - if we don't get a 200/202 after 5 retries, ExponentialBackoffRetry will throw an error - do not catch it
  const response = await retryStrategy.sendRequest(() =>
    axiosInstance.get(endpointHref + "block/current")
  );

  const blockHeight = response?.data.height;
  const timestamp = response?.data.timestamp;

  if (!blockHeight || !timestamp) {
    throw Error("Failed to fetch block info via node proxy");
  }

  logger.debug(
    "Successfully fetched block height and timestamp via node proxy",
    {
      blockHeight,
      timestamp,
    }
  );

  return {
    blockHeight,
    timestamp,
  };
}
