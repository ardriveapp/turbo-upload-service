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
import { FATAL_CHUNK_UPLOAD_ERRORS, INITIAL_ERROR_DELAY } from "../constants";
import logger from "../logger";

const rateLimitStatus = 429;
const rateLimitTimeout = 60_000;

interface RetryStrategyParams {
  maxRetriesPerRequest?: number;
  initialErrorDelayMS?: number;
  fatalErrors?: string[];
  validStatusCodes?: number[];
}

export interface ArweaveNetworkResponse {
  status: number;
  statusText: string;
}

export abstract class RetryStrategy<T extends ArweaveNetworkResponse> {
  public abstract sendRequest(request: () => Promise<T>): Promise<T>;
}

export class NoRetryStrategy<
  T extends ArweaveNetworkResponse
> extends RetryStrategy<T> {
  public async sendRequest(request: () => Promise<T>): Promise<T> {
    const response = await this.tryRequest(request);
    if (response) {
      return response;
    } else {
      throw new Error("Request failed");
    }
  }

  private async tryRequest(request: () => Promise<T>): Promise<T | undefined> {
    const resp = await request();

    return resp;
  }
}

export class ExponentialBackoffRetryStrategy<
  T extends ArweaveNetworkResponse
> extends RetryStrategy<T> {
  private maxRetriesPerRequest: number;
  private initialErrorDelayMS: number;
  private fatalErrors: string[];
  private validStatusCodes: number[];

  constructor({
    maxRetriesPerRequest = 5,
    initialErrorDelayMS = INITIAL_ERROR_DELAY,
    fatalErrors = [
      ...FATAL_CHUNK_UPLOAD_ERRORS,
      "Nodes rejected the TX headers",
    ],
    validStatusCodes = [200],
  }: RetryStrategyParams) {
    super();
    this.maxRetriesPerRequest = maxRetriesPerRequest;
    this.initialErrorDelayMS = initialErrorDelayMS;
    this.fatalErrors = fatalErrors;
    this.validStatusCodes = validStatusCodes;
  }

  private lastError = "unknown error";
  private lastRespStatus = 0;

  /**
   * Retries the given request until the response returns a successful
   * status code or the maxRetries setting has been exceeded
   *
   * @throws when a fatal error has been returned by request
   * @throws when max retries have been exhausted
   */
  public async sendRequest(request: () => Promise<T>): Promise<T> {
    let retryNumber = 0;

    while (retryNumber <= this.maxRetriesPerRequest) {
      const response = await this.tryRequest(request);

      if (response) {
        if (retryNumber > 0) {
          logger.warn(`Request has been successfully retried!`);
        }
        return response;
      }
      this.throwIfFatalError();

      if (this.lastRespStatus === rateLimitStatus) {
        // When rate limited by the gateway, we will wait without incrementing retry count
        await this.rateLimitThrottle();
        continue;
      }

      logger.warn(
        `Request to gateway has failed: (Status: ${this.lastRespStatus}) ${this.lastError}`
      );

      const nextRetry = retryNumber + 1;

      if (nextRetry <= this.maxRetriesPerRequest) {
        await this.exponentialBackOffAfterFailedRequest(retryNumber);

        logger.warn(`Retrying request, retry attempt ${nextRetry}...`);
      }

      retryNumber = nextRetry;
    }

    // Didn't succeed within number of allocated retries
    throw new Error(
      `Request to gateway has failed: (Status: ${this.lastRespStatus}) ${this.lastError}`
    );
  }

  private async tryRequest(request: () => Promise<T>): Promise<T | undefined> {
    try {
      const resp = await request();
      this.lastRespStatus = resp.status;

      if (this.isRequestSuccessful()) {
        return resp;
      }

      this.lastError = resp.statusText ?? JSON.stringify(resp);
    } catch (err) {
      this.lastError = err instanceof Error ? err.message : "unknown error";
    }

    return undefined;
  }

  private isRequestSuccessful(): boolean {
    return this.validStatusCodes.includes(this.lastRespStatus);
  }

  private throwIfFatalError() {
    if (this.fatalErrors.includes(this.lastError)) {
      throw new Error(
        `Fatal error encountered: (Status: ${this.lastRespStatus}) ${this.lastError}`
      );
    }
  }

  private async exponentialBackOffAfterFailedRequest(
    retryNumber: number
  ): Promise<void> {
    const delay = Math.pow(2, retryNumber) * this.initialErrorDelayMS;
    logger.warn(
      `Waiting for ${(delay / 1000).toFixed(1)} seconds before next request...`
    );
    await new Promise((res) => setTimeout(res, delay));
  }

  private async rateLimitThrottle() {
    logger.warn(
      `Gateway has returned a ${
        this.lastRespStatus
      } status which means your IP is being rate limited. Pausing for ${(
        rateLimitTimeout / 1000
      ).toFixed(1)} seconds before trying next request...`
    );
    await new Promise((res) => setTimeout(res, rateLimitTimeout));
  }
}
