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

/* eslint-disable @typescript-eslint/no-explicit-any */
import axios, {
  AxiosInstance,
  AxiosRequestConfig,
  AxiosResponse,
  CanceledError,
} from "axios";
import { Agent as HttpAgent } from "http";
import { Agent as HttpsAgent } from "https";

import globalLogger from "../logger";
import { sleep } from "./common";

const httpAgent = new HttpAgent({
  timeout: 60000,
  keepAlive: true,
  keepAliveMsecs: 30_000,
});
const httpsAgent = new HttpsAgent({
  timeout: 60000,
  keepAlive: true,
  keepAliveMsecs: 30_000,
});
export const getHttpAgents = () => {
  return { httpAgent, httpsAgent };
};

export interface RetryHttpClientParams {
  config?: AxiosRequestConfig;
  maxTries?: number;
  retryDelayMs?: number;
}
const defaultConfig: AxiosRequestConfig = {
  validateStatus: () => true,
  ...getHttpAgents(),
};
export class RetryHttpClient {
  private axiosInstance: AxiosInstance;
  private maxTries: number;
  private retryDelayMs: number;
  private logger = globalLogger;

  constructor({
    config = defaultConfig,
    maxTries = 5,
    retryDelayMs = 1000,
  }: RetryHttpClientParams = {}) {
    this.axiosInstance = axios.create({
      ...defaultConfig,
      ...config,
    });
    this.maxTries = maxTries;
    this.retryDelayMs = retryDelayMs;
  }

  private async retryRequest<T>({
    requestFn,
    url,
    config,
  }: {
    requestFn: (
      url: string,
      config?: AxiosRequestConfig
    ) => Promise<AxiosResponse<T>>;
    url: string;
    config?: AxiosRequestConfig;
  }): Promise<AxiosResponse<T>> {
    let attempt = 0;
    let lastError: unknown;
    let lastResponse: AxiosResponse<T> | undefined = undefined;

    do {
      attempt++;
      this.logger.debug(`Attempting request...`, {
        attempt,
        maxTries: this.maxTries,
        url,
        config,
      });

      if (config?.signal?.aborted) {
        throw new CanceledError();
      }

      try {
        const response = await requestFn(url, config);
        lastResponse = response;
        if (response.status >= 500) {
          this.logger.warn(`Server error status returned`, {
            url,
            response: {
              status: response.status,
              statusText: response.statusText,
              data: response.data,
            },
          });
        } else if (response.status === 429) {
          this.logger.warn(`Rate limit exceeded`, {
            url,
            response: {
              status: response.status,
              statusText: response.statusText,
              data: response.data,
            },
          });
        } else {
          return response;
        }
      } catch (error) {
        lastError = error;
      }

      if (attempt >= this.maxTries) {
        break;
      }

      const delay = this.retryDelayMs * Math.pow(2, attempt - 1); // Exponential backoff
      this.logger.warn(
        `Request failed (attempt ${attempt} of ${this.maxTries}). Retrying in ${delay}ms...`,
        {
          url,
          config,
          error: lastError instanceof Error ? lastError.message : lastError,
          lastResponse:
            lastResponse !== undefined
              ? {
                  status: lastResponse.status,
                  statusText: lastResponse.statusText,
                  data: lastResponse.data,
                }
              : undefined,
        }
      );
      const promises = [sleep(delay)];

      if (
        config !== undefined &&
        config.signal !== undefined &&
        config.signal.addEventListener !== undefined
      ) {
        promises.push(
          new Promise<void>((_, reject) => {
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            config!.signal!.addEventListener!("abort", () => {
              reject(new CanceledError());
            });
          })
        );
      }
      await Promise.race(promises);
    } while (attempt <= this.maxTries);

    if (lastResponse) {
      return lastResponse;
    }
    this.logger.error(
      `Request failed after ${this.maxTries} attempts. No more retries.`,
      {
        error: lastError instanceof Error ? lastError.message : lastError,
        url,
        config,
      }
    );
    throw lastError;
  }

  public async get<T = any>(
    url: string,
    config?: AxiosRequestConfig
  ): Promise<AxiosResponse<T, any>> {
    return this.retryRequest({
      // TODO: Use native node fetch, remove axios dependency
      requestFn: (url, config) => this.axiosInstance.get<T>(url, config),
      url,
      config,
    });
  }

  public async post<T = any>(
    url: string,
    data?: unknown,
    config?: AxiosRequestConfig
  ): Promise<AxiosResponse<T, any>> {
    return this.retryRequest({
      // TODO: Use native node fetch, remove axios dependency
      requestFn: (url, config) => this.axiosInstance.post<T>(url, data, config),
      url,
      config,
    });
  }
}

export const createRetryHttpClient = (params: RetryHttpClientParams = {}) => {
  return new RetryHttpClient(params);
};
