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
import axios, { AxiosRequestConfig } from "axios";
import axiosRetry from "axios-retry";
import { Agent as HttpAgent } from "http";
import { Agent as HttpsAgent } from "https";

export interface CreateAxiosInstanceParams {
  config?: AxiosRequestConfig;
  retries?: number;
  retryDelay?: (retryNumber: number) => number;
}

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

export const createAxiosInstance = ({
  config = {},
  retries = 8,
  retryDelay = axiosRetry.exponentialDelay,
}: CreateAxiosInstanceParams) => {
  const axiosInstance = axios.create({
    ...getHttpAgents(),
    ...config,
  });

  if (retries > 0) {
    axiosRetry(axiosInstance, {
      retries,
      retryDelay,
    });
  }
  return axiosInstance;
};
