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
import { ConsumerOptions } from "sqs-consumer";
import winston from "winston";

import { Architecture } from "../../../../src/arch/architecture";

export type QueueHandlerConfig = {
  queueUrl: string;
  handler: (
    planId: string,
    arch: Partial<Omit<Architecture, "logger">>,
    logger: winston.Logger
  ) => Promise<void>;
  logger: winston.Logger;
  consumerOptions?: Partial<ConsumerOptions>;
};

const awsCredentials =
  process.env.AWS_ACCESS_KEY_ID !== undefined &&
  process.env.AWS_SECRET_ACCESS_KEY !== undefined
    ? {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        ...(process.env.AWS_SESSION_TOKEN
          ? {
              sessionToken: process.env.AWS_SESSION_TOKEN,
            }
          : {}),
      }
    : undefined;

const endpoint = process.env.AWS_ENDPOINT;
export const defaultSQSOptions = {
  region: process.env.AWS_REGION ?? "us-east-1",
  maxAttempts: 3,
  ...(endpoint
    ? {
        endpoint,
      }
    : {}),
  ...(awsCredentials
    ? {
        credentials: awsCredentials,
      }
    : {}),
};

export const stubQueueHandler = async (
  _: string,
  __: Partial<Omit<Architecture, "logger">>,
  ___: winston.Logger
) => {
  return;
};
