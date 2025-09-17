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
import { SSMClient } from "@aws-sdk/client-ssm";

const endpoint = process.env.AWS_ENDPOINT;
const awsRegion = process.env.AWS_REGION ?? "us-east-1";
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

export const ssmClient = new SSMClient({
  region: awsRegion,
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
});
