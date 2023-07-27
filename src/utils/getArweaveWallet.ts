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
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from "@aws-sdk/client-secrets-manager";
import { GetParameterCommand, SSMClient } from "@aws-sdk/client-ssm";
import { ArweaveSigner } from "arbundles";
import { Base64UrlString } from "arweave/node/lib/utils";

import { msPerMinute } from "../constants";
import logger from "../logger";
import { JWKInterface } from "../types/jwkTypes";
import { PromiseCache } from "./promiseCache";

const opticalWalletCache = new PromiseCache<string, JWKInterface>({
  cacheCapacity: 1,
  cacheTTL: msPerMinute * 60,
});

const opticalPubKeyCache = new PromiseCache<string, string>({
  cacheCapacity: 1,
  cacheTTL: msPerMinute * 60,
});

// eslint-disable-next-line @typescript-eslint/no-non-null-assertion
const awsRegion = process.env.AWS_REGION!;

const secretsMgrClient = new SecretsManagerClient({
  region: awsRegion,
});

const svcsSystemsMgrClient = new SSMClient({ region: awsRegion });

/* eslint-disable @typescript-eslint/no-explicit-any */
function getSecretValue(secretName: string): Promise<string> {
  return secretsMgrClient
    .send(
      new GetSecretValueCommand({
        SecretId: secretName,
      })
    )
    .then((result) => {
      if (result.SecretString) {
        return result.SecretString;
      }

      throw new Error(
        `Unexpectedly got undefined string for secret ${secretName}`
      );
    });
}

export async function getArweaveWallet(): Promise<JWKInterface> {
  return JSON.parse(await getSecretValue("arweave-wallet"));
}

export async function getOpticalWallet(): Promise<JWKInterface> {
  const secretName = `turbo-optical-key-${process.env.NODE_ENV}`;

  // Return any inflight, potentially-resolved promise for the wallet OR
  // start, cache, and return a new one
  return (
    opticalWalletCache.get(secretName) ??
    opticalWalletCache.put(
      secretName,
      await getSecretValue(secretName).then((walletString) =>
        JSON.parse(walletString)
      )
    )
  );
}

export async function getOpticalPubKey(): Promise<Base64UrlString> {
  const ssmParameterName = `turbo-optical-public-key-${process.env.NODE_ENV}`;

  // Return any inflight, potentially-resolved promise for the pubkey OR
  // start, cache, and return a new one
  return (
    opticalPubKeyCache.get(ssmParameterName) ??
    opticalPubKeyCache.put(
      ssmParameterName,
      (async () => {
        // Try SSM, otherwise fallback to the SecretsMgr
        try {
          const command = new GetParameterCommand({
            Name: ssmParameterName,
          });
          const pubKey = (await svcsSystemsMgrClient.send(command)).Parameter
            ?.Value;
          if (pubKey) {
            return pubKey;
          }
          throw new Error(`PubKey unexpectedly undefined or zero length!`);
        } catch (error) {
          logger.error(
            "Couldn't retrieve pubKey from SSM! Falling back to SecretsMgr",
            error
          );
          const jwk = await getOpticalWallet();
          return new ArweaveSigner(jwk).publicKey.toString("base64url");
        }
      })()
    )
  );
}
