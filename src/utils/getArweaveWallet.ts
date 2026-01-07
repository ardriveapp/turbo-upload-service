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
import {
  PromiseCache,
  ReadThroughPromiseCache,
} from "@ardrive/ardrive-promise-cache";
import {
  GetSecretValueCommand,
  SecretsManagerClient,
} from "@aws-sdk/client-secrets-manager";
import { GetParameterCommand } from "@aws-sdk/client-ssm";
import { ArweaveSigner } from "@dha-team/arbundles";
import { Base64UrlString } from "arweave/node/lib/utils";
import winston from "winston";

import { ssmClient as svcsSystemsMgrClient } from "../arch/ssmClient";
import { msPerMinute, turboLocalEvmSigner, turboLocalJwk } from "../constants";
import logger from "../logger";
import { JWKInterface } from "../types/jwkTypes";

const sixtyMinutes = msPerMinute * 60;

const opticalWalletCache = new PromiseCache<string, JWKInterface>({
  cacheCapacity: 1,
  cacheTTLMillis: sixtyMinutes,
});

const opticalPubKeyCache = new PromiseCache<string, string>({
  cacheCapacity: 1,
  cacheTTLMillis: sixtyMinutes,
});

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

const endpoint = process.env.AWS_ENDPOINT;
const secretsMgrClient = new SecretsManagerClient({
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

function getSecretValue(
  secretName: string,
  logger?: winston.Logger
): Promise<string> {
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
    })
    .catch((error) => {
      logger?.error(
        `Failed to retrieve '${secretName}' from Secrets Manager.`,
        error
      );
      throw error;
    });
}

const arweaveWalletSecretName = "arweave-wallet";

const signingWalletCache = new ReadThroughPromiseCache<string, JWKInterface>({
  cacheParams: {
    cacheCapacity: 1,
    cacheTTLMillis: sixtyMinutes,
  },
  readThroughFunction: async () => {
    return getSecretValue(arweaveWalletSecretName).then((walletString) =>
      JSON.parse(walletString)
    );
  },
});
export async function getArweaveWallet(): Promise<JWKInterface> {
  if (turboLocalJwk) {
    logger.debug("Using local JWk for Turbo wallet");
    return turboLocalJwk;
  }
  // Return any inflight, potentially-resolved promise for the wallet OR
  // start, cache, and return a new one
  return signingWalletCache.get(arweaveWalletSecretName);
}

const evmDataItemSigningWalletSecretName = "turbo-evm-data-item-signing-key";

const dataItemSigningWalletCache = new ReadThroughPromiseCache<string, string>({
  cacheParams: {
    cacheCapacity: 1,
    cacheTTLMillis: sixtyMinutes,
  },
  readThroughFunction: async () =>
    getSecretValue(evmDataItemSigningWalletSecretName),
});
export async function getEvmDataItemSigningPrivateKey(): Promise<string> {
  if (turboLocalEvmSigner) {
    logger.debug("Using local wallet for Turbo EVM data item signing wallet");
    return turboLocalEvmSigner;
  }
  // Return any inflight, potentially-resolved promise for the wallet OR
  // start, cache, and return a new one
  return dataItemSigningWalletCache.get(evmDataItemSigningWalletSecretName);
}

export async function getOpticalWallet(): Promise<JWKInterface> {
  if (turboLocalJwk) {
    logger.debug("Using local JWk for Turbo optical wallet");
    return turboLocalJwk;
  }
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
  if (turboLocalJwk) {
    logger.debug("Using local JWk for Turbo optical pub key");
    return new ArweaveSigner(turboLocalJwk).publicKey.toString("base64url");
  }

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
          logger.debug(
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

const ssmParamCache = new ReadThroughPromiseCache<string, string>({
  cacheParams: {
    cacheCapacity: 100,
    cacheTTLMillis: sixtyMinutes,
  },
  readThroughFunction: async (parameterName) => {
    const command = new GetParameterCommand({
      Name: parameterName,
    });
    const parameter = await svcsSystemsMgrClient.send(command);
    return parameter.Parameter?.Value ?? "";
  },
});

export async function getSSMParameter(parameterName: string): Promise<string> {
  return ssmParamCache.get(parameterName);
}
