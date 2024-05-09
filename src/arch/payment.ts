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
import { AxiosInstance } from "axios";
import { sign } from "jsonwebtoken";
import winston from "winston";

import {
  allowListedSignatureTypes,
  signatureTypeInfo,
} from "../bundles/verifyDataItem";
import {
  allowArFSData,
  allowListPublicAddresses,
  freeUploadLimitBytes,
  testPrivateRouteSecret,
} from "../constants";
import defaultLogger from "../logger";
import { MetricRegistry } from "../metricRegistry";
import {
  ByteCount,
  NativeAddress,
  TransactionId,
  W,
  Winston,
} from "../types/types";
import { createAxiosInstance } from "./axiosClient";

// TODO: Payment service response API
export interface ReserveBalanceResponse {
  walletExists: boolean;
  isReserved: boolean;
  costOfDataItem: Winston;
}

export interface CheckBalanceResponse {
  userHasSufficientBalance: boolean;
  bytesCostInWinc: Winston;
  userBalanceInWinc?: Winston;
}

interface PaymentServiceCheckBalanceResponse {
  userHasSufficientBalance: boolean;
  bytesCostInWinc: Winston;
  userBalanceInWinc: Winston;
  adjustments: Record<string, unknown>[];
}

interface CheckBalanceParams {
  size: ByteCount;
  nativeAddress: NativeAddress;
  signatureType: number;
}

interface ReserveBalanceParams extends CheckBalanceParams {
  dataItemId: TransactionId;
}

export interface RefundBalanceResponse {
  walletExists: boolean;
}

interface RefundBalanceParams {
  winston: Winston;
  nativeAddress: NativeAddress;
  dataItemId: TransactionId;
  signatureType: number;
}

export interface PaymentService {
  checkBalanceForData(
    params: CheckBalanceParams
  ): Promise<CheckBalanceResponse>;
  reserveBalanceForData(
    params: ReserveBalanceParams
  ): Promise<ReserveBalanceResponse>;
  refundBalanceForData(params: RefundBalanceParams): Promise<void>;
  getFiatToARConversionRate(currency: "usd"): Promise<number>; // TODO: create type for currency
  paymentServiceURL: string;
}

const allowedReserveBalanceResponse: ReserveBalanceResponse = {
  walletExists: true,
  costOfDataItem: W(0),
  isReserved: true,
};

const secret = process.env.PRIVATE_ROUTE_SECRET ?? testPrivateRouteSecret;
export class TurboPaymentService implements PaymentService {
  constructor(
    private readonly shouldAllowArFSData: boolean = allowArFSData,
    // TODO: create a client config with base url pointing at the base url of the payment service
    private readonly axios: AxiosInstance = createAxiosInstance({}),
    private readonly logger: winston.Logger = defaultLogger,
    readonly paymentServiceURL: string = process.env.PAYMENT_SERVICE_BASE_URL ??
      "payment.ardrive.dev",
    paymentServiceProtocol: string = process.env.PAYMENT_SERVICE_PROTOCOL ??
      "https"
  ) {
    this.logger = logger.child({
      class: this.constructor.name,
      paymentServiceURL,
      shouldAllowArFSData,
    });
    this.axios = axios;
    this.paymentServiceURL = `${paymentServiceProtocol}://${paymentServiceURL}`;
  }

  public async checkBalanceForData({
    size,
    nativeAddress,
    signatureType,
  }: CheckBalanceParams): Promise<CheckBalanceResponse> {
    const logger = this.logger.child({ nativeAddress, size });

    logger.debug("Checking balance for wallet.");

    if (
      await this.checkBalanceForDataInternal({
        size,
        nativeAddress,
        signatureType,
      })
    ) {
      logger.debug(
        "Data was allowed via internal upload service business logic. Not calling payment service to check balance..."
      );
      return {
        userHasSufficientBalance: true,
        bytesCostInWinc: W(0),
      };
    }

    logger.debug("Calling payment service to check balance...");

    const token = sign({}, secret, {
      expiresIn: "1h",
    });
    const url = `${this.paymentServiceURL}/v1/check-balance/${signatureTypeInfo[signatureType].name}/${nativeAddress}?byteCount=${size}`;

    const { status, statusText, data } = await this.axios.get<
      PaymentServiceCheckBalanceResponse | string
    >(url, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
      validateStatus: (status) => {
        if (status >= 500) {
          throw new Error(`Payment service unavailable. Status: ${status}`);
        }
        return true;
      },
    });

    logger.debug("Payment service response.", {
      status,
      statusText,
      data,
    });

    if (typeof data === "string") {
      throw new Error(
        `Payment service returned a string instead of a json object. Body: ${data} | Status: ${status} | StatusText: ${statusText}`
      );
    }

    return data;
  }

  private async checkBalanceForDataInternal({
    size,
    nativeAddress,
    signatureType,
  }: CheckBalanceParams): Promise<boolean> {
    const logger = this.logger.child({ nativeAddress, size });

    logger.debug("Checking balance for wallet.");

    if (allowListPublicAddresses.includes(nativeAddress)) {
      logger.debug(
        "The owner's address is on the arweave public address allow list. Allowing data item to be bundled by the service..."
      );
      return true;
    }

    if (this.shouldAllowArFSData && size <= freeUploadLimitBytes) {
      // TODO: Add limitations PE-2603
      logger.debug(
        "This data item is under the free ArFS data limit. Allowing data item to be bundled by the service..."
      );

      return true;
    }

    if (allowListedSignatureTypes.has(signatureType)) {
      logger.info(
        "Allow listed signature detected. Allowing data item to be bundled by the service...",
        { signatureType }
      );
      return true;
    }

    return false;
  }

  public async reserveBalanceForData({
    size,
    nativeAddress,
    dataItemId,
    signatureType,
  }: ReserveBalanceParams): Promise<ReserveBalanceResponse> {
    const logger = this.logger.child({ nativeAddress, size });

    logger.debug("Reserving balance for wallet.");

    if (
      await this.checkBalanceForDataInternal({
        size,
        nativeAddress,
        signatureType,
      })
    ) {
      logger.debug(
        "Data was allowed via internal upload service business logic. Not calling payment service to reserve balance..."
      );
      return allowedReserveBalanceResponse;
    }

    logger.debug("Calling payment service to reserve balance...");

    const token = sign({}, secret, {
      expiresIn: "1h",
    });
    const url = `${this.paymentServiceURL}/v1/reserve-balance/${signatureTypeInfo[signatureType].name}/${nativeAddress}?byteCount=${size}&dataItemId=${dataItemId}`;

    const { status, statusText, data } = await this.axios.get(url, {
      headers: {
        Authorization: `Bearer ${token}`,
      },
      validateStatus: (status) => {
        if (status >= 500) {
          throw new Error(`Payment service unavailable. Status: ${status}`);
        }
        return true;
      },
    });

    logger.debug("Payment service response.", {
      status,
      statusText,
      data,
    });

    const walletExists = +status !== 404;
    const costOfDataItem = +status === 200 ? W(+data) : W(0);
    const isReserved = +status === 200;

    return {
      walletExists,
      costOfDataItem,
      isReserved,
    };
  }

  public async refundBalanceForData(
    params: RefundBalanceParams
  ): Promise<void> {
    const logger = this.logger.child({ ...params });
    const { nativeAddress, winston, dataItemId, signatureType } = params;

    logger.debug("Refunding balance for wallet.", {
      nativeAddress,
      winston,
    });

    if (allowListPublicAddresses.includes(nativeAddress)) {
      logger.info(
        "The owner's address is on the arweave public address allow list. Not calling payment service to refund balance..."
      );
      return;
    }

    const token = sign({}, secret, {
      expiresIn: "1h",
    });

    try {
      await this.axios.get(
        `${this.paymentServiceURL}/v1/refund-balance/${signatureTypeInfo[signatureType].name}/${nativeAddress}?winstonCredits=${winston}&dataItemId=${dataItemId}`,
        {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      );
      logger.debug("Successfully refunded balance for wallet.");
    } catch (error) {
      // TODO: add prometheus metric for when this fails - we may need to manually intervene to distribute the refund
      MetricRegistry.refundBalanceFail.inc();
      const message = error instanceof Error ? error.message : "Unknown error";
      logger.error("Unable to issue refund!", {
        error: message,
      });
    }
  }

  public async getFiatToARConversionRate(
    currency: "usd" = "usd"
  ): Promise<number> {
    const { data: fiatToArRate } = await this.axios.get(
      `${this.paymentServiceURL}/v1/rates/${currency}`
    );
    return +fiatToArRate.rate;
  }
}
