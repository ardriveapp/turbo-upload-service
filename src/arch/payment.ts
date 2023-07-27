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
import { AxiosInstance } from "axios";
import { sign } from "jsonwebtoken";
import winston from "winston";

import {
  allowArFSData,
  allowListPublicAddresses,
  freeArfsDataAllowLimit,
  testPrivateRouteSecret,
} from "../constants";
import defaultLogger from "../logger";
import { MetricRegistry } from "../metricRegistry";
import {
  ByteCount,
  PublicArweaveAddress,
  TransactionId,
  W,
  Winston,
} from "../types/types";
import { createAxiosInstance } from "./axiosClient";

export interface ReserveBalanceResponse {
  walletExists: boolean;
  isReserved: boolean;
  costOfDataItem: Winston;
}

interface ReserveBalanceParams {
  size: ByteCount;
  ownerPublicAddress: PublicArweaveAddress;
  dataItemId: TransactionId;
}

export interface RefundBalanceResponse {
  walletExists: boolean;
}

interface RefundBalanceParams {
  winston: Winston;
  ownerPublicAddress: PublicArweaveAddress;
  dataItemId: TransactionId;
}

export interface PaymentService {
  reserveBalanceForData(
    params: ReserveBalanceParams
  ): Promise<ReserveBalanceResponse>;
  refundBalanceForData(params: RefundBalanceParams): Promise<void>;
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

    private readonly axios: AxiosInstance = createAxiosInstance({}),
    private readonly logger: winston.Logger = defaultLogger,
    private readonly paymentServiceURL: string = process.env
      .PAYMENT_SERVICE_BASE_URL ?? "payment.ardrive.dev"
  ) {
    this.logger = logger.child({
      class: this.constructor.name,
      paymentServiceURL,
      shouldAllowArFSData,
    });
    this.axios = axios;
    this.paymentServiceURL = `https://${paymentServiceURL}`;
  }

  public async reserveBalanceForData({
    size,
    ownerPublicAddress,
    dataItemId,
  }: ReserveBalanceParams): Promise<ReserveBalanceResponse> {
    const logger = this.logger.child({ ownerPublicAddress, size });

    logger.info("Reserving balance for wallet.");

    if (allowListPublicAddresses.includes(ownerPublicAddress)) {
      logger.info(
        "The owner's address is on the arweave public address allow list. Allowing data item to be bundled by the service..."
      );
      return allowedReserveBalanceResponse;
    }

    if (this.shouldAllowArFSData && size <= freeArfsDataAllowLimit) {
      logger.info(
        "This data item is under the free ArFS data limit. Allowing data item to be bundled by the service..."
      );

      return allowedReserveBalanceResponse;
    }

    logger.info("Calling payment service to reserve balance...");

    const token = sign({}, secret, {
      expiresIn: "1h",
    });
    const url = `${this.paymentServiceURL}/v1/reserve-balance/${ownerPublicAddress}?byteCount=${size}&dataItemId=${dataItemId}`;

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

    logger.info("Payment service response.", {
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
    const { ownerPublicAddress, winston, dataItemId } = params;

    logger.info("Refunding balance for wallet.", {
      ownerPublicAddress,
      winston,
    });

    if (allowListPublicAddresses.includes(ownerPublicAddress)) {
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
        `${this.paymentServiceURL}/v1/refund-balance/${ownerPublicAddress}?winstonCredits=${winston}&dataItemId=${dataItemId}`,
        {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      );
      logger.info("Successfully refunded balance for wallet.");
    } catch (error) {
      MetricRegistry.refundBalanceFail.inc();
      const message = error instanceof Error ? error.message : "Unknown error";
      logger.error("Unable to issue refund!", {
        error: message,
      });
    }
  }
}
