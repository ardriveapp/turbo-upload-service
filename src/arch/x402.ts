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
import { createFacilitatorConfig } from "@coinbase/x402";
import { Logger } from "winston";
import { processPriceToAtomicAmount } from "x402/shared";
import {
  ERC20TokenAmount,
  FacilitatorConfig,
  PaymentPayload,
  PaymentRequirements,
  settleResponseHeader,
} from "x402/types";
import { useFacilitator } from "x402/verify";

import defaultLogger from "../logger";

/**
 * Result of payment verification
 */
export interface PaymentVerificationResult {
  /** Whether the payment is valid */
  isValid: boolean;
  /** Reason for invalid payment, if any */
  invalidReason?: string;
  /** Payer address */
  payerAddress?: string;
  /** USDC amount paid */
  usdcAmount?: string;
}

/**
 * Result of payment settlement
 */
export interface PaymentSettlementResult {
  /** Whether settlement succeeded */
  success: boolean;
  /** Error reason if settlement failed */
  errorReason?: string;
  /** Settlement response header value */
  responseHeader?: string;
  /** Transaction of the settlement, if available */
  transaction?: string;
  /** Network of the settlement, if available */
  network: string;
}

// Recommended facilitator URLs:
// https://x402.org/facilitator -> official Coinbase facilitator for testnet (base-sepolia)
// https://facilitator.x402.rs -> experimental facilitator, works for both base and base-sepolia without CDP API keys
// https://open.x402.host -> community-run facilitator for mainnet (base, ethereum-mainnet, polygon-mainnet)

// For mainnet: requires CDP API keys (see @coinbase/x402 package)
// https://api.cdp.coinbase.com/platform/v2/x402 -> official Coinbase facilitator for mainnet (requires CDP API keys)
// Prod must fulfill CDP_API_KEY_ID and CDP_API_SECRET_ID environment variables
export class X402Service {
  protected facilitator: ReturnType<typeof useFacilitator>;
  protected settlementTimeoutMs: number;
  protected logger: Logger;
  protected network: "base" | "base-sepolia";
  protected walletAddress: string;
  protected facilitatorUrl: string | undefined;

  constructor({
    facilitatorConfig = { url: "https://x402.org/facilitator" },
    network = process.env.NODE_ENV === "prod" ? "base" : "base-sepolia",
    walletAddress = process.env.X402_BASE_ADDRESS ||
      process.env.ETHEREUM_ADDRESS ||
      "0x9B13eb5096264B12532b8C648Eba4A662b4078ce", // default turbo dev wallet
    settlementTimeoutMs = 1000 * 60 * 3, // default 3 minutes
    logger = defaultLogger,
  }: {
    network?: "base" | "base-sepolia";
    walletAddress?: string;
    facilitatorConfig?: FacilitatorConfig;
    settlementTimeoutMs?: number;
    logger?: Logger;
  } = {}) {
    this.logger = logger.child({ service: "X402Service" });

    this.facilitator = useFacilitator(facilitatorConfig);
    this.walletAddress = walletAddress;
    this.settlementTimeoutMs = settlementTimeoutMs;
    this.network = network;
  }

  public extractPaymentPayload(
    paymentHeader: string | undefined
  ): PaymentPayload | undefined {
    this.logger.info("Extracting x402 payment payload from header", {
      paymentHeader,
    });
    if (!paymentHeader) {
      this.logger.warn("No payment header provided");
      return undefined;
    }
    try {
      const paymentPayload: PaymentPayload = JSON.parse(
        Buffer.from(paymentHeader, "base64").toString("utf-8")
      );
      return paymentPayload;
    } catch (error) {
      this.logger.error("Failed to extract x402 payment payload", {
        error: error instanceof Error ? error.message : error,
      });
      return undefined;
    }
  }

  /**
   * Calculate payment requirements based on content context
   */
  public calculateRequirements({
    usdcAmount,
    contentLength,
    contentType,
    resourceUrl,
  }: {
    usdcAmount: `$${string}`;
    contentLength: number;
    contentType: string;
    resourceUrl: string;
  }): PaymentRequirements {
    const atomicAssetPrice = processPriceToAtomicAmount(
      usdcAmount,
      this.network
    );

    if ("error" in atomicAssetPrice) {
      throw new Error(`Invalid price format: ${usdcAmount}`);
    }

    if (this.walletAddress.length === 0) {
      throw new Error("X402 wallet address is not configured");
    }

    // Give longer timeouts for larger uploads
    // Default 5 minutes, if larger than 10 MiB use 30 minutes, if larger than 100 MiB use 60 minutes
    const maxTimeoutSeconds =
      contentLength > 100 * 1024 * 1024
        ? 60 * 60
        : contentLength > 10 * 1024 * 1024
        ? 30 * 60
        : 5 * 60;

    return {
      scheme: "exact" as const,
      description: `Turbo cost to upload ${contentLength} bytes`,
      network: this.network,
      maxAmountRequired: atomicAssetPrice.maxAmountRequired,
      payTo: this.walletAddress,
      asset: atomicAssetPrice.asset.address,
      resource: resourceUrl,
      mimeType: contentType,
      maxTimeoutSeconds,
      extra: (atomicAssetPrice.asset as ERC20TokenAmount["asset"]).eip712,
    };
  }

  /**
   * Verify a payment payload against requirements
   */
  public async verifyPayment(
    paymentPayload: PaymentPayload,
    requirements: PaymentRequirements
  ): Promise<PaymentVerificationResult> {
    // Validate that the payment payload matches our requirements
    if (paymentPayload.scheme !== requirements.scheme) {
      return {
        isValid: false,
        invalidReason: "Payment scheme mismatch",
      };
    }

    if (paymentPayload.network !== requirements.network) {
      return {
        isValid: false,
        invalidReason: "Payment network mismatch",
      };
    }

    try {
      // Verify the payment using facilitator
      const verifyResponse = await this.facilitator.verify(
        paymentPayload,
        requirements
      );

      this.logger.info("Payment verification response", {
        verifyResponse,
      });

      // isValid indicates the payment is unique and has not been settled on chain yet
      if (!verifyResponse.isValid) {
        return {
          isValid: false,
          invalidReason: verifyResponse.invalidReason || "Invalid payment",
          payerAddress: verifyResponse.payer,
        };
      }

      return {
        isValid: true,
        payerAddress: verifyResponse.payer,
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore the type always includes payload.authorization.value when isValid is true
        usdcAmount: paymentPayload.payload.authorization.value,
      };
    } catch (error: any) {
      this.logger.error("Payment verification error", {
        error: error.message,
      });
      return {
        isValid: false,
        invalidReason: "Payment verification failed",
      };
    }
  }

  /**
   * Settle a verified payment
   */
  public async settlePayment(
    paymentPayload: PaymentPayload,
    requirements: PaymentRequirements
  ): Promise<PaymentSettlementResult> {
    try {
      this.logger.info("Settling payment", {
        paymentPayload,
        requirements,
        facilitatorUrl: this.facilitatorUrl,
      });

      // Wrap settlement with timeout to prevent indefinite hanging
      const settlementResult = await Promise.race([
        this.facilitator.settle(paymentPayload, requirements),
        new Promise<never>((_, reject) =>
          setTimeout(
            () => reject(new Error("Settlement timeout")),
            this.settlementTimeoutMs
          )
        ),
      ]);

      const settlementResultHeader = settleResponseHeader(settlementResult);

      this.logger.info("Payment settlement result", {
        settlementResult,
      });

      if (!settlementResult.success) {
        return {
          success: false,
          errorReason: settlementResult.errorReason,
          responseHeader: settlementResultHeader,
          transaction: settlementResult.transaction,
          network: settlementResult.network,
        };
      }

      return {
        success: true,
        responseHeader: settlementResultHeader,
        transaction: settlementResult.transaction,
        network: settlementResult.network,
      };
    } catch (error: any) {
      this.logger.error("Payment settlement error", {
        error: error.message,
        stack: error.stack,
      });
      return {
        success: false,
        errorReason: error.message ?? "settlement_error",
        network: this.network,
      };
    }
  }
}

const defaultFallbackUrls: `${string}://${string}`[] = [
  "https://facilitator.mogami.tech",
  "https://open.x402.host",
  "https://openx402.ai",
  "https://facilitator.x402.rs",
];

const fallbackFacilitatorUrls: `${string}://${string}`[] =
  (process.env.FALLBACK_FACILITATOR_URLS?.split(
    ","
  ) as `${string}://${string}`[]) ?? defaultFallbackUrls;

const defaultFacilitators: X402Service[] = [];
for (const url of fallbackFacilitatorUrls) {
  defaultFacilitators.push(new X402Service({ facilitatorConfig: { url } }));
}

if (process.env.CDP_API_KEY_ID && process.env.CDP_API_SECRET_ID) {
  defaultFacilitators.push(
    new X402Service({
      facilitatorConfig: createFacilitatorConfig(
        process.env.CDP_API_KEY_ID,
        process.env.CDP_API_SECRET_ID
      ),
    })
  );
}

export class CompositeX402Service extends X402Service {
  private services: X402Service[];

  constructor({
    services = defaultFacilitators,
    logger = defaultLogger,
  }: {
    services?: X402Service[];
    logger?: Logger;
  } = {}) {
    super({ logger });
    this.services = services;
    this.logger = logger.child({ service: "CompositeX402Service" });

    if (services.length === 0) {
      throw new Error("At least one X402Service must be provided");
    }
  }

  public extractPaymentPayload(
    paymentHeader: string
  ): PaymentPayload | undefined {
    return this.services[0].extractPaymentPayload(paymentHeader);
  }

  public calculateRequirements({
    usdcAmount,
    contentLength,
    contentType,
    resourceUrl,
  }: {
    usdcAmount: `$${string}`;
    contentLength: number;
    contentType: string;
    resourceUrl: string;
  }): PaymentRequirements {
    return this.services[0].calculateRequirements({
      usdcAmount,
      contentLength,
      contentType,
      resourceUrl,
    });
  }

  public async verifyPayment(
    paymentPayload: PaymentPayload,
    requirements: PaymentRequirements
  ): Promise<PaymentVerificationResult> {
    const errors: string[] = [];

    for (let i = 0; i < this.services.length; i++) {
      const service = this.services[i];
      try {
        this.logger.debug(`Attempting verification with service ${i + 1}`);
        const result = await service.verifyPayment(
          paymentPayload,
          requirements
        );

        if (result.isValid) {
          this.logger.info(
            `Payment verified successfully with service ${i + 1}`
          );
          return result;
        }

        errors.push(`Service ${i + 1}: ${result.invalidReason || "Invalid"}`);
      } catch (error: any) {
        const errorMessage = error.message || "Unknown error";
        errors.push(`Service ${i + 1}: ${errorMessage}`);
        this.logger.warn(
          `Verification failed with service ${i + 1}: ${errorMessage}`
        );
      }
    }

    return {
      isValid: false,
      invalidReason: `All services failed: ${errors.join("; ")}`,
    };
  }

  public async settlePayment(
    paymentPayload: PaymentPayload,
    requirements: PaymentRequirements
  ): Promise<PaymentSettlementResult> {
    const errors: string[] = [];

    for (let i = 0; i < this.services.length; i++) {
      const service = this.services[i];
      try {
        this.logger.debug(`Attempting settlement with service ${i + 1}`);
        const result = await service.settlePayment(
          paymentPayload,
          requirements
        );

        if (result.success) {
          this.logger.info(
            `Payment settled successfully with service ${i + 1}`
          );
          return result;
        }

        errors.push(`Service ${i + 1}: ${result.errorReason || "Failed"}`);
      } catch (error: any) {
        const errorMessage = error.message || "Unknown error";
        errors.push(`Service ${i + 1}: ${errorMessage}`);
        this.logger.warn(
          `Settlement failed with service ${i + 1}: ${errorMessage}`
        );
      }
    }

    return {
      success: false,
      errorReason: `All services failed: ${errors.join("; ")}`,
      network: requirements.network,
    };
  }
}

export function paymentPayloadHasAuthorization(
  paymentPayload: unknown
): paymentPayload is {
  payload: { authorization: { from: string } };
} {
  if (
    typeof paymentPayload !== "object" ||
    paymentPayload === null ||
    !("payload" in paymentPayload)
  ) {
    return false;
  }

  const payload = (paymentPayload as any).payload;
  if (
    typeof payload !== "object" ||
    payload === null ||
    !("authorization" in payload)
  ) {
    return false;
  }

  const authorization = payload.authorization;
  if (
    typeof authorization !== "object" ||
    authorization === null ||
    !("from" in authorization)
  ) {
    return false;
  }

  return typeof authorization.from === "string";
}
