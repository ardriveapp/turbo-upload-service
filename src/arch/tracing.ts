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
import { Tracer, trace } from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-http";
import { AwsInstrumentation } from "@opentelemetry/instrumentation-aws-sdk";
import { PgInstrumentation } from "@opentelemetry/instrumentation-pg";
import { Resource } from "@opentelemetry/resources";
import { NodeSDK } from "@opentelemetry/sdk-node";
import { SemanticResourceAttributes } from "@opentelemetry/semantic-conventions";
import winston from "winston";

import { otelSampleRate } from "../constants";
import globalLogger from "../logger";

export class OTELExporter {
  protected exporter: NodeSDK | undefined;
  protected logger: winston.Logger;

  constructor({
    apiKey,
    logger = globalLogger,
    serviceName = "upload-service",
  }: {
    apiKey: string | undefined;
    serviceName?: string;
    logger?: winston.Logger;
  }) {
    this.logger = logger.child({
      className: "otel-exporter",
      service: serviceName,
    });
    if (!apiKey) {
      this.logger.error("No API key provided for OTEL exporter");
      return;
    }
    const resource = new Resource({
      [SemanticResourceAttributes.SERVICE_NAME]: serviceName,
      SampleRate: otelSampleRate,
    });
    const exporter = new OTLPTraceExporter({
      url: "https://api.honeycomb.io/v1/traces",
      headers: {
        "x-honeycomb-team": apiKey,
        "x-honeycomb-dataset": serviceName,
      },
    });
    this.exporter = new NodeSDK({
      resource,
      traceExporter: exporter,
      instrumentations: [
        // getNodeAutoInstrumentations(), // disabled for now to avoid honeycomb throttling
        new AwsInstrumentation({}),
        new PgInstrumentation(),
      ],
    });
    this.start();
  }

  start() {
    this.logger.info("Starting OTEL exporter", {
      dataset: "upload-service",
    });
    if (this.exporter) {
      this.exporter.start();
    }
  }

  async shutdown() {
    this.logger.debug("Shutting down OTEL exporter");
    if (this.exporter) {
      return this.exporter.shutdown();
    }
  }

  getTracer(tracer: string): Tracer {
    return trace.getTracer(tracer);
  }
}
