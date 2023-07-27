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
import * as promClient from "prom-client";

export class MetricRegistry {
  private static instance: MetricRegistry;
  private registry: promClient.Registry;

  public static opticalBridgeEnqueueFail = new promClient.Counter({
    name: "optical_bridge_enqueue_fail_count",
    help: "Number of times the service has failed to enqueue data items for optical bridging",
  });

  public static unbundleBdiEnqueueFail = new promClient.Counter({
    name: "unbundle_bdi_enqueue_fail_count",
    help: "Number of times the service has failed to enqueue BDIs for unbundling",
  });

  public static refundBalanceFail = new promClient.Counter({
    name: "refund_failed_call_count",
    help: "Number of times the service is unable to refund a user's balance via the payment service",
  });

  public static uncaughtExceptionCounter = new promClient.Counter({
    name: "uncaught_exceptions_total",
    help: "Count of uncaught exceptions",
  });

  private constructor() {
    this.registry = new promClient.Registry();

    this.registry.registerMetric(MetricRegistry.uncaughtExceptionCounter);
    this.registry.registerMetric(MetricRegistry.opticalBridgeEnqueueFail);
    this.registry.registerMetric(MetricRegistry.unbundleBdiEnqueueFail);
    this.registry.registerMetric(MetricRegistry.refundBalanceFail);
  }

  public static getInstance(): MetricRegistry {
    if (!MetricRegistry.instance) {
      MetricRegistry.instance = new MetricRegistry();
    }

    return MetricRegistry.instance;
  }

  public getRegistry(): promClient.Registry {
    return this.registry;
  }

  public registerMetric(metric: promClient.Metric<any>): void {
    this.registry.registerMetric(metric);
  }
}
