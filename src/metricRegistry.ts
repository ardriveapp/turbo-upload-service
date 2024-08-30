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

  public static usdToArRateFail = new promClient.Counter({
    name: "usd_to_ar_rate_fail_count",
    help: "Count of failed API calls to the USD/AR endpoint of the payment service",
  });

  public static localCacheDataItemHit = new promClient.Counter({
    name: "local_cache_data_item_hit_count",
    help: "Count of data items that were found already in the local cache",
  });

  public static fulfillmentJobDurationsSeconds = new promClient.Histogram({
    name: "fulfillment_job_durations_seconds",
    help: "Duration of fulfillment jobs in seconds",
    labelNames: ["job_name"],
    buckets: [
      0.01, // 10ms
      0.05, // 50ms
      0.1, //  100ms
      0.25, // 250ms
      0.5, //  500ms
      1, //    1s
      5, //    5s
      10, //   10s
      30, //   30s
      60, //   1min
      300, //  5min
      600, //  10min
      1_200, // 20min
      1_800, // 30min
    ],
  });

  public static fulfillmentJobFailures = new promClient.Counter({
    name: "fulfillment_job_failures",
    help: "Count of failures in fulfillment jobs",
    labelNames: ["job_name"],
  });

  public static fulfillmentJobSuccesses = new promClient.Counter({
    name: "fulfillment_job_successes",
    help: "Count of successes in fulfillment jobs",
    labelNames: ["job_name"],
  });

  public static dataItemRemoveCanceledWhenFoundInDb = new promClient.Counter({
    name: "data_item_remove_canceled_when_found_in_db_count",
    help: "Count of data items that were not removed from object store because they were found in the database",
  });

  public static duplicateDataItemsWithinBatch = new promClient.Counter({
    name: "duplicate_data_items_within_batch_count",
    help: "Count of duplicate data items within a batch",
  });

  public static duplicateDataItemsFoundFromDatabaseReader =
    new promClient.Counter({
      name: "duplicate_data_items_found_from_database_reader_count",
      help: "Count of duplicate data items found from the database reader",
    });

  public static primaryKeyErrorsEncounteredOnNewDataItemBatchInsert =
    new promClient.Counter({
      name: "primary_key_errors_encountered_on_new_data_item_batch_insert_count",
      help: "Count of primary key errors encountered on new data item batch insert",
    });

  public static newDataItemInsertBatchSizes = new promClient.Histogram({
    name: "new_data_item_insert_batch_size",
    help: "Size of the batch of new data items being inserted",
    buckets: [1, 5, 10, 20, 50, 100, 110],
  });

  private constructor() {
    this.registry = new promClient.Registry();

    const metricRegistries = [
      MetricRegistry.opticalBridgeEnqueueFail,
      MetricRegistry.unbundleBdiEnqueueFail,
      MetricRegistry.refundBalanceFail,
      MetricRegistry.uncaughtExceptionCounter,
      MetricRegistry.usdToArRateFail,
      MetricRegistry.localCacheDataItemHit,
      MetricRegistry.dataItemRemoveCanceledWhenFoundInDb,
      MetricRegistry.fulfillmentJobDurationsSeconds,
      MetricRegistry.fulfillmentJobFailures,
      MetricRegistry.fulfillmentJobSuccesses,
      MetricRegistry.duplicateDataItemsWithinBatch,
      MetricRegistry.primaryKeyErrorsEncounteredOnNewDataItemBatchInsert,
      MetricRegistry.duplicateDataItemsFoundFromDatabaseReader,
      MetricRegistry.newDataItemInsertBatchSizes,
    ];

    metricRegistries.forEach((metric) => {
      this.registry.registerMetric(metric);
    });
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

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public registerMetric(metric: promClient.Metric<any>): void {
    this.registry.registerMetric(metric);
  }
}
