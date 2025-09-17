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
import CircuitBreaker from "opossum";
import * as promClient from "prom-client";
import winston from "winston";

const breakerSourceNames = [
  "elasticache",
  "fsBackup",
  "dynamodb",
  "remoteConfig",
  "optical_goldsky",
  "optical_legacyGateway",
  "optical_ardriveGateway",
  "unknown",
] as const;
export type BreakerSource = (typeof breakerSourceNames)[number];
const breakerSources: BreakerSource[] = [...breakerSourceNames];

type CounterCfgPlusLabelValues = promClient.CounterConfiguration<string> & {
  expectedLabelNames?: Record<string, string[]>;
};

type GaugeCfgPlusLabelValues = promClient.GaugeConfiguration<string> & {
  expectedLabelNames?: Record<string, string[]>;
};

export class MetricRegistry {
  private static instance: MetricRegistry;
  private registry: promClient.Registry;

  private static createCounter(
    config: CounterCfgPlusLabelValues
  ): promClient.Counter<string> {
    const counter = new promClient.Counter(config);
    this.getInstance().registerMetric(counter);
    // Initialize the counter to zero so it will print right away
    if (config.expectedLabelNames) {
      for (const [labelName, labelValues] of Object.entries(
        config.expectedLabelNames
      )) {
        for (const labelValue of labelValues) {
          counter.inc({ [labelName]: labelValue }, 0);
        }
      }
    } else {
      counter.inc(0);
    }
    return counter;
  }

  private static createHistogram(
    config: promClient.HistogramConfiguration<string>
  ): promClient.Histogram<string> {
    const histogram = new promClient.Histogram(config);
    // Register the histogram with the registry
    this.getInstance().registerMetric(histogram);
    return histogram;
  }

  private static createGauge(
    config: GaugeCfgPlusLabelValues
  ): promClient.Gauge<string> {
    const gauge = new promClient.Gauge(config);
    this.getInstance().registerMetric(gauge);
    // Initialize the gauge to zero so it will print right away
    if (config.expectedLabelNames) {
      for (const [labelName, labelValues] of Object.entries(
        config.expectedLabelNames
      )) {
        for (const labelValue of labelValues) {
          gauge.set({ [labelName]: labelValue }, 0);
        }
      }
    } else {
      gauge.set(0);
    }
    return gauge;
  }

  public static opticalBridgeEnqueueFail = MetricRegistry.createCounter({
    name: "optical_bridge_enqueue_fail_count",
    help: "Number of times the service has failed to enqueue data items for optical bridging",
  });

  public static unbundleBdiEnqueueFail = MetricRegistry.createCounter({
    name: "unbundle_bdi_enqueue_fail_count",
    help: "Number of times the service has failed to enqueue BDIs for unbundling",
  });

  public static refundBalanceFail = MetricRegistry.createCounter({
    name: "refund_failed_call_count",
    help: "Number of times the service is unable to refund a user's balance via the payment service",
  });

  public static uncaughtExceptionCounter = MetricRegistry.createCounter({
    name: "uncaught_exceptions_total",
    help: "Count of uncaught exceptions",
    labelNames: ["error_code"],
  });

  public static usdToArRateFail = MetricRegistry.createCounter({
    name: "usd_to_ar_rate_fail_count",
    help: "Count of failed API calls to the USD/AR endpoint of the payment service",
  });

  public static localCacheDataItemHit = MetricRegistry.createCounter({
    name: "local_cache_data_item_hit_count",
    help: "Count of data items that were found already in the local cache",
  });

  public static fulfillmentJobDurationsSeconds = MetricRegistry.createHistogram(
    {
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
    }
  );

  public static fulfillmentJobFailures = MetricRegistry.createCounter({
    name: "fulfillment_job_failures",
    help: "Count of failures in fulfillment jobs",
    labelNames: ["job_name"],
  });

  public static fulfillmentJobSuccesses = MetricRegistry.createCounter({
    name: "fulfillment_job_successes",
    help: "Count of successes in fulfillment jobs",
    labelNames: ["job_name"],
  });

  public static dataItemRemoveCanceledWhenFoundInDb =
    MetricRegistry.createCounter({
      name: "data_item_remove_canceled_when_found_in_db_count",
      help: "Count of data items that were not removed from object store because they were found in the database",
    });

  public static duplicateDataItemsWithinBatch = MetricRegistry.createCounter({
    name: "duplicate_data_items_within_batch_count",
    help: "Count of duplicate data items within a batch",
  });

  public static duplicateDataItemsFoundFromDatabaseReader =
    MetricRegistry.createCounter({
      name: "duplicate_data_items_found_from_database_reader_count",
      help: "Count of duplicate data items found from the database reader",
    });

  public static primaryKeyErrorsEncounteredOnNewDataItemBatchInsert =
    MetricRegistry.createCounter({
      name: "primary_key_errors_encountered_on_new_data_item_batch_insert_count",
      help: "Count of primary key errors encountered on new data item batch insert",
    });

  public static newDataItemInsertBatchSizes = MetricRegistry.createHistogram({
    name: "new_data_item_insert_batch_size",
    help: "Size of the batch of new data items being inserted",
    buckets: [1, 5, 10, 20, 50, 100, 110],
  });

  public static circuitBreakerOpenCount = MetricRegistry.createCounter({
    name: "circuit_breaker_open_count",
    help: "Count of occasions when a circuit breaker has opened",
    labelNames: ["breaker"],
    expectedLabelNames: {
      breaker: breakerSources,
    },
  });

  public static circuitBreakerState = MetricRegistry.createGauge({
    name: "circuit_breaker_state",
    help: "State of the circuit breaker (1 is open, 0 is closed, 0.5 is half open)",
    labelNames: ["breaker"],
    expectedLabelNames: {
      breaker: breakerSources,
    },
  });

  public static cacheQuarantineSuccess = MetricRegistry.createCounter({
    name: "cache_quarantine_success_count",
    help: "Number of times a data item was successfully quarantined from the cache successfully",
  });

  public static cacheQuarantineFailure = MetricRegistry.createCounter({
    name: "cache_quarantine_failure_count",
    help: "Number of times a data item failed to be quarantined from the cache successfully",
  });

  public static fsBackupQuarantineSuccess = MetricRegistry.createCounter({
    name: "fs_backup_quarantine_success_count",
    help: "Number of times a data item was successfully quarantined from the backup file system successfully",
  });

  public static fsBackupQuarantineFailure = MetricRegistry.createCounter({
    name: "fs_backup_quarantine_failure_count",
    help: "Number of times a data item failed to be quarantined from the backup file system successfully",
  });

  public static objectStoreQuarantineSuccess = MetricRegistry.createCounter({
    name: "obj_store_quarantine_success_count",
    help: "Number of times a data item was successfully quarantined from the object store successfully",
  });

  public static objectStoreQuarantineFailure = MetricRegistry.createCounter({
    name: "obj_store_quarantine_failure_count",
    help: "Number of times a data item failed to be quarantined from the object store successfully",
  });

  public static goldskyOpticalFailure = MetricRegistry.createCounter({
    name: "goldsky_optical_failure_count",
    help: "Number of times the service failure to post to the goldsky optical bridge",
  });

  public static legacyGatewayOpticalFailure = MetricRegistry.createCounter({
    name: "legacy_gateway_optical_failure_count",
    help: "Number of times the service failure to post to the legacy gateway optical bridge",
  });

  public static ardriveGatewayOpticalFailure = MetricRegistry.createCounter({
    name: "ardrive_gateway_optical_failure_count",
    help: "Number of times the service failure to post to the ardrive gateway optical bridge",
  });

  private constructor() {
    this.registry = new promClient.Registry();
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

export function setUpCircuitBreakerListenerMetrics(
  breakerName: BreakerSource,
  breaker: CircuitBreaker,
  logger?: winston.Logger | undefined
) {
  breaker.on("open", () => {
    MetricRegistry.circuitBreakerOpenCount.inc({
      breaker: breakerName,
    });
    MetricRegistry.circuitBreakerState.set(
      {
        breaker: breakerName,
      },
      1
    );
    logger?.error(`${breakerName} circuit breaker opened`);
  });
  breaker.on("close", () => {
    MetricRegistry.circuitBreakerState.set(
      {
        breaker: breakerName,
      },
      0
    );
    logger?.info(`${breakerName} circuit breaker closed`);
  });
  breaker.on("halfOpen", () => {
    MetricRegistry.circuitBreakerState.set(
      {
        breaker: breakerName,
      },
      0.5
    );
    logger?.info(`${breakerName} circuit breaker half-open`);
  });
}
