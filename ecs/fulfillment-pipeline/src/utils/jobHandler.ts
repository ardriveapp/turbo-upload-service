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
import { JobLabel } from "../../../../src/constants";
import { MetricRegistry } from "../../../../src/metricRegistry";

/**
 * Helper function to provide metrics around fulfillment pipeline job
 * handler success/failure counts and job durations. This function should
 * be used as a wrapper around the main handler function for each job.
 */
export async function fulfillmentJobHandler(
  handler: () => Promise<void>,
  jobName: JobLabel
) {
  const jobStartTimeMs = Date.now();

  try {
    await handler();
    MetricRegistry.fulfillmentJobSuccesses.labels(jobName).inc();
  } catch (error) {
    MetricRegistry.fulfillmentJobFailures.labels(jobName).inc();
    throw error;
  } finally {
    MetricRegistry.fulfillmentJobDurationsSeconds
      .labels(jobName)
      .observe((Date.now() - jobStartTimeMs) / 1000);
  }
}
