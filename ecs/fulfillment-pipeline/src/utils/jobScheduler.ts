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
import { EventEmitter } from "events";
import winston from "winston";

// A simple job scheduler that emits events for job start, job complete, and job error
// Additionally, it attempts to maintain a fixed interval between jobs, and runs successive jobs
// immediately whenever the previous job run's duration was longer than the intended interval
export abstract class JobScheduler extends EventEmitter {
  private timer?: NodeJS.Timeout;
  private shouldKeepRunning = false;
  private lastJobStartTime: number;
  private isProcessing = false;
  private intervalMs: number;
  private schedulerName: string;
  protected logger: winston.Logger;

  constructor({
    intervalMs,
    schedulerName,
    logger,
  }: {
    intervalMs: number;
    schedulerName: string;
    logger: winston.Logger;
  }) {
    super();
    this.shouldKeepRunning = false;
    this.lastJobStartTime = Date.now();
    this.intervalMs = intervalMs;
    this.schedulerName = schedulerName;
    this.logger = logger.child({ scheduler: schedulerName });
  }

  public start(): void {
    if (this.shouldKeepRunning) {
      throw new Error(
        `${this.schedulerName} job scheduler is already running!`
      );
    }
    this.logger.info("Starting job scheduler");
    this.shouldKeepRunning = true;
    this.lastJobStartTime = Date.now();
    this.scheduleNextJob();
  }

  private scheduleNextJob(): void {
    if (!this.shouldKeepRunning) return;

    const nextRunDelay = this.lastJobStartTime + this.intervalMs - Date.now();
    if (nextRunDelay < 0) {
      this.logger.info("Job overdue. Running immediately.");
      this.emit("job-overdue", this.schedulerName);
    } else {
      this.logger.info("Scheduling next job", {
        nextRunDelayMs: nextRunDelay,
      });
    }

    this.timer = setTimeout(() => {
      this.emit("job-start", this.schedulerName);
      this.lastJobStartTime = Date.now();
      void this.executeJob();
    }, Math.max(nextRunDelay, 0)); // Ensure non-negative delay (i.e. run immediately)
  }

  private async executeJob(): Promise<void> {
    try {
      this.isProcessing = true;
      this.logger.info("Starting job");
      await this.processJob();
      this.logger.info("Finished job", {
        duration: Date.now() - this.lastJobStartTime,
      });
      this.emit("job-complete", this.schedulerName);
    } catch (error) {
      this.logger.error("Errored job", {
        durationMs: Date.now() - this.lastJobStartTime,
        error,
      });
      this.emit("job-error", this.schedulerName, error);
    } finally {
      this.isProcessing = false;
      this.scheduleNextJob();
    }
  }

  protected abstract processJob(): Promise<void>;

  public stop(): void {
    if (!this.shouldKeepRunning) {
      this.logger.warn("Job scheduler is already stopped.");
      return;
    }

    this.logger.info("Stopping job scheduler");
    this.shouldKeepRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (this.isProcessing) {
      this.once("job-complete", () => this.emit("stopped", this.schedulerName));
    } else {
      this.emit("stopped", this.schedulerName);
    }
  }
}
