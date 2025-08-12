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
import { EventEmitter, once } from "events";
import type winston from "winston";

export class TaskCounter {
  private counter: number;
  private eventEmitter: EventEmitter;
  private logger?: winston.Logger;

  constructor(logger?: winston.Logger) {
    this.counter = 0;
    this.eventEmitter = new EventEmitter();
    this.logger = logger;
  }

  startTask(): void {
    this.counter++;
    this.logger?.debug("TaskCounter.startTask: incremented", {
      activeTaskCount: this.counter,
    });
  }

  finishTask(): void {
    if (this.counter > 0) {
      this.counter--;
      if (this.counter === 0) {
        this.logger?.debug(
          'TaskCounter.finishTask: counter reached zero; emitting "zero"'
        );
        this.eventEmitter.emit("zero");
      } else {
        this.logger?.debug("TaskCounter.finishTask: decremented", {
          activeTaskCount: this.counter,
        });
      }
    } else {
      throw new Error("No active tasks to finish");
    }
  }

  async waitForZero(timeoutMs?: number): Promise<void> {
    if (this.counter === 0) {
      this.logger?.debug("TaskCounter.waitForZero: already zero; returning");
      return;
    }

    if (timeoutMs === undefined) {
      this.logger?.debug("TaskCounter.waitForZero: waiting (no timeout)", {
        activeTaskCount: this.counter,
      });
      await once(this.eventEmitter, "zero");
      this.logger?.debug(
        "TaskCounter.waitForZero: received 'zero'; returning (no timeout)"
      );
      return;
    }

    this.logger?.debug("TaskCounter.waitForZero: waiting with timeout", {
      timeoutMs,
      activeTaskCount: this.counter,
    });

    await new Promise<void>((resolve, reject) => {
      // eslint-disable-next-line prefer-const
      let timer: NodeJS.Timeout;
      const onZero = () => {
        this.logger?.debug(
          "TaskCounter.waitForZero: received 'zero' before timeout; resolving"
        );
        clearTimeout(timer);
        resolve();
      };
      timer = setTimeout(() => {
        this.logger?.debug(
          "TaskCounter.waitForZero: timeout reached; removing listener; rejecting",
          { timeoutMs, activeTaskCount: this.counter }
        );
        this.eventEmitter.removeListener("zero", onZero);
        reject(new Error("Timeout waiting for tasks to finish"));
      }, timeoutMs);

      this.eventEmitter.once("zero", onZero);
    });
  }

  activeTaskCount(): number {
    return this.counter;
  }
}
