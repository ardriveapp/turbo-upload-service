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
import { fail } from "assert/strict";
import { expect } from "chai";

import { TaskCounter } from "./taskCounter";

describe("TaskCounter", () => {
  let taskCounter: TaskCounter;

  beforeEach(() => {
    taskCounter = new TaskCounter();
  });

  it("should start with zero active tasks", () => {
    expect(taskCounter.activeTaskCount()).to.equal(0);
  });

  it("should increment the counter when startTask is called", () => {
    taskCounter.startTask();
    expect(taskCounter.activeTaskCount()).to.equal(1);

    taskCounter.startTask();
    expect(taskCounter.activeTaskCount()).to.equal(2);
  });

  it("should decrement the counter when finishTask is called", () => {
    taskCounter.startTask();
    taskCounter.startTask();
    expect(taskCounter.activeTaskCount()).to.equal(2);

    taskCounter.finishTask();
    expect(taskCounter.activeTaskCount()).to.equal(1);

    taskCounter.finishTask();
    expect(taskCounter.activeTaskCount()).to.equal(0);
  });

  it("should throw an error if finishTask is called with no active tasks", () => {
    expect(() => taskCounter.finishTask()).to.throw(
      "No active tasks to finish"
    );
  });

  it("should resolve waitForZero immediately if no active tasks", async () => {
    try {
      await taskCounter.waitForZero(1);
    } catch (error) {
      throw new Error(
        "waitForZero should not throw an error when no active tasks"
      );
    }
  });

  it("should resolve waitForZero when all tasks are finished", async () => {
    taskCounter.startTask();
    taskCounter.startTask();

    const waitPromise = taskCounter.waitForZero();

    setTimeout(() => {
      taskCounter.finishTask();
      taskCounter.finishTask();
    }, 50);

    try {
      await waitPromise;
    } catch (error) {
      throw new Error(
        "waitForZero should not throw an error when all tasks are finished"
      );
    }
  });

  it("should reject waitForZero if timeout is reached", async () => {
    taskCounter.startTask();

    try {
      await taskCounter.waitForZero(50);
      fail("Expected waitForZero to throw an error due to timeout");
    } catch (error) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      expect((error as any).message).to.include(
        "Timeout waiting for tasks to finish"
      );
      return;
    }

    // Ensure the counter is still accurate after timeout
    expect(taskCounter.activeTaskCount()).to.equal(1);
  });

  it("should resolve waitForZero before timeout if tasks finish in time", async () => {
    taskCounter.startTask();
    const waitPromise = taskCounter.waitForZero(1000);
    taskCounter.finishTask();
    try {
      await waitPromise;
    } catch (error) {
      fail("Expected waitForZero to resolve before timeout");
    }
  });

  it("should handle multiple waitForZero calls correctly", async () => {
    taskCounter.startTask();
    taskCounter.startTask();

    const waitPromise1 = taskCounter.waitForZero(100);
    const waitPromise2 = taskCounter.waitForZero(100);

    taskCounter.finishTask();
    taskCounter.finishTask();

    try {
      await waitPromise1;
      await waitPromise2;
    } catch (error) {
      fail("Expected both waitForZero calls to resolve without error");
    }
  });
});
