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
import winston from "winston";

import { verifyBundleHandler } from "../../../../src/jobs/verify";
import { JobScheduler } from "../utils/jobScheduler";

export class VerifyBundleJobScheduler extends JobScheduler {
  constructor({
    intervalMs = 60_000,
    logger,
  }: {
    intervalMs: number;
    logger: winston.Logger;
  }) {
    super({
      intervalMs,
      schedulerName: "verify-bundle",
      logger,
    });
  }

  async processJob(): Promise<void> {
    await verifyBundleHandler({ logger: this.logger }).catch((error) => {
      this.logger.error("Error verifying bundle", error);
    });
  }
}
