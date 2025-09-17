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

import { Database } from "../../../../src/arch/db/database";
import { jobLabels } from "../../../../src/constants";
import { planBundleHandler } from "../../../../src/jobs/plan";
import { fulfillmentJobHandler } from "../utils/jobHandler";
import { JobScheduler } from "../utils/jobScheduler";

export class PlanBundleJobScheduler extends JobScheduler {
  private database: Database;

  constructor({
    intervalMs = 60_000,
    logger,
    database,
  }: {
    intervalMs: number;
    logger: winston.Logger;
    database: Database;
  }) {
    super({ intervalMs, schedulerName: jobLabels.planBundle, logger });
    this.database = database;
  }

  async processJob(): Promise<void> {
    await fulfillmentJobHandler(
      () =>
        planBundleHandler(this.database, undefined, this.logger).catch(
          (error) => {
            this.logger.error("Error planning bundle", error);
          }
        ),
      jobLabels.planBundle
    );
  }
}
