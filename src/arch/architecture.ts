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
import { Tracer } from "@opentelemetry/api";
import { JWKInterface } from "arbundles";
import knex from "knex";
import winston from "winston";

import { gatewayUrl, migrateOnStartup } from "../constants";
import globalLogger from "../logger";
import { getArweaveWallet } from "../utils/getArweaveWallet";
import { getS3ObjectStore } from "../utils/objectStoreUtils";
import { ArweaveGateway } from "./arweaveGateway";
import { Database } from "./db/database";
import { getReaderConfig, getWriterConfig } from "./db/knexConfig";
import { PostgresDatabase } from "./db/postgres";
import { ObjectStore } from "./objectStore";
import { PaymentService, TurboPaymentService } from "./payment";

export interface Architecture {
  objectStore: ObjectStore;
  database: Database;
  paymentService: PaymentService;
  logger: winston.Logger;
  arweaveGateway: ArweaveGateway;
  getArweaveWallet: () => Promise<JWKInterface>;
  tracer?: Tracer;
}

export const defaultArchitecture: Architecture = {
  database: new PostgresDatabase({
    migrate: migrateOnStartup,
    writer: knex(getWriterConfig()),
    reader: knex(getReaderConfig()),
  }),
  objectStore: getS3ObjectStore(),
  paymentService: new TurboPaymentService(),
  logger: globalLogger,
  getArweaveWallet: () => getArweaveWallet(),
  arweaveGateway: new ArweaveGateway({
    endpoint: gatewayUrl,
  }),
};
