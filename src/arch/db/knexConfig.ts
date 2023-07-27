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
import type { Knex } from "knex";
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import KnexDialect from "knex/lib/dialects/postgres";

const dbHost =
  process.env.DB_WRITER_ENDPOINT || process.env.DB_HOST || "127.0.0.1";
const dbPort = process.env.DB_PORT || 5432;
const dbPassword = process.env.DB_PASSWORD || "postgres";

const dbConnection = `postgres://postgres:${dbPassword}@${dbHost}:${dbPort}/postgres?sslmode=disable`;

export const writerConfig: Knex.Config = {
  client: KnexDialect,
  version: "13.8",
  connection: dbConnection,
  migrations: {
    tableName: "knex_migrations",
    directory: "../../../migrations",
  },
};

export const readerConfig: Knex.Config = {
  ...writerConfig,
  connection: `postgres://postgres:${dbPassword}@${
    process.env.DB_READER_ENDPOINT || dbHost
  }:${dbPort}/postgres?sslmode=disable`,
};
