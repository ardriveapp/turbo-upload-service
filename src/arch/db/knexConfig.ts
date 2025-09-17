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
// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import KnexDialect from "knex/lib/dialects/postgres";
import path from "path";

import logger from "../../logger";

const baseConfig = {
  client: KnexDialect,
  version: process.env.POSTGRES_VERSION ?? "16.1",
  migrations: {
    tableName: "knex_migrations",
    directory: path.join(__dirname, "../../migrations"),
  },
};

function getDbConnection(host: string) {
  const dbUser = process.env.DB_USER || "postgres";
  const dbPassword = process.env.DB_PASSWORD || "postgres";
  const dbPort = +(process.env.DB_PORT || 5432);
  const dbDatabase = process.env.DB_DATABASE || "postgres";

  logger.debug("Getting DB Connection", {
    host,
    dbPort,
  });

  return `postgres://${dbUser}:${dbPassword}@${host}:${dbPort}/${dbDatabase}?sslmode=disable`;
}

export function getWriterConfig() {
  const dbHost =
    process.env.DB_WRITER_ENDPOINT || process.env.DB_HOST || "127.0.0.1";
  return {
    ...baseConfig,
    connection: getDbConnection(dbHost),
  };
}

export function getReaderConfig() {
  const dbHost =
    process.env.DB_READER_ENDPOINT ||
    process.env.DB_WRITER_ENDPOINT ||
    process.env.DB_HOST ||
    "127.0.0.1";
  return {
    ...baseConfig,
    connection: getDbConnection(dbHost),
  };
}
