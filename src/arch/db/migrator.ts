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
import { Knex } from "knex";

import globalLogger from "../../logger";
import { columnNames, tableNames } from "./dbConstants";

export abstract class Migrator {
  protected async operate({
    name,
    operation,
  }: {
    name: string;
    operation: () => Promise<void>;
  }) {
    globalLogger.info(`Starting ${name}...`);
    const startTime = Date.now();

    await operation();

    globalLogger.info(`Finished ${name}!`, {
      durationMs: Date.now() - startTime,
    });
  }

  abstract migrate(): Promise<void>;
  abstract rollback(): Promise<void>;
}

export class NullableContentTypeMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "migrate to nullable content type",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table.string(columnNames.contentType).nullable();
        });
        await this.knex.schema.alterTable(
          tableNames.plannedDataItem,
          (table) => {
            table.string(columnNames.contentType).nullable();
          }
        );
        await this.knex.schema.alterTable(
          tableNames.permanentDataItem,
          (table) => {
            table.string(columnNames.contentType).nullable().index();
          }
        );
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from nullable content type",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table.dropColumn(columnNames.contentType);
        });
        await this.knex.schema.alterTable(
          tableNames.plannedDataItem,
          (table) => {
            table.dropColumn(columnNames.contentType);
          }
        );
        await this.knex.schema.alterTable(
          tableNames.permanentDataItem,
          (table) => {
            table.dropColumn(columnNames.contentType);
          }
        );
      },
    });
  }
}
