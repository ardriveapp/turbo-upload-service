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
import { Knex } from "knex";

import { defaultPremiumFeatureType, maxSignatureLength } from "../../constants";
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

export class MultiPartMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }
  private noTimeZone = { useTz: false };

  public migrate() {
    return this.operate({
      name: "migrate to multipart upload",
      operation: async () => {
        // in flight multipart upload table
        await this.knex.schema.createTable(
          tableNames.inFlightMultiPartUpload,
          async (table) => {
            table.string(columnNames.uploadId).primary();
            table.string(columnNames.uploadKey).notNullable();
            table.string(columnNames.chunkSize).nullable();
            table
              .timestamp(columnNames.createdAt, this.noTimeZone)
              .notNullable()
              .defaultTo(this.knex.fn.now());
            table
              .timestamp(columnNames.expiresAt, this.noTimeZone)
              .notNullable()
              .defaultTo(this.knex.raw("now() + interval '1 day'")); // 24 hours
          }
        );

        await this.knex.schema.createTable(
          tableNames.finishedMultiPartUpload,
          async (table) => {
            table.string(columnNames.uploadId).primary(); // add index
            table.string(columnNames.uploadKey).notNullable();
            table.string(columnNames.chunkSize).notNullable();
            table
              .timestamp(columnNames.createdAt, this.noTimeZone)
              .notNullable();
            table
              .timestamp(columnNames.expiresAt, this.noTimeZone)
              .notNullable();
            table.string(columnNames.etag).notNullable(); // the final etag of the upload
            table
              .timestamp(columnNames.finalizedAt, this.noTimeZone)
              .notNullable()
              .defaultTo(this.knex.fn.now());
            table.string(columnNames.dataItemId).notNullable().index();
          }
        );
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from multipart upload",
      operation: async () => {
        await this.knex.schema.dropTableIfExists(
          tableNames.inFlightMultiPartUpload
        );
        await this.knex.schema.dropTableIfExists(
          tableNames.finishedMultiPartUpload
        );
      },
    });
  }
}

export class IndexUploadDateMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }
  private noTimeZone = { useTz: false };

  public migrate() {
    return this.operate({
      name: "migrate to index upload date",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table
            .timestamp(columnNames.uploadedDate, this.noTimeZone)
            .defaultTo(this.knex.fn.now())
            .notNullable()
            .index()
            .alter();
        });
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback to index upload date",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table.dropIndex(columnNames.uploadedDate);
        });
      },
    });
  }
}

export class DedicatedBundlesMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "migrate to dedicated bundles",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table
            .string(columnNames.premiumFeatureType)
            .defaultTo(defaultPremiumFeatureType);
        });
        await this.knex.schema.alterTable(
          tableNames.plannedDataItem,
          (table) => {
            table
              .string(columnNames.premiumFeatureType)
              .defaultTo(defaultPremiumFeatureType);
          }
        );
        await this.knex.schema.alterTable(
          tableNames.permanentDataItem,
          (table) => {
            table
              .string(columnNames.premiumFeatureType)
              .defaultTo(defaultPremiumFeatureType);
          }
        );
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback to dedicated bundles",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table.dropColumn(columnNames.premiumFeatureType);
        });
        await this.knex.schema.alterTable(
          tableNames.plannedDataItem,
          (table) => {
            table.dropColumn(columnNames.premiumFeatureType);
          }
        );
        await this.knex.schema.alterTable(
          tableNames.permanentDataItem,
          (table) => {
            table.dropColumn(columnNames.premiumFeatureType);
          }
        );
      },
    });
  }
}

export class SignatureFromDbMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "migrate to signature from db",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table.binary(columnNames.signature, maxSignatureLength).nullable();
        });
        await this.knex.schema.alterTable(
          tableNames.plannedDataItem,
          (table) => {
            table.binary(columnNames.signature, maxSignatureLength).nullable();
          }
        );
        // Don't include on permanent data item
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from signature from db",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table.dropColumn(columnNames.signature);
        });
        await this.knex.schema.alterTable(
          tableNames.plannedDataItem,
          (table) => {
            table.dropColumn(columnNames.signature);
          }
        );
      },
    });
  }
}

export class IndexDataItemOwner extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "migration to index data item owner concurrently",
      operation: async () => {
        // NOTE: we use raw statements to execute this migration concurrently and avoid table locking.
        await this.knex.raw(`
          CREATE INDEX CONCURRENTLY IF NOT EXISTS ${tableNames.newDataItem}_${columnNames.owner}_index ON ${tableNames.newDataItem} (${columnNames.owner});
        `);

        await this.knex.raw(`
          CREATE INDEX CONCURRENTLY IF NOT EXISTS ${tableNames.plannedDataItem}_${columnNames.owner}_index ON ${tableNames.plannedDataItem} (${columnNames.owner});
        `);

        await this.knex.raw(`
          CREATE INDEX CONCURRENTLY IF NOT EXISTS ${tableNames.permanentDataItem}_${columnNames.owner}_index ON ${tableNames.permanentDataItem} (${columnNames.owner});
        `);
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from index data item owner",
      operation: async () => {
        await this.knex.raw(`
          DROP INDEX CONCURRENTLY IF EXISTS ${tableNames.newDataItem}_${columnNames.owner}_index;
        `);
        await this.knex.raw(`
          DROP INDEX CONCURRENTLY IF EXISTS ${tableNames.plannedDataItem}_${columnNames.owner}_index;
        `);
        await this.knex.raw(`
          DROP INDEX CONCURRENTLY IF EXISTS ${tableNames.permanentDataItem}_${columnNames.owner}_index;
        `);
      },
    });
  }
}

export class MultiPartFailureReasonMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "migrate to multipart upload failure reason",
      operation: async () => {
        await this.knex.schema.alterTable(
          tableNames.inFlightMultiPartUpload,
          async (table) => {
            table.string(columnNames.failedReason).nullable();
          }
        );
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from multipart upload failure reason",
      operation: async () => {
        await this.knex.schema.alterTable(
          tableNames.inFlightMultiPartUpload,
          (table) => {
            table.dropColumn(columnNames.failedReason);
          }
        );
      },
    });
  }
}

export class FinishedMultiPartFailureReasonMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "migrate to finished multipart upload failure reason",
      operation: async () => {
        await this.knex.schema.alterTable(
          tableNames.finishedMultiPartUpload,
          async (table) => {
            table.string(columnNames.failedReason).nullable();
          }
        );
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from finished multipart upload failure reason",
      operation: async () => {
        await this.knex.schema.alterTable(
          tableNames.finishedMultiPartUpload,
          (table) => {
            table.dropColumn(columnNames.failedReason);
          }
        );
      },
    });
  }
}

export class DeadlineHeightMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "migrate to deadline height",
      operation: async () => {
        await this.knex.schema.alterTable(
          tableNames.newDataItem,
          async (table) => {
            table.string(columnNames.deadlineHeight).nullable();
          }
        );
        await this.knex.schema.alterTable(
          tableNames.plannedDataItem,
          async (table) => {
            table.string(columnNames.deadlineHeight).nullable();
          }
        );
        await this.knex.schema.alterTable(
          tableNames.permanentDataItem,
          async (table) => {
            table.string(columnNames.deadlineHeight).nullable();
          }
        );
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from finished multipart upload failure reason",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table.dropColumn(columnNames.deadlineHeight);
        });
        await this.knex.schema.alterTable(
          tableNames.plannedDataItem,
          (table) => {
            table.dropColumn(columnNames.deadlineHeight);
          }
        );
        await this.knex.schema.alterTable(
          tableNames.permanentDataItem,
          (table) => {
            table.dropColumn(columnNames.deadlineHeight);
          }
        );
      },
    });
  }
}
