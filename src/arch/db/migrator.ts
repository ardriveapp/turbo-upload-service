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

import {
  defaultPremiumFeatureType,
  failedBundleCSVColumnLength,
  maxSignatureLength,
} from "../../constants";
import logger from "../../logger";
import globalLogger from "../../logger";
import { PermanentDataItemDBResult } from "../../types/dbTypes";
import { isValidArweaveBase64URL } from "../../utils/base64";
import { generateArrayChunks } from "../../utils/common";
import {
  PostgresError,
  postgresInsertFailedPrimaryKeyNotUniqueCode,
} from "../../utils/errors";
import { columnNames, tableNames } from "./dbConstants";

export abstract class Migrator {
  protected async operate({
    name,
    operation,
  }: {
    name: string;
    operation: () => Promise<void>;
  }) {
    globalLogger.debug(`Starting ${name}...`);
    const startTime = Date.now();

    await operation();

    globalLogger.debug(`Finished ${name}!`, {
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

export class FailedDataItemMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "migrate to failed data item",
      operation: async () => {
        await this.knex.schema.createTableLike(
          tableNames.failedDataItem,
          tableNames.plannedDataItem,
          async (table) => {
            table
              .timestamp(columnNames.failedDate)
              .notNullable()
              .defaultTo(this.knex.fn.now())
              .index();
            table.string(columnNames.failedReason).notNullable().index();
          }
        );
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from failed data item",
      operation: async () => {
        await this.knex.schema.dropTableIfExists(tableNames.failedDataItem);
      },
    });
  }
}

export class BumpFailedBundlesCharLimitMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "bump failed bundles char limit",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table
            .string(columnNames.failedBundles, failedBundleCSVColumnLength)
            .nullable()
            .alter();
        });
        await this.knex.schema.alterTable(
          tableNames.plannedDataItem,
          (table) => {
            table
              .string(columnNames.failedBundles, failedBundleCSVColumnLength)
              .nullable()
              .alter();
          }
        );
        await this.knex.schema.alterTable(
          tableNames.failedDataItem,
          (table) => {
            table
              .string(columnNames.failedBundles, failedBundleCSVColumnLength)
              .nullable()
              .alter();
          }
        );
        await this.knex.schema.alterTable(
          tableNames.permanentDataItem,
          (table) => {
            table
              .string(columnNames.failedBundles, failedBundleCSVColumnLength)
              .nullable()
              .alter();
          }
        );
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from bump failed bundles char limit",
      operation: async () => {
        await this.knex.schema.alterTable(tableNames.newDataItem, (table) => {
          table.string(columnNames.failedBundles).nullable().alter();
        });
        await this.knex.schema.alterTable(
          tableNames.plannedDataItem,
          (table) => {
            table.string(columnNames.failedBundles).nullable().alter();
          }
        );
        await this.knex.schema.alterTable(
          tableNames.permanentDataItem,
          (table) => {
            table.string(columnNames.failedBundles).nullable().alter();
          }
        );
      },
    });
  }
}

export class PartitionedPermanentDataItemsMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "migrate to partitioned permanent data items",
      operation: async () => {
        await this.knex.schema.raw(`
          CREATE TABLE IF NOT EXISTS permanent_data_items (
            data_item_id           VARCHAR(43)                  NOT NULL,
            owner_public_address   VARCHAR(43)                  NOT NULL,
            byte_count             VARCHAR(255)                 NOT NULL,
            uploaded_date          TIMESTAMP without time zone  NOT NULL  DEFAULT now(),
            assessed_winston_price VARCHAR(255)                 NOT NULL,
            plan_id                VARCHAR(255)                 NOT NULL,
            planned_date           TIMESTAMP                    NOT NULL  DEFAULT now(),
            bundle_id              VARCHAR(43)                  NOT NULL,
            permanent_date         TIMESTAMP                    NOT NULL  DEFAULT now(),
            block_height           INTEGER                      NOT NULL,
            data_start             INTEGER,
            signature_type         INTEGER,
            failed_bundles         VARCHAR(880),
            content_type           VARCHAR(255),
            premium_feature_type   VARCHAR(255)                           DEFAULT 'default',
            deadline_height        INTEGER,
            PRIMARY KEY (data_item_id, uploaded_date)
          ) PARTITION BY RANGE (uploaded_date);

          CREATE INDEX IF NOT EXISTS permanent_data_items_owner_public_address_index ON permanent_data_items (owner_public_address);
          CREATE INDEX IF NOT EXISTS permanent_data_items_block_height_index ON permanent_data_items (block_height);
          CREATE INDEX IF NOT EXISTS permanent_data_items_plan_id_index ON permanent_data_items (plan_id);
          CREATE INDEX IF NOT EXISTS permanent_data_items_bundle_id_index ON permanent_data_items (bundle_id);
          
          CREATE INDEX IF NOT EXISTS permanent_data_items_signature_type_index ON permanent_data_items (signature_type);

          CREATE TABLE IF NOT EXISTS permanent_data_items_pre_12_2023 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2020-01-01') TO ('2023-12-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_12_2023_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2023-12-01') TO ('2023-12-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_12_2023_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2023-12-15') TO ('2024-01-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_01_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-01-01') TO ('2024-01-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_01_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-01-15') TO ('2024-02-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_02_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-02-01') TO ('2024-02-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_02_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-02-15') TO ('2024-03-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_03_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-03-01') TO ('2024-03-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_03_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-03-15') TO ('2024-04-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_04_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-04-01') TO ('2024-04-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_04_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-04-15') TO ('2024-05-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_05_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-05-01') TO ('2024-05-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_05_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-05-15') TO ('2024-06-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_06_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-06-01') TO ('2024-06-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_06_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-06-15') TO ('2024-07-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_07_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-07-01') TO ('2024-07-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_07_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-07-15') TO ('2024-08-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_08_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-08-01') TO ('2024-08-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_08_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-08-15') TO ('2024-09-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_09_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-09-01') TO ('2024-09-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_09_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-09-15') TO ('2024-10-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_10_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-10-01') TO ('2024-10-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_10_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-10-15') TO ('2024-11-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_11_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-11-01') TO ('2024-11-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_11_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-11-15') TO ('2024-12-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_12_2024_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-12-01') TO ('2024-12-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_12_2024_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2024-12-15') TO ('2025-01-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_01_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-01-01') TO ('2025-01-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_01_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-01-15') TO ('2025-02-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_02_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-02-01') TO ('2025-02-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_02_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-02-15') TO ('2025-03-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_03_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-03-01') TO ('2025-03-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_03_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-03-15') TO ('2025-04-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_04_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-04-01') TO ('2025-04-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_04_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-04-15') TO ('2025-05-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_05_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-05-01') TO ('2025-05-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_05_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-05-15') TO ('2025-06-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_06_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-06-01') TO ('2025-06-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_06_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-06-15') TO ('2025-07-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_07_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-07-01') TO ('2025-07-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_07_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-07-15') TO ('2025-08-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_08_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-08-01') TO ('2025-08-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_08_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-08-15') TO ('2025-09-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_09_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-09-01') TO ('2025-09-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_09_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-09-15') TO ('2025-10-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_10_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-10-01') TO ('2025-10-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_10_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-10-15') TO ('2025-11-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_11_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-11-01') TO ('2025-11-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_11_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-11-15') TO ('2025-12-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_future PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-01-01') TO (MAXVALUE);
          `);
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from partitioned permanent data items",
      operation: async () => {
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_pre_12_2023"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_12_2023_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_12_2023_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_01_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_01_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_02_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_02_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_03_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_03_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_04_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_04_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_05_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_05_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_06_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_06_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_07_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_07_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_08_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_08_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_09_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_09_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_10_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_10_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_11_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_11_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_12_2024_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_12_2024_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_01_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_01_2025_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_02_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_02_2025_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_03_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_03_2025_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_04_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_04_2025_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_05_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_05_2025_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_06_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_06_2025_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_07_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_07_2025_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_08_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_08_2025_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_09_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_09_2025_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_10_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_10_2025_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_11_2025_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_11_2025_02"
        );
        await this.knex.schema.dropTableIfExists("permanent_data_items_future");
      },
    });
  }
}

type DeprecatedPermanentDataItemDBResult = PermanentDataItemDBResult & {
  block_height: string;
  deadline_height: string | null;
};

/** Extracted to a function so we can run the backfill before and outside the scope of the migration */
export async function backfillPermanentDataItems(knex: Knex) {
  const batchCount = 1000;
  const startBlock = +(
    process.env.PERMANENT_DATA_ITEM_BACKFILL_START_BLOCK || 1045991
  ); // First height in turbo PROD
  const endBlock = +(
    process.env.PERMANENT_DATA_ITEM_BACKFILL_END_BLOCK || 1470456
  ); // Last height in turbo PROD

  const heightsToBackfill =
    process.env
      .PERMANENT_DATA_ITEM_BACKFILL_SHOULD_SKIP_DISTINCT_BLOCK_HEIGHT_QUERY ===
    "true"
      ? Array.from(
          { length: endBlock - startBlock + 1 },
          (_, i) => i + startBlock
        )
      : (
          await knex<DeprecatedPermanentDataItemDBResult>(
            tableNames.permanentDataItem
          ).distinct(columnNames.blockHeight)
        )
          .map((row) => Number(row.block_height))
          .sort((a, b) => a - b)
          // Filter out blocks that are are outside the range
          .filter(
            (block_height) =>
              block_height >= startBlock && block_height <= endBlock
          );

  for (let i = 0; i < heightsToBackfill.length; i++) {
    const block_height = heightsToBackfill[i];

    const permanentDataItems = await knex<DeprecatedPermanentDataItemDBResult>(
      tableNames.permanentDataItem
    ).where(columnNames.blockHeight, `${block_height}`);

    if (permanentDataItems.length === 0) {
      logger.info(`No permanent data items for block height ${block_height}`);
      continue;
    }

    const batchedItems = generateArrayChunks(permanentDataItems, batchCount);

    for (const batch of batchedItems) {
      await knex.transaction(async (trx) => {
        let attempts = 0;
        const maxAttempts = batch.length;

        async function performInsert(
          batch: DeprecatedPermanentDataItemDBResult[]
        ): Promise<void> {
          try {
            await trx.batchInsert(
              tableNames.permanentDataItems,
              batch.map((row) => ({
                ...row,
                block_height: Number(row.block_height),
                deadline_height: Number(row.deadline_height),
              }))
            );
          } catch (error) {
            attempts++;

            if (attempts >= maxAttempts) {
              logger.error("Failed to insert row after max attempts", error);
              throw error;
            }

            const failedId = (error as PostgresError).detail?.match(
              /\(data_item_id\)=\(([^)]+)\)/
            )?.[1];

            if (
              (error as PostgresError).code ===
                postgresInsertFailedPrimaryKeyNotUniqueCode &&
              failedId &&
              isValidArweaveBase64URL(failedId)
            ) {
              const batchWithoutFailedId = batch.filter(
                (row) => row.data_item_id !== failedId
              );

              if (batchWithoutFailedId.length === batch.length) {
                logger.error(
                  "Failed id not found in batch to remove and proceed",
                  failedId
                );
                throw error;
              }

              // Remove this from the batch and try again
              return performInsert(batchWithoutFailedId);
            }
          }
        }

        await performInsert(batch);
      });
    }

    logger.info(
      `Backfilled ${permanentDataItems.length} permanent data items for block height ${block_height}`
    );
  }
}

export class BackfillPermanentDataItemsMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public async migrate() {
    return this.operate({
      name: "backfill permanent data items",
      operation: async () => {
        await backfillPermanentDataItems(this.knex);
        await this.knex.schema.dropTableIfExists(tableNames.permanentDataItem);
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from backfill permanent data items",
      operation: async () => {
        await this.knex.schema.createTableIfNotExists(
          tableNames.permanentDataItem,
          async (table) => {
            table.string(columnNames.dataItemId).primary();
            table.string(columnNames.owner).notNullable().index();
            table.string(columnNames.byteCount).notNullable();
            table.timestamp(columnNames.uploadedDate).notNullable();
            table.string(columnNames.winstonPrice).notNullable();
            table.string(columnNames.planId).notNullable().index();
            table.timestamp(columnNames.plannedDate).notNullable();
            table.string(columnNames.bundleId).notNullable().index();
            table.string(columnNames.blockHeight).notNullable().index();
            table.integer(columnNames.dataStart).nullable();
            table.integer(columnNames.signatureType).nullable();
            table.string(columnNames.failedBundles).nullable();
            table.string(columnNames.contentType).nullable();
            table.string(columnNames.premiumFeatureType).defaultTo("default");
            table.string(columnNames.deadlineHeight).nullable();
          }
        );
      },
    });
  }
}

export class Add2026PartitionsMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }

  public migrate() {
    return this.operate({
      name: "add 2026 partitions to permanent data items",
      operation: async () => {
        await this.knex.schema.raw(`
          DROP TABLE IF EXISTS permanent_data_items_future;

          CREATE TABLE IF NOT EXISTS permanent_data_items_12_2025_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-12-01') TO ('2025-12-15');
          CREATE TABLE IF NOT EXISTS permanent_data_items_12_2025_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2025-12-15') TO ('2026-01-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_01_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-01-01') TO ('2026-01-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_01_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-01-15') TO ('2026-02-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_02_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-02-01') TO ('2026-02-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_02_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-02-15') TO ('2026-03-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_03_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-03-01') TO ('2026-03-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_03_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-03-15') TO ('2026-04-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_04_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-04-01') TO ('2026-04-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_04_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-04-15') TO ('2026-05-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_05_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-05-01') TO ('2026-05-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_05_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-05-15') TO ('2026-06-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_06_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-06-01') TO ('2026-06-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_06_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-06-15') TO ('2026-07-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_07_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-07-01') TO ('2026-07-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_07_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-07-15') TO ('2026-08-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_08_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-08-01') TO ('2026-08-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_08_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-08-15') TO ('2026-09-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_09_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-09-01') TO ('2026-09-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_09_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-09-15') TO ('2026-10-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_10_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-10-01') TO ('2026-10-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_10_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-10-15') TO ('2026-11-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_11_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-11-01') TO ('2026-11-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_11_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-11-15') TO ('2026-12-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_12_2026_01 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-12-01') TO ('2026-12-15');

          CREATE TABLE IF NOT EXISTS permanent_data_items_12_2026_02 PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-12-15') TO ('2027-01-01');

          CREATE TABLE IF NOT EXISTS permanent_data_items_future PARTITION OF permanent_data_items
          FOR VALUES FROM ('2027-01-01') TO (MAXVALUE);
          `);
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from add 2026 partitions to permanent data items",
      operation: async () => {
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_01_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_01_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_02_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_02_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_03_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_03_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_04_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_04_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_05_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_05_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_06_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_06_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_07_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_07_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_08_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_08_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_09_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_09_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_10_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_10_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_11_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_11_2026_02"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_12_2026_01"
        );
        await this.knex.schema.dropTableIfExists(
          "permanent_data_items_12_2026_02"
        );
        await this.knex.schema.dropTableIfExists("permanent_data_items_future");
        // Recreate the old future partition
        await this.knex.schema.raw(`
          CREATE TABLE IF NOT EXISTS permanent_data_items_future PARTITION OF permanent_data_items
          FOR VALUES FROM ('2026-01-01') TO (MAXVALUE);
          `);
      },
    });
  }
}

export class X402PaymentsMigrator extends Migrator {
  constructor(private readonly knex: Knex) {
    super();
  }
  private noTimeZone = { useTz: false };

  public migrate() {
    return this.operate({
      name: "migrate to x402 payments table",
      operation: async () => {
        await this.knex.schema.createTable("x402_payments", (table) => {
          table.string("tx_hash", 66).notNullable().primary();
          table.string("network", 50).notNullable();
          table.string("payer_address", 66).notNullable().index(); // gives room to support longer address formats in future (Solana is 44, Ethereum is 42 with 0x, Arweave is 43, etc)
          table.string("usdc_amount", 255).notNullable();
          table.string("winc_amount", 255).notNullable();
          table.string("data_item_id", 43).nullable().index();
          table.bigInteger("byte_count").notNullable();
          table
            .timestamp("created_at", this.noTimeZone)
            .defaultTo(this.knex.fn.now())
            .notNullable();
          table
            .timestamp("settled_at", this.noTimeZone)
            .defaultTo(this.knex.fn.now())
            .notNullable();

          // Composite index for queries by payer and created date
          table.index(["payer_address", "created_at"]);
        });
      },
    });
  }

  public rollback() {
    return this.operate({
      name: "rollback from x402 payments table",
      operation: async () => {
        await this.knex.schema.dropTableIfExists("x402_payments");
      },
    });
  }
}
