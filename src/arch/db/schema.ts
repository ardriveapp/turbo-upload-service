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

import logger from "../../logger";
import {
  BlockHeight,
  PermanentBundleDBResult,
  PlanId,
  PostedBundleDBResult,
  SeedResultDBResult,
  SeededBundleDBResult,
} from "../../types/dbTypes";
import { TransactionId } from "../../types/types";
import { createAxiosInstance } from "../axiosClient";
import { columnNames, tableNames } from "./dbConstants";

const {
  bundlePlan,
  failedBundle,
  newBundle,
  newDataItem,
  permanentBundle,
  permanentDataItem,
  plannedDataItem,
  postedBundle,
  seededBundle,
  seedResult,
} = tableNames;

const {
  blockHeight,
  bundleId,
  byteCount,
  dataItemId,
  failedDate,
  owner,
  permanentDate,
  planId,
  plannedDate,
  postedDate,
  reward,
  seedResultStatus,
  seededDate,
  signedDate,
  uploadedDate,
  winstonPrice,
  indexedOnGQL,
} = columnNames;

export class Schema {
  private constructor(private readonly pg: Knex) {}

  public static create(pg: Knex): Promise<void> {
    return new Schema(pg).initializeSchema();
  }

  public static rollback(pg: Knex): Promise<void> {
    return new Schema(pg).rollbackInitialSchema();
  }

  public static migrateToVerify(pg: Knex): Promise<void> {
    return new Schema(pg).migrateToVerify();
  }

  public static rollbackFromVerify(pg: Knex): Promise<void> {
    return new Schema(pg).rollbackFromVerify();
  }

  public static migrateToIndexPlanIds(pg: Knex): Promise<void> {
    return new Schema(pg).migrateToIndexPlanIds();
  }

  public static rollbackFromIndexPlanIds(pg: Knex): Promise<void> {
    return new Schema(pg).rollbackFromIndexPlanIds();
  }

  public static migrateToPreserveBlockHeight(pg: Knex): Promise<void> {
    return new Schema(pg).migrateToPreserveBlockHeight();
  }

  public static rollbackFromPreserveBlockHeight(pg: Knex): Promise<void> {
    return new Schema(pg).rollbackFromPreserveBlockHeight();
  }

  public static migrateToPreserveSigType(pg: Knex): Promise<void> {
    return new Schema(pg).migrateToPreserveSigType();
  }

  public static rollbackFromPreserveSigType(pg: Knex): Promise<void> {
    return new Schema(pg).rollbackFromPreserveSigType();
  }

  public static rollbackFull(pg: Knex): Promise<void> {
    return new Schema(pg).rollbackFullSchema();
  }

  public static migrateToAddARtoUSDConversionRates(pg: Knex): Promise<void> {
    return new Schema(pg).migrateToAddARtoUSDConversionRates();
  }

  public static rollbackFromAddARtoUSDConversionRates(pg: Knex): Promise<void> {
    return new Schema(pg).rollbackFromAddARtoUSDConversionRates();
  }

  public static migrateToByteCountBundleColumns(pg: Knex): Promise<void> {
    return new Schema(pg).migrateToByteCountBundleColumns();
  }

  public static rollbackFromByteCountBundleColumns(pg: Knex): Promise<void> {
    return new Schema(pg).rollbackFromByteCountBundleColumns();
  }

  private async initializeSchema(): Promise<void> {
    logger.info("Starting initial migration...");

    await this.createNewDataItemTable();
    await this.createBundlePlanTable();
    await this.createPlannedDataItemTable();
    await this.createNewBundleTable();
    await this.createPostedBundleTable();
    await this.createSeededBundleTable();
    await this.createFailedBundleTable();
    await this.createPermanentBundleTable();
    await this.createPermanentDataItemTable();
    await this.createSeedResultTable();

    logger.info("Finished initial migration!");
  }

  /** rollback from the initial migration */
  private async rollbackInitialSchema(): Promise<void> {
    await this.pg.schema.dropTable(seedResult);
    return this.rollbackFullSchema();
  }

  /** rollback from a fully migrated DB */
  private async rollbackFullSchema(): Promise<void> {
    // drop all tables except seedResult which is dropped in block height migration
    await this.pg.schema.dropTable(newDataItem);
    await this.pg.schema.dropTable(plannedDataItem);
    await this.pg.schema.dropTable(bundlePlan);
    await this.pg.schema.dropTable(newBundle);
    await this.pg.schema.dropTable(postedBundle);
    await this.pg.schema.dropTable(seededBundle);
    await this.pg.schema.dropTable(failedBundle);
    await this.pg.schema.dropTable(permanentDataItem);
    await this.pg.schema.dropTable(permanentBundle);
  }

  private async migrateToVerify() {
    logger.info("Starting verify job migration...");
    const migrationStartTime = Date.now();
    const batchSize = 1000;

    try {
      // Create plan id column on seedResult table
      await this.pg.schema.table(tableNames.seedResult, (t) => {
        t.string(planId);
      });
    } catch (error) {
      // Will happen if lambda runs multiple times
      logger.error("Column creation failed!", { error });
    }

    let continueMigration = true;
    while (continueMigration) {
      logger.info("Starting next backfill plan_id batch transaction...");
      await this.pg.transaction(async (knexTransaction) => {
        const seedResultDbResults = await knexTransaction<SeedResultDBResult>(
          tableNames.seedResult
        )
          .whereNull(columnNames.planId)
          .limit(batchSize);

        if (seedResultDbResults.length === 0) {
          // End this loop if none are left to backfill
          continueMigration = false;
          return;
        }

        for (const { bundle_id } of seedResultDbResults) {
          const planId = await this.getPlanIdForSeedResultBundleId(
            knexTransaction,
            bundle_id
          );

          if (planId) {
            // Add the plan_id to the new column
            await knexTransaction<SeedResultDBResult>(tableNames.seedResult)
              .where({ bundle_id })
              .update({
                [columnNames.planId]: planId,
              });
          } else {
            // This shouldn't happen. Throw an error if it does, because primary key
            // alteration below will throw an error and fail if we were to reach this
            throw Error(
              `No seeded_bundle or posted_bundle or permanent_bundle found for seed_result! Bundle ID: ${bundle_id}`
            );
          }
        }
      });
    }

    // Alter plan_id column on seed_result table to be the primary key
    await this.pg.schema.table(tableNames.seedResult, (t) => {
      t.string(planId).primary().alter();
    });

    // Add indexed_on_gql column on permanent_bundle
    await this.pg.schema.table(tableNames.permanentBundle, (t) => {
      t.boolean(indexedOnGQL).nullable();
    });

    logger.info("Finished verify job migration!", {
      migrationMs: Date.now() - migrationStartTime,
    });
  }

  private async getPlanIdForSeedResultBundleId(
    knexTransaction: Knex.Transaction,
    bundle_id: TransactionId
  ): Promise<PlanId | undefined> {
    const seededBundleDbResult = await knexTransaction<SeededBundleDBResult>(
      tableNames.seededBundle
    ).where({ bundle_id });
    if (seededBundleDbResult.length !== 0) {
      return seededBundleDbResult[0].plan_id;
    }

    const postedBundleDbResults = await knexTransaction<PostedBundleDBResult>(
      tableNames.postedBundle
    ).where({ bundle_id });
    if (postedBundleDbResults.length !== 0) {
      return postedBundleDbResults[0].plan_id;
    }

    const permanentBundleDbResults =
      await knexTransaction<PermanentBundleDBResult>(
        tableNames.permanentBundle
      ).where({ bundle_id });
    if (permanentBundleDbResults.length !== 0) {
      return permanentBundleDbResults[0].plan_id;
    }

    return undefined;
  }

  private async rollbackFromVerify() {
    return this.pg.transaction(async (transaction) => {
      await transaction.schema.alterTable(tableNames.seedResult, (t) => {
        t.dropColumn(planId);
      });
      await transaction.schema.alterTable(tableNames.permanentBundle, (t) => {
        t.dropColumn(indexedOnGQL);
      });
    });
  }

  private async migrateToIndexPlanIds() {
    logger.info("Starting index plan ids migration...");
    const migrationStartTime = Date.now();

    return this.pg.transaction(async (transaction) => {
      await transaction.schema.alterTable(tableNames.plannedDataItem, (t) => {
        t.string(planId).alter().index().notNullable();
      });
      await transaction.schema.alterTable(tableNames.permanentDataItem, (t) => {
        t.string(planId).alter().index().notNullable();
      });
      await transaction.schema.alterTable(tableNames.newBundle, (t) => {
        t.string(planId).alter().index().notNullable();
      });
      await transaction.schema.alterTable(tableNames.postedBundle, (t) => {
        t.string(planId).alter().index().notNullable();
      });
      await transaction.schema.alterTable(tableNames.seededBundle, (t) => {
        t.string(planId).alter().index().notNullable();
      });
      await transaction.schema.alterTable(tableNames.failedBundle, (t) => {
        t.string(planId).alter().index().notNullable();
      });
      await transaction.schema.alterTable(tableNames.permanentBundle, (t) => {
        t.string(planId).alter().index().notNullable();
      });

      logger.info("Finished index plan ids migration!", {
        migrationMs: Date.now() - migrationStartTime,
      });
    });
  }

  private async rollbackFromIndexPlanIds() {
    return this.pg.transaction(async (transaction) => {
      await transaction.schema.alterTable(tableNames.plannedDataItem, (t) => {
        t.dropIndex(planId);
      });
      await transaction.schema.alterTable(tableNames.permanentDataItem, (t) => {
        t.dropIndex(planId);
      });
      await transaction.schema.alterTable(tableNames.newBundle, (t) => {
        t.dropIndex(planId);
      });
      await transaction.schema.alterTable(tableNames.postedBundle, (t) => {
        t.dropIndex(planId);
      });
      await transaction.schema.alterTable(tableNames.seededBundle, (t) => {
        t.dropIndex(planId);
      });
      await transaction.schema.alterTable(tableNames.failedBundle, (t) => {
        t.dropIndex(planId);
      });
      await transaction.schema.alterTable(tableNames.permanentBundle, (t) => {
        t.dropIndex(planId);
      });
    });
  }

  private async migrateToPreserveBlockHeight(): Promise<void> {
    logger.info("Starting preserve block height migration...");
    const migrationStartTime = Date.now();

    logger.info("Altering tables to allow nullable block heights...", {
      migrationStartTime,
    });
    await this.pg.schema.alterTable(tableNames.permanentBundle, (t) => {
      t.string(columnNames.blockHeight).nullable().index();
    });
    await this.pg.schema.table(tableNames.permanentDataItem, (t) => {
      t.string(columnNames.blockHeight).nullable().index();
      t.string(columnNames.bundleId, 43).notNullable().index().alter();
    });

    logger.info("Back filling block heights...", {
      migrationMs: Date.now() - migrationStartTime,
    });
    await this.backFillBlockHeight(migrationStartTime);

    logger.info("Altering tables to allow NOT nullable block heights...", {
      migrationMs: Date.now() - migrationStartTime,
    });
    await this.pg.schema.table(tableNames.permanentBundle, (t) => {
      t.string(columnNames.blockHeight).notNullable().alter();
    });
    await this.pg.schema.table(tableNames.permanentDataItem, (t) => {
      t.string(columnNames.blockHeight).notNullable().alter();
    });

    logger.info("Excising seed_result table...", {
      migrationMs: Date.now() - migrationStartTime,
    });
    await this.pg.schema.dropTable(tableNames.seedResult);

    logger.info("Finished preserve block height migration!", {
      migrationMs: Date.now() - migrationStartTime,
    });
  }

  private async backFillBlockHeight(migrationStartTime: number): Promise<void> {
    const batchSize = 100;
    let continueBackfill = true;

    const allPermanentBundles = await this.pg<PermanentBundleDBResult>(
      tableNames.permanentBundle
    );

    const totalBundles = allPermanentBundles.length;
    let bundlesProcessed = 0;

    logger.info("Back filling block heights from network results...", {
      migrationMs: Date.now() - migrationStartTime,
    });

    while (continueBackfill) {
      const permanentBundleDBResults = await this.pg<PermanentBundleDBResult>(
        tableNames.permanentBundle
      )
        .whereNull(columnNames.blockHeight)
        .limit(batchSize);

      if (permanentBundleDBResults.length === 0) {
        // End loop at end of backfill
        continueBackfill = false;
        break;
      }

      const bundleIds = permanentBundleDBResults.map((b) => b.bundle_id);
      logger.info("Getting next batch from network...", {
        migrationMs: Date.now() - migrationStartTime,
        bundlesProcessed,
        totalBundles,
      });
      // Get block heights from a gateway using GQL
      const response = await createAxiosInstance({}).post(
        "https://arweave.net/graphql",
        {
          query: `query {
          transactions(ids: [${bundleIds.map((b) => `"${b}"`)}] first: 100) {
            edges {
              node {
                block {
                  height
                }
                id
              }
            }
          } 
        }
        `,
        }
      );

      const edges = response.data.data.transactions.edges;
      const idsAndHeights: [[TransactionId, BlockHeight]] = edges.map(
        ({ node }: { node: { id: string; block: { height: string } } }) => [
          node.id,
          +node.block.height,
        ]
      );

      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const promises: Promise<any>[] = [];
      idsAndHeights.forEach(([bundle_id, block_height]) => {
        promises.push(
          this.pg(tableNames.permanentBundle)
            .where({ bundle_id })
            .update({ [columnNames.blockHeight]: `${block_height}` }),
          this.pg(tableNames.permanentDataItem)
            .where({ bundle_id })
            .update({
              [columnNames.blockHeight]: `${block_height}`,
            })
        );
      });

      logger.info("Updating batch into permanent bundles ...", {
        migrationMs: Date.now() - migrationStartTime,
        bundlesProcessed,
        totalBundles,
      });
      await Promise.all(promises);
      bundlesProcessed += batchSize;
    }
  }

  private async rollbackFromPreserveBlockHeight() {
    return this.pg.transaction(async (transaction) => {
      await transaction.schema.alterTable(tableNames.permanentBundle, (t) => {
        t.dropColumn(columnNames.blockHeight);
      });
      await transaction.schema.alterTable(tableNames.permanentDataItem, (t) => {
        t.dropColumn(columnNames.blockHeight);
      });
      await transaction.schema.createTable(seedResult, (t) => {
        t.string(bundleId, 43).notNullable();
        t.string(planId).notNullable().index();
        t.string(blockHeight).nullable();
        t.string(seedResultStatus).notNullable().defaultTo("posted");
      });
    });
  }

  private async migrateToPreserveSigType(): Promise<void> {
    logger.info("Starting preserve signature type migration...");
    const migrationStartTime = Date.now();

    await this.pg.schema.alterTable(tableNames.newDataItem, (t) => {
      t.integer(columnNames.dataStart).nullable();
      t.integer(columnNames.signatureType).nullable();
      t.string(columnNames.failedBundles).nullable();
    });
    await this.pg.schema.alterTable(tableNames.plannedDataItem, (t) => {
      t.integer(columnNames.dataStart).nullable();
      t.integer(columnNames.signatureType).nullable();
      t.string(columnNames.failedBundles).nullable();
    });
    await this.pg.schema.alterTable(tableNames.permanentDataItem, (t) => {
      t.integer(columnNames.dataStart).nullable();
      t.integer(columnNames.signatureType).nullable();
      t.string(columnNames.failedBundles).nullable();
    });

    await this.pg.schema.alterTable(tableNames.newBundle, (t) => {
      t.integer(columnNames.transactionByteCount).nullable();
      t.integer(columnNames.headerByteCount).nullable();
      t.integer(columnNames.payloadByteCount).nullable();
    });
    await this.pg.schema.alterTable(tableNames.postedBundle, (t) => {
      t.integer(columnNames.transactionByteCount).nullable();
      t.integer(columnNames.headerByteCount).nullable();
      t.integer(columnNames.payloadByteCount).nullable();
    });
    await this.pg.schema.alterTable(tableNames.seededBundle, (t) => {
      t.integer(columnNames.transactionByteCount).nullable();
      t.integer(columnNames.headerByteCount).nullable();
      t.integer(columnNames.payloadByteCount).nullable();
    });
    await this.pg.schema.alterTable(tableNames.failedBundle, (t) => {
      t.integer(columnNames.transactionByteCount).nullable();
      t.integer(columnNames.headerByteCount).nullable();
      t.integer(columnNames.payloadByteCount).nullable();
      t.string(columnNames.failedReason).nullable();
    });
    await this.pg.schema.alterTable(tableNames.permanentBundle, (t) => {
      t.integer(columnNames.transactionByteCount).nullable();
      t.integer(columnNames.headerByteCount).nullable();
      t.integer(columnNames.payloadByteCount).nullable();
    });

    logger.info("Finished preserve signature type migration!", {
      migrationMs: Date.now() - migrationStartTime,
    });
  }

  private async rollbackFromPreserveSigType() {
    logger.info("Starting preserve signature type rollback...");
    const rollbackStartTime = Date.now();

    await this.pg.schema.alterTable(tableNames.newDataItem, (t) => {
      t.dropColumn(columnNames.dataStart);
      t.dropColumn(columnNames.signatureType);
      t.dropColumn(columnNames.failedBundles);
    });
    await this.pg.schema.alterTable(tableNames.plannedDataItem, (t) => {
      t.dropColumn(columnNames.dataStart);
      t.dropColumn(columnNames.signatureType);
      t.dropColumn(columnNames.failedBundles);
    });
    await this.pg.schema.alterTable(tableNames.permanentDataItem, (t) => {
      t.dropColumn(columnNames.dataStart);
      t.dropColumn(columnNames.signatureType);
      t.dropColumn(columnNames.failedBundles);
    });

    await this.pg.schema.alterTable(tableNames.newBundle, (t) => {
      t.dropColumn(columnNames.transactionByteCount);
      t.dropColumn(columnNames.headerByteCount);
      t.dropColumn(columnNames.payloadByteCount);
    });
    await this.pg.schema.alterTable(tableNames.postedBundle, (t) => {
      t.dropColumn(columnNames.transactionByteCount);
      t.dropColumn(columnNames.headerByteCount);
      t.dropColumn(columnNames.payloadByteCount);
    });
    await this.pg.schema.alterTable(tableNames.seededBundle, (t) => {
      t.dropColumn(columnNames.transactionByteCount);
      t.dropColumn(columnNames.headerByteCount);
      t.dropColumn(columnNames.payloadByteCount);
    });
    await this.pg.schema.alterTable(tableNames.failedBundle, (t) => {
      t.dropColumn(columnNames.transactionByteCount);
      t.dropColumn(columnNames.headerByteCount);
      t.dropColumn(columnNames.payloadByteCount);
      t.dropColumn(columnNames.failedReason);
    });
    await this.pg.schema.alterTable(tableNames.permanentBundle, (t) => {
      t.dropColumn(columnNames.transactionByteCount);
      t.dropColumn(columnNames.headerByteCount);
      t.dropColumn(columnNames.payloadByteCount);
    });

    logger.info("Finished preserve signature type rollback!", {
      rollbackMs: Date.now() - rollbackStartTime,
    });
  }

  private async createNewDataItemTable(): Promise<void> {
    return this.pg.schema.createTable(newDataItem, (t) => {
      t.string(dataItemId, 43).primary().notNullable();
      t.string(owner, 43).notNullable();
      t.string(byteCount).notNullable();
      t.timestamp(uploadedDate, this.noTimeZone)
        .defaultTo(this.defaultTimestamp())
        .notNullable();
      t.string(winstonPrice).notNullable();
    });
  }

  private async createBundlePlanTable(): Promise<void> {
    return this.pg.schema.createTable(bundlePlan, (t) => {
      t.string(planId).primary();
      t.timestamp(plannedDate, this.noTimeZone)
        .defaultTo(this.defaultTimestamp())
        .notNullable();
    });
  }

  private async createPlannedDataItemTable(): Promise<void> {
    return this.pg.schema.createTableLike(plannedDataItem, newDataItem, (t) => {
      t.string(planId).notNullable();
      t.timestamp(plannedDate, this.noTimeZone)
        .defaultTo(this.defaultTimestamp())
        .notNullable();
    });
  }

  private async createPermanentDataItemTable(): Promise<void> {
    return this.pg.schema.createTableLike(
      permanentDataItem,
      plannedDataItem,
      (t) => {
        t.string(bundleId, 43).notNullable();
        t.timestamp(permanentDate, this.noTimeZone)
          .defaultTo(this.defaultTimestamp())
          .notNullable();
      }
    );
  }

  private async createNewBundleTable(): Promise<void> {
    return this.pg.schema.createTable(newBundle, (t) => {
      t.string(bundleId, 43).primary();
      t.string(planId).notNullable();
      t.timestamp(plannedDate, this.noTimeZone)
        .defaultTo(this.defaultTimestamp())
        .notNullable();
      t.string(reward).notNullable();
      t.timestamp(signedDate, this.noTimeZone)
        .defaultTo(this.defaultTimestamp())
        .notNullable();
    });
  }

  private async createPostedBundleTable(): Promise<void> {
    return this.pg.schema.createTableLike(postedBundle, newBundle, (t) => {
      t.timestamp(postedDate, this.noTimeZone)
        .defaultTo(this.defaultTimestamp())
        .notNullable();
    });
  }

  private async createSeededBundleTable(): Promise<void> {
    return this.pg.schema.createTableLike(seededBundle, postedBundle, (t) => {
      t.timestamp(seededDate, this.noTimeZone)
        .defaultTo(this.defaultTimestamp())
        .notNullable();
    });
  }

  private async createFailedBundleTable(): Promise<void> {
    return this.pg.schema.createTableLike(failedBundle, seededBundle, (t) => {
      t.timestamp(failedDate, this.noTimeZone)
        .defaultTo(this.defaultTimestamp())
        .notNullable();
    });
  }

  private async createPermanentBundleTable(): Promise<void> {
    return this.pg.schema.createTableLike(
      permanentBundle,
      seededBundle,
      (t) => {
        t.timestamp(permanentDate, this.noTimeZone)
          .defaultTo(this.defaultTimestamp())
          .notNullable();
      }
    );
  }

  private async createSeedResultTable(): Promise<void> {
    return this.pg.schema.createTable(seedResult, (t) => {
      t.string(bundleId, 43).notNullable();
      t.string(blockHeight).nullable();
      t.string(seedResultStatus).notNullable().defaultTo("posted");
    });
  }

  private async migrateToAddARtoUSDConversionRates(): Promise<void> {
    logger.info("Starting add AR to USD conversion rates migration...");
    const migrationStartTime = Date.now();

    // create column on bundle tables for AR/USD conversion rate - allow nullable on initial migration
    return this.pg.transaction(async (transaction) => {
      await transaction.schema.alterTable(tableNames.postedBundle, (t) => {
        t.decimal(columnNames.usdToArRate).nullable();
      });
      await transaction.schema.alterTable(tableNames.seededBundle, (t) => {
        t.decimal(columnNames.usdToArRate).nullable();
      });
      await transaction.schema.alterTable(tableNames.permanentBundle, (t) => {
        t.decimal(columnNames.usdToArRate).nullable();
      });
      await transaction.schema.alterTable(tableNames.failedBundle, (t) => {
        t.decimal(columnNames.usdToArRate).nullable();
      });
      logger.info("Finished add AR to USD conversion rates migration!", {
        migrationMs: Date.now() - migrationStartTime,
      });
    });
  }

  private async rollbackFromAddARtoUSDConversionRates(): Promise<void> {
    logger.info("Starting add AR to USD conversion rates migration...");
    const rollbackStartTime = Date.now();

    // drop columns in relevant tables
    return this.pg.transaction(async (transaction) => {
      await transaction.schema.alterTable(tableNames.postedBundle, (t) => {
        t.dropColumn(columnNames.usdToArRate);
      });
      await transaction.schema.alterTable(tableNames.seededBundle, (t) => {
        t.dropColumn(columnNames.usdToArRate);
      });
      await transaction.schema.alterTable(tableNames.permanentBundle, (t) => {
        t.dropColumn(columnNames.usdToArRate);
      });
      await transaction.schema.alterTable(tableNames.failedBundle, (t) => {
        t.dropColumn(columnNames.usdToArRate);
      });
      logger.info("Finished add AR to USD conversion rates rollback!", {
        rollbackMs: Date.now() - rollbackStartTime,
      });
    });
  }

  private async migrateToByteCountBundleColumns(): Promise<void> {
    logger.info(
      "Starting bigInteger column conversion for bundle tables migration..."
    );
    const migrationStartTime = Date.now();

    await this.pg.schema.alterTable(tableNames.newBundle, (t) => {
      t.bigInteger(columnNames.transactionByteCount).alter();
      t.bigInteger(columnNames.headerByteCount).alter();
      t.bigInteger(columnNames.payloadByteCount).alter();
    });
    await this.pg.schema.alterTable(tableNames.postedBundle, (t) => {
      t.bigInteger(columnNames.transactionByteCount).alter();
      t.bigInteger(columnNames.headerByteCount).alter();
      t.bigInteger(columnNames.payloadByteCount).alter();
    });
    await this.pg.schema.alterTable(tableNames.seededBundle, (t) => {
      t.bigInteger(columnNames.transactionByteCount).alter();
      t.bigInteger(columnNames.headerByteCount).alter();
      t.bigInteger(columnNames.payloadByteCount).alter();
    });
    await this.pg.schema.alterTable(tableNames.failedBundle, (t) => {
      t.bigInteger(columnNames.transactionByteCount).alter();
      t.bigInteger(columnNames.headerByteCount).alter();
      t.bigInteger(columnNames.payloadByteCount).alter();
    });
    await this.pg.schema.alterTable(tableNames.permanentBundle, (t) => {
      t.bigInteger(columnNames.transactionByteCount).alter();
      t.bigInteger(columnNames.headerByteCount).alter();
      t.bigInteger(columnNames.payloadByteCount).alter();
    });

    logger.info(
      "Finished bigInteger column conversion for bundle tables migration!",
      {
        migrationMs: Date.now() - migrationStartTime,
      }
    );
  }

  public async rollbackFromByteCountBundleColumns(): Promise<void> {
    logger.info(
      "Starting bigInteger column conversion for bundle tables rollback..."
    );
    const rollbackStartTime = Date.now();

    await this.pg.schema.alterTable(tableNames.newBundle, (t) => {
      t.integer(columnNames.transactionByteCount).alter();
      t.integer(columnNames.headerByteCount).alter();
      t.integer(columnNames.payloadByteCount).alter();
    });
    await this.pg.schema.alterTable(tableNames.postedBundle, (t) => {
      t.integer(columnNames.transactionByteCount).alter();
      t.integer(columnNames.headerByteCount).alter();
      t.integer(columnNames.payloadByteCount).alter();
    });
    await this.pg.schema.alterTable(tableNames.seededBundle, (t) => {
      t.integer(columnNames.transactionByteCount).alter();
      t.integer(columnNames.headerByteCount).alter();
      t.integer(columnNames.payloadByteCount).alter();
    });
    await this.pg.schema.alterTable(tableNames.failedBundle, (t) => {
      t.integer(columnNames.transactionByteCount).alter();
      t.integer(columnNames.headerByteCount).alter();
      t.integer(columnNames.payloadByteCount).alter();
    });
    await this.pg.schema.alterTable(tableNames.permanentBundle, (t) => {
      t.integer(columnNames.transactionByteCount).alter();
      t.integer(columnNames.headerByteCount).alter();
      t.integer(columnNames.payloadByteCount).alter();
    });

    logger.info(
      "Finished bigInteger column conversion for bundle tables rollback!",
      {
        migrationMs: Date.now() - rollbackStartTime,
      }
    );
  }

  private defaultTimestamp() {
    return this.pg.fn.now();
  }

  private noTimeZone = { useTz: false };
}
