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
import knex, { Knex } from "knex";
import pLimit from "p-limit";
import path from "path";
import winston from "winston";

import {
  batchingSize,
  failedReasons,
  maxDataItemsPerBundle,
} from "../../constants";
import logger from "../../logger";
import {
  BundlePlanDBResult,
  DataItemDbResults,
  FailedBundleDbInsert,
  FinishedMultiPartUpload,
  FinishedMultiPartUploadDBInsert,
  FinishedMultiPartUploadDBResult,
  InFlightMultiPartUpload,
  InFlightMultiPartUploadDBInsert,
  InFlightMultiPartUploadDBResult,
  InFlightMultiPartUploadParams,
  InsertNewBundleParams,
  MultipartUploadFailedReason,
  NewBundle,
  NewBundleDBInsert,
  NewBundleDBResult,
  NewDataItem,
  NewDataItemDBInsert,
  NewDataItemDBResult,
  PermanentBundleDBResult,
  PermanentBundleDbInsert,
  PermanentDataItemDBInsert,
  PermanentDataItemDBResult,
  PlanId,
  PlannedDataItem,
  PlannedDataItemDBInsert,
  PlannedDataItemDBResult,
  PostedBundle,
  PostedBundleDBResult,
  PostedNewDataItem,
  RePackDataItemDbInsert,
  SeededBundle,
  SeededBundleDBResult,
} from "../../types/dbTypes";
import { TransactionId, UploadId, W, Winston } from "../../types/types";
import { generateArrayChunks } from "../../utils/common";
import {
  BundlePlanExistsInAnotherStateWarning,
  DataItemExistsWarning,
  MultiPartUploadNotFound,
  PostgresError,
  postgresInsertFailedPrimaryKeyNotUniqueCode,
  postgresTableRowsLockedUniqueCode,
} from "../../utils/errors";
import { Database, UpdateDataItemsToPermanentParams } from "./database";
import { columnNames, tableNames } from "./dbConstants";
import {
  newBundleDbResultToNewBundleMap,
  newDataItemDbResultToNewDataItemMap,
  plannedDataItemDbResultToPlannedDataItemMap,
  postedBundleDbResultToPostedBundleMap,
  seededBundleDbResultToSeededBundleMap,
} from "./dbMaps";
import { getReaderConfig, getWriterConfig } from "./knexConfig";

export class PostgresDatabase implements Database {
  private log: winston.Logger;
  private reader: Knex;
  private writer: Knex;

  constructor({
    writer = knex(getWriterConfig()),
    reader = knex(getReaderConfig()),
    // TODO: add tracer for spans
    migrate = false,
  }: {
    writer?: Knex;
    reader?: Knex;
    migrate?: boolean;
  } = {}) {
    this.log = logger.child({ class: this.constructor.name });
    this.writer = writer;
    this.reader = reader;
    if (migrate) {
      this.log.info("Migrating database...");
      // for testing purposes
      this.writer.migrate
        .latest({ directory: path.join(__dirname, "../../migrations") })
        .then(() => this.log.info("Database migration complete."))
        .catch((error) => this.log.error("Failed to migrate database!", error));
    }
  }

  public async insertNewDataItem(
    newDataItem: PostedNewDataItem
  ): Promise<void> {
    const { signature, ...restOfNewDataItem } = newDataItem;
    this.log.debug("Inserting new data item...", {
      dataItem: restOfNewDataItem,
    });

    if (await this.dataItemExists(newDataItem.dataItemId)) {
      throw new DataItemExistsWarning(newDataItem.dataItemId);
    }

    try {
      await this.writer(tableNames.newDataItem).insert(
        this.newDataItemToDbInsert(newDataItem)
      );
    } catch (error) {
      if (
        // Catch race conditions of new_data_item primary key (dataItemId) on insert and throw as DataItemExistsWarning
        (error as PostgresError).code ===
        postgresInsertFailedPrimaryKeyNotUniqueCode
      ) {
        throw new DataItemExistsWarning(newDataItem.dataItemId);
      }

      // Log and re throw other unknown errors on insert
      this.log.error("Data Item Insert Failed: ", { error });
      throw error;
    }

    return;
  }

  private dataItemTables = [
    tableNames.newDataItem,
    tableNames.plannedDataItem,
    tableNames.permanentDataItem,
    // TODO: tableNames.failedDataItem,
  ] as const;

  private async getDataItemsDbResultsById(
    dataItemIds: TransactionId[]
  ): Promise<DataItemDbResults[]> {
    return this.reader.transaction(async (knexTransaction) => {
      const dataItemResults = await Promise.all(
        this.dataItemTables.map((tableName) =>
          knexTransaction(tableName).whereIn(
            columnNames.dataItemId,
            dataItemIds
          )
        )
      );

      return dataItemResults.flat();
    });
  }

  public async insertNewDataItemBatch(
    dataItemBatch: PostedNewDataItem[]
  ): Promise<void> {
    this.log.debug("Inserting new data item batch...", {
      dataItemBatch,
    });

    // Check if any data items already exist in the database
    const existingDataItemDbResults = await this.getDataItemsDbResultsById(
      dataItemBatch.map((newDataItem) => newDataItem.dataItemId)
    );
    if (existingDataItemDbResults.length > 0) {
      const existingDataItemIds = new Set<TransactionId>(
        existingDataItemDbResults.map((r) => r.data_item_id)
      );

      this.log.warn(
        "Data items already exist in database! Removing from batch insert...",
        {
          existingDataItemIds,
        }
      );

      dataItemBatch = dataItemBatch.filter(
        (newDataItem) => !existingDataItemIds.has(newDataItem.dataItemId)
      );
    }

    // Insert new data items
    const dataItemInserts = dataItemBatch.map((newDataItem) =>
      this.newDataItemToDbInsert(newDataItem)
    );
    await this.writer.batchInsert<NewDataItemDBInsert, NewDataItemDBResult>(
      tableNames.newDataItem,
      dataItemInserts
    );
  }

  private async dataItemExists(data_item_id: TransactionId): Promise<boolean> {
    return this.reader.transaction(async (knexTransaction) => {
      const dataItemResults = await Promise.all([
        knexTransaction<NewDataItemDBResult>(tableNames.newDataItem).where({
          data_item_id,
        }),
        knexTransaction<PlannedDataItemDBResult>(
          tableNames.plannedDataItem
        ).where({
          data_item_id,
        }),
        knexTransaction<PermanentDataItemDBResult>(
          tableNames.permanentDataItem
        ).where({
          data_item_id,
        }),
      ]);

      for (const result of dataItemResults) {
        if (result.length > 0) {
          return true;
        }
      }
      return false;
    });
  }

  private newDataItemToDbInsert({
    assessedWinstonPrice,
    byteCount,
    dataItemId,
    ownerPublicAddress,
    payloadDataStart,
    signatureType,
    failedBundles,
    uploadedDate,
    payloadContentType,
    premiumFeatureType,
    signature,
    deadlineHeight,
  }: PostedNewDataItem): NewDataItemDBInsert {
    return {
      assessed_winston_price: assessedWinstonPrice.toString(),
      byte_count: byteCount.toString(),
      data_item_id: dataItemId,
      owner_public_address: ownerPublicAddress,
      data_start: payloadDataStart,
      failed_bundles: failedBundles.length > 0 ? failedBundles.join(",") : "",
      signature_type: signatureType,
      uploaded_date: uploadedDate,
      content_type: payloadContentType,
      premium_feature_type: premiumFeatureType,
      signature,
      deadline_height: deadlineHeight?.toString(),
    };
  }

  public async getNewDataItems(): Promise<NewDataItem[]> {
    this.log.debug("Getting new data items from database...");

    try {
      /**
       * Note: Locking will only occur for the duration of this query, it will be released
       * once the query completes.
       */
      // Using a raw query here due to the db driver's behavior of returning uploaded_date in the "wrong" UTC timezone
      const fetchStartTimestamp = Date.now();
      const dbResult: (NewDataItemDBResult & { uploaded_date_utc: string })[] =
        (
          (await this.writer.raw(
            `SELECT *, uploaded_date AT TIME ZONE 'UTC' as uploaded_date_utc
              FROM ${tableNames.newDataItem}
              ORDER BY uploaded_date
              LIMIT ${maxDataItemsPerBundle * 5}
              FOR UPDATE
              NOWAIT
            `
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
          )) as any
        ).rows;
      dbResult.forEach((result) => {
        result.uploaded_date = result.uploaded_date_utc;
      });
      const durationMs = Date.now() - fetchStartTimestamp;
      this.log.info(`Fetched new data items from database.`, {
        count: dbResult.length,
        durationMs,
        msPerRow: durationMs / dbResult.length,
      });
      return dbResult.map(newDataItemDbResultToNewDataItemMap);
    } catch (error) {
      if ((error as PostgresError).code === postgresTableRowsLockedUniqueCode) {
        this.log.warn("Table rows are locked by another execution...skipping");
        return [];
      }
      this.log.error("Failed to fetch new data items from database.", {
        error,
      });
      throw error;
    }
  }

  public async insertBundlePlan(
    planId: PlanId,
    dataItemIds: TransactionId[]
  ): Promise<void> {
    this.log.debug("Inserting bundle plan...", {
      planId,
      dataItemIds,
    });

    const dataItemIdBatches = [
      ...generateArrayChunks<TransactionId>(dataItemIds, batchingSize),
    ];

    const { planned_date } = (
      await this.writer<BundlePlanDBResult>(tableNames.bundlePlan)
        .insert({ plan_id: planId })
        .returning("planned_date")
    )[0];

    let encounteredEmptyOrLockedDataItem = false;

    try {
      logger.debug(
        `Batch moving ${dataItemIdBatches.length} batches of ${batchingSize} or less data items from ${tableNames.newDataItem} table to  ${tableNames.plannedDataItem} table...`
      );
      let batchNumber = 1;
      for (const dataItemIds of dataItemIdBatches) {
        logger.debug(
          `Moving batch ${batchNumber} of ${dataItemIdBatches.length} from ${tableNames.newDataItem} table to ${tableNames.plannedDataItem} table...`
        );
        await this.writer.transaction(async (knexTransaction) => {
          const deletedDataItems = await knexTransaction<NewDataItemDBResult>(
            tableNames.newDataItem
          )
            .whereIn("data_item_id", dataItemIds)
            .forUpdate()
            .noWait()
            .del()
            .returning("*");

          const dbInserts: PlannedDataItemDBInsert[] = deletedDataItems.map(
            (deletedDataItem) => ({
              ...deletedDataItem,
              plan_id: planId,
              planned_date,
            })
          );

          await knexTransaction.batchInsert<
            PlannedDataItemDBInsert,
            PlannedDataItemDBResult
          >(tableNames.plannedDataItem, dbInserts);
        });

        logger.debug(
          `Finished moving batch ${batchNumber++} of ${
            dataItemIdBatches.length
          } from ${tableNames.newDataItem} table to ${
            tableNames.plannedDataItem
          } table...`
        );
      }
    } catch (error) {
      if ((error as PostgresError).code === postgresTableRowsLockedUniqueCode) {
        this.log.warn("Data items are locked by another execution...skipping");
        encounteredEmptyOrLockedDataItem = true;
      }
      throw error;
    }

    // Confirm there are actually data items in the bundled plan, remove if not
    if (encounteredEmptyOrLockedDataItem) {
      const bundledDataItems = await this.reader(
        tableNames.plannedDataItem
      ).where({ plan_id: planId });

      if (!bundledDataItems.length) {
        this.log.warn("No data items in bundle plan, removing...", {
          planId,
        });
        // remove empty bundle plan immediately so it doesn't get shared
        await this.writer(tableNames.bundlePlan)
          .where({ plan_id: planId })
          .del();
      }
    }
  }

  public async getPlannedDataItemsForPlanId(
    planId: PlanId
  ): Promise<PlannedDataItem[]> {
    this.log.debug("Getting planned data items from database...", { planId });

    // Check if bundle plan still exists before getting planned data items
    const bundlePlanDbResult = await this.reader<BundlePlanDBResult>(
      tableNames.bundlePlan
    ).where({ plan_id: planId });
    if (bundlePlanDbResult.length === 0) {
      logger.warn(
        `[DUPLICATE-MESSAGE] No bundle plan still exists for plan id!`
      );
      return [];
    }

    return this.getPlannedDataItemsByPlanId(planId);
  }

  private async getPlannedDataItemsByPlanId(
    planId: PlanId
  ): Promise<PlannedDataItem[]> {
    const plannedDataItemDbResult = await this.reader<PlannedDataItemDBResult>(
      tableNames.plannedDataItem
    ).where({
      plan_id: planId,
    });

    return plannedDataItemDbResult.map(
      plannedDataItemDbResultToPlannedDataItemMap
    );
  }

  public getPlannedDataItemsForVerification(
    planId: PlanId
  ): Promise<PlannedDataItem[]> {
    this.log.debug("Getting planned data items for verification...", {
      planId,
    });

    return this.getPlannedDataItemsByPlanId(planId);
  }

  public insertNewBundle({
    bundleId,
    planId,
    reward,
    headerByteCount,
    payloadByteCount,
    transactionByteCount,
  }: InsertNewBundleParams): Promise<void> {
    this.log.debug("Inserting new bundle...", {
      bundleId,
      planId,
      reward: reward.toString(),
    });

    return this.writer.transaction(async (knexTransaction) => {
      const bundlePlanDbResults = await knexTransaction<BundlePlanDBResult>(
        tableNames.bundlePlan
      )
        .where({ plan_id: planId })
        .forUpdate() // lock row
        .noWait() // don't wait for fetching locked row, throws errors
        .del() // once it is deleted, it can't be included in another bundle
        .returning("*");

      if (bundlePlanDbResults.length === 0) {
        // If no bundle plan is found, check if plan id exists in another table
        logger.warn(
          "No bundle plan found! Checking other tables for plan id...",
          { planId, bundleId }
        );
        const bundlePlanResults = await Promise.all([
          knexTransaction<NewBundleDBResult>(tableNames.newBundle).where({
            plan_id: planId,
          }),
          knexTransaction<PostedBundleDBResult>(tableNames.postedBundle).where({
            plan_id: planId,
          }),
          knexTransaction<SeededBundleDBResult>(tableNames.seededBundle).where({
            plan_id: planId,
          }),
          knexTransaction<PermanentBundleDBResult>(
            tableNames.permanentBundle
          ).where({
            plan_id: planId,
          }),
        ]);
        if (
          bundlePlanResults.some((bundlePlanResult) => bundlePlanResult.length)
        ) {
          throw new BundlePlanExistsInAnotherStateWarning(planId, bundleId);
        } else {
          throw Error(`No bundle plan found for plan id ${planId}!`);
        }
      }

      const newBundleInsert: NewBundleDBInsert = {
        bundle_id: bundleId,
        plan_id: planId,
        planned_date: bundlePlanDbResults[0].planned_date,
        reward: reward.toString(),
        header_byte_count: headerByteCount.toString(),
        payload_byte_count: payloadByteCount.toString(),
        transaction_byte_count: transactionByteCount.toString(),
      };

      await knexTransaction(tableNames.newBundle).insert(newBundleInsert);
    });
  }

  public async getNextBundleToPostByPlanId(planId: PlanId): Promise<NewBundle> {
    this.log.debug("Getting new_bundle from database...", { planId });

    const newBundleDbResult = await this.writer<NewBundleDBResult>(
      tableNames.newBundle
    ).where(columnNames.planId, planId);

    if (newBundleDbResult.length === 0) {
      throw Error(`No new_bundle found for plan id ${planId}!`);
    }

    return newBundleDbResultToNewBundleMap(newBundleDbResult[0]);
  }

  public insertPostedBundle({
    bundleId,
    usdToArRate,
  }: {
    bundleId: TransactionId;
    usdToArRate?: number;
  }): Promise<void> {
    this.log.debug("Inserting posted bundle...", {
      bundleId,
      usdToArRate,
    });

    return this.writer.transaction(async (tx) => {
      const newBundleDbResult = (
        await tx<NewBundleDBResult>(tableNames.newBundle)
          .where({ bundle_id: bundleId })
          .del()
          .returning("*")
      )[0];

      // append USD/AR conversion rate for accounting purposes
      await tx(tableNames.postedBundle).insert({
        ...newBundleDbResult,
        usd_to_ar_rate: usdToArRate,
      });
    });
  }

  public async getNextBundleAndDataItemsToSeedByPlanId(
    planId: PlanId
  ): Promise<{
    bundleToSeed: PostedBundle;
    dataItemsToSeed: PlannedDataItem[];
  }> {
    this.log.debug("Getting posted bundle from database...", { planId });

    const postedBundleDbResult = await this.writer<PostedBundleDBResult>(
      tableNames.postedBundle
    ).where({ plan_id: planId });

    if (postedBundleDbResult.length === 0) {
      throw Error(`No posted_bundle found for plan id ${planId}!`);
    }

    const plannedDataItemDbResults = await this.writer<PlannedDataItemDBResult>(
      tableNames.plannedDataItem
    ).where({ plan_id: planId });

    return {
      bundleToSeed: postedBundleDbResultToPostedBundleMap(
        postedBundleDbResult[0]
      ),
      dataItemsToSeed: plannedDataItemDbResults.map(
        plannedDataItemDbResultToPlannedDataItemMap
      ),
    };
  }

  public insertSeededBundle(bundleId: TransactionId): Promise<void> {
    this.log.debug("Inserting seeded bundle with ID: ", { bundleId });

    return this.writer.transaction(async (knexTransaction) => {
      const postedBundleDbResult = (
        await knexTransaction<PostedBundleDBResult>(tableNames.postedBundle)
          .where({ bundle_id: bundleId })
          .del()
          .returning("*")
      )[0];

      return knexTransaction(tableNames.seededBundle).insert(
        postedBundleDbResult
      );
    });
  }

  public async getSeededBundles(limit = 50): Promise<SeededBundle[]> {
    this.log.debug("Getting seeded bundles from database...", {
      limit,
    });

    try {
      const seededResultDbResult = await this.writer<SeededBundleDBResult>(
        tableNames.seededBundle
      )
        .orderBy(columnNames.postedDate)
        .limit(limit)
        .forUpdate() // locks relevant rows
        .noWait(); // don't wait for any rows to come unlocked, this will throw on errors

      if (seededResultDbResult.length === 0) {
        return [];
      }

      return seededResultDbResult.map(seededBundleDbResultToSeededBundleMap);
    } catch (error) {
      if ((error as PostgresError).code === postgresTableRowsLockedUniqueCode) {
        this.log.warn("Table rows are locked by another execution...skipping");
        return [];
      }
      this.log.error("Failed to fetch seeded results from database.", {
        error,
      });
      throw error;
    }
  }

  public async updateBundleAsPermanent(
    planId: string,
    blockHeight: number,
    indexedOnGQL: boolean
  ): Promise<void> {
    await this.writer.transaction(async (dbTx) => {
      // Delete the seeded bundle entry
      const seededBundleDbResult = (
        await dbTx<SeededBundleDBResult>(tableNames.seededBundle)
          .where({ plan_id: planId })
          .del()
          .returning("*")
      )[0];

      // Insert permanent bundle entry
      await dbTx(tableNames.permanentBundle).insert<PermanentBundleDbInsert>({
        ...seededBundleDbResult,
        indexed_on_gql: indexedOnGQL,
        block_height: blockHeight,
      });
    });
  }

  public async updateDataItemsAsPermanent({
    dataItemIds,
    blockHeight,
    bundleId,
  }: UpdateDataItemsToPermanentParams): Promise<void> {
    if (dataItemIds.length > batchingSize) {
      throw Error(
        `This method expects ${batchingSize} data items at a time! Please batch those data items up`
      );
    }

    return this.writer.transaction(async (dbTx) => {
      const dataItems = await dbTx<PlannedDataItemDBResult>(
        tableNames.plannedDataItem
      )
        .whereIn(columnNames.dataItemId, dataItemIds)
        .del()
        .returning("*");

      const permanentDataItemInserts: PermanentDataItemDBInsert[] =
        dataItems.map(({ signature: _, ...restOfPlannedDataItem }) => ({
          ...restOfPlannedDataItem,
          block_height: blockHeight.toString(),
          bundle_id: bundleId,
        }));

      await dbTx.batchInsert<PermanentDataItemDBResult>(
        tableNames.permanentDataItem,
        permanentDataItemInserts
      );
    });
  }

  public async updateDataItemsToBeRePacked(
    dataItemIds: TransactionId[],
    failedBundleId: TransactionId
  ): Promise<void> {
    if (dataItemIds.length > batchingSize) {
      throw Error(
        `This method expects ${batchingSize} data items at a time! Please batch those data items up`
      );
    }

    this.log.info("Updating data items to be re packed...", {
      dataItemIds,
      failedBundleId,
    });

    return this.writer.transaction(async (knexTransaction) => {
      const deletedDataItems = await knexTransaction<PlannedDataItemDBResult>(
        tableNames.plannedDataItem
      )
        .whereIn("data_item_id", dataItemIds)
        .del()
        .returning("*");

      const dbInserts: RePackDataItemDbInsert[] = deletedDataItems.map(
        ({ plan_id: _pi, planned_date: _pd, ...restOfDataItem }) => ({
          ...restOfDataItem,
          failed_bundles: [
            ...(restOfDataItem.failed_bundles
              ? restOfDataItem.failed_bundles.split(",")
              : []),
            failedBundleId,
          ].join(","),
        })
      );

      await knexTransaction.batchInsert<
        RePackDataItemDbInsert,
        NewDataItemDBResult
      >(tableNames.newDataItem, dbInserts);
    });
  }

  public async updateSeededBundleToDropped(
    planId: PlanId,
    bundleId: TransactionId
  ): Promise<void> {
    await this.rePackDataItemsForPlanId(planId, bundleId);

    // Now that we've moved all the planned data items to new data items, we will delete the seeded bundle and insert as a failed bundle
    await this.writer.transaction(async (dbTx) => {
      const seededBundleDbResult = (
        await dbTx<SeededBundleDBResult>(tableNames.seededBundle)
          .where({ plan_id: planId })
          .del()
          .returning("*")
      )[0];
      await dbTx(tableNames.failedBundle).insert<FailedBundleDbInsert>({
        ...seededBundleDbResult,
        failed_reason: failedReasons.notFound,
      });
    });
  }

  // Migrates new bundle that failed the post bundle job and its planned data items to their failed and unplanned ("new") counterparts, respectively
  public async updateNewBundleToFailedToPost(
    planId: PlanId,
    bundleId: TransactionId
  ): Promise<void> {
    this.log.info("Inserting failed to post bundle...", { bundleId, planId });
    await this.rePackDataItemsForPlanId(planId, bundleId);
    await this.writer.transaction(async (dbTx) => {
      const newBundleDbResult = (
        await dbTx<NewBundleDBResult>(tableNames.newBundle)
          .where({ bundle_id: bundleId })
          .del()
          .returning("*")
      )[0];

      const failedBundleDbInsert: FailedBundleDbInsert = {
        ...newBundleDbResult,
        // Stub in planned_date for posted/seeded date as the columns are non-nullable. TODO: PE-5637 -- make these columns nullable
        posted_date: newBundleDbResult.planned_date,
        seeded_date: newBundleDbResult.planned_date,
        failed_reason: failedReasons.failedToPost,
      };

      await dbTx(tableNames.failedBundle).insert(failedBundleDbInsert);
    });
  }

  /** For a given plan Id, move data items from planned_data_item to new_data_item for repacking in plan job */
  private async rePackDataItemsForPlanId(
    planId: PlanId,
    failedBundleId: TransactionId
  ): Promise<void> {
    const plannedDataItems = await this.reader<PlannedDataItemDBResult>(
      tableNames.plannedDataItem
    ).where({ plan_id: planId });

    const newDataItemInserts = plannedDataItems.map(
      ({ plan_id: _pi, planned_date: _pd, failed_bundles, ...rest }) => {
        const failedBundlesArray = failed_bundles
          ? failed_bundles.split(",")
          : [];
        failedBundlesArray.push(failedBundleId);

        return {
          ...rest,
          failed_bundles: failedBundlesArray.join(","),
        };
      }
    );

    const rePackDataItemInsertBatches = [
      ...generateArrayChunks<RePackDataItemDbInsert>(
        newDataItemInserts,
        batchingSize
      ),
    ];
    const parallelLimit = pLimit(1);
    const transactionPromises = rePackDataItemInsertBatches.map((batch) =>
      parallelLimit(() =>
        // Each batch will insert and delete in its own atomic transaction
        this.writer.transaction(async (dbTx) => {
          await dbTx.batchInsert<NewDataItemDBResult>(
            tableNames.newDataItem,
            batch
          );
          await dbTx(tableNames.plannedDataItem)
            .whereIn(
              columnNames.dataItemId,
              batch.map((b) => b.data_item_id)
            )
            .del();
        })
      )
    );

    await Promise.all(transactionPromises);
  }

  public async getDataItemInfo(dataItemId: string): Promise<
    | {
        status: "new" | "pending" | "permanent";
        assessedWinstonPrice: Winston;
        bundleId?: string | undefined;
        uploadedTimestamp: number;
        deadlineHeight?: number;
      }
    | undefined
  > {
    this.log.debug("Getting data item info...", {
      dataItemId,
    });

    // Check for brand new data item
    const newDataItemDbResult = await this.reader<NewDataItemDBResult>(
      tableNames.newDataItem
    ).where({ data_item_id: dataItemId });
    if (newDataItemDbResult.length > 0) {
      return {
        status: "new",
        assessedWinstonPrice: W(newDataItemDbResult[0].assessed_winston_price),
        // TODO: HANDLE POSTGRES TIMEZONE ISSUE IF NECESSARY
        uploadedTimestamp: new Date(
          newDataItemDbResult[0].uploaded_date
        ).getTime(),
        deadlineHeight: newDataItemDbResult[0].deadline_height
          ? +newDataItemDbResult[0].deadline_height
          : undefined,
      };
    }

    // Check for a bundled data item that's not yet permanent
    const plannedDataItemDbResult = await this.reader<PlannedDataItemDBResult>(
      tableNames.plannedDataItem
    ).where({ data_item_id: dataItemId });
    if (plannedDataItemDbResult.length > 0) {
      const bundleDbResult = await Promise.all([
        this.reader<NewBundleDBResult>(tableNames.newBundle).where({
          plan_id: plannedDataItemDbResult[0].plan_id,
        }),
        this.reader<PostedBundleDBResult>(tableNames.postedBundle).where({
          plan_id: plannedDataItemDbResult[0].plan_id,
        }),
        this.reader<SeededBundleDBResult>(tableNames.seededBundle).where({
          plan_id: plannedDataItemDbResult[0].plan_id,
        }),
      ]).then((results) => {
        return results.flat();
      });

      const bundleId =
        bundleDbResult.length > 0 ? bundleDbResult[0].bundle_id : undefined;

      return {
        status: "pending",
        assessedWinstonPrice: W(
          plannedDataItemDbResult[0].assessed_winston_price
        ),
        bundleId,
        uploadedTimestamp: new Date(
          plannedDataItemDbResult[0].uploaded_date
        ).getTime(),
        deadlineHeight: plannedDataItemDbResult[0].deadline_height
          ? +plannedDataItemDbResult[0].deadline_height
          : undefined,
      };
    }

    // Check for a permanent data item
    const permanentDataItemDbResult =
      await this.reader<PermanentDataItemDBResult>(
        tableNames.permanentDataItem
      ).where({ data_item_id: dataItemId });
    if (permanentDataItemDbResult.length > 0) {
      return {
        status: "permanent",
        assessedWinstonPrice: W(
          permanentDataItemDbResult[0].assessed_winston_price
        ),
        bundleId: permanentDataItemDbResult[0].bundle_id,
        uploadedTimestamp: new Date(
          permanentDataItemDbResult[0].uploaded_date
        ).getTime(),
        deadlineHeight: permanentDataItemDbResult[0].deadline_height
          ? +permanentDataItemDbResult[0].deadline_height
          : undefined,
      };
    }

    // Data item not found
    return undefined;
  }

  public async getLastDataItemInBundle(
    plan_id: string
  ): Promise<PlannedDataItem> {
    this.log.debug("Getting last data item in bundle ...", {
      plan_id,
    });

    const plannedDataItemDbResult = await this.writer<PlannedDataItemDBResult>(
      tableNames.plannedDataItem
    ).where({ plan_id });
    const lastDataItemDbResult = plannedDataItemDbResult.pop();

    if (lastDataItemDbResult) {
      return plannedDataItemDbResultToPlannedDataItemMap(lastDataItemDbResult);
    } else {
      throw Error(`No data items found for plan_id :${plan_id}`);
    }
  }

  public async insertInFlightMultiPartUpload({
    uploadId,
    uploadKey,
  }: InFlightMultiPartUploadParams): Promise<void> {
    this.log.debug("Inserting in flight multipart upload...", {
      uploadId,
      uploadKey,
    });

    return this.writer.transaction(async (knexTransaction) => {
      await knexTransaction(tableNames.inFlightMultiPartUpload).insert({
        upload_id: uploadId,
        upload_key: uploadKey,
      });
    });
  }

  public async finalizeMultiPartUpload({
    dataItemId,
    etag,
    uploadId,
  }: {
    uploadId: UploadId;
    etag: string;
    dataItemId: string;
  }) {
    this.log.debug("Finalizing multipart upload...", {
      uploadId,
    });

    return this.writer.transaction(async (knexTransaction) => {
      const inFlightMultiPartUploadDbResult = (
        await knexTransaction<InFlightMultiPartUploadDBResult>(
          tableNames.inFlightMultiPartUpload
        )
          .where({ upload_id: uploadId })
          .del()
          .returning("*")
      )[0];

      if (!inFlightMultiPartUploadDbResult) {
        this.log.debug("In-flight multipart upload not found!", {
          uploadId,
        });
        throw new MultiPartUploadNotFound(uploadId);
      }

      await knexTransaction(
        tableNames.finishedMultiPartUpload
      ).insert<FinishedMultiPartUploadDBInsert>({
        ...inFlightMultiPartUploadDbResult,
        etag,
        data_item_id: dataItemId,
      });
    });
  }

  public async getInflightMultiPartUpload(
    uploadId: UploadId
  ): Promise<InFlightMultiPartUpload> {
    this.log.debug("Getting in flight multipart upload...", {
      uploadId,
    });

    const inFlightUpload = await this.reader<InFlightMultiPartUploadDBResult>(
      tableNames.inFlightMultiPartUpload
    )
      .where({ upload_id: uploadId })
      .first();

    if (!inFlightUpload) {
      this.log.debug("In-flight multipart upload not found!", {
        uploadId,
      });
      throw new MultiPartUploadNotFound(uploadId);
    }

    return {
      uploadId: inFlightUpload.upload_id,
      uploadKey: inFlightUpload.upload_key,
      createdAt: inFlightUpload.created_at,
      expiresAt: inFlightUpload.expires_at,
      chunkSize: inFlightUpload.chunk_size
        ? +inFlightUpload.chunk_size
        : undefined,
      failedReason: isMultipartUploadFailedReason(inFlightUpload.failed_reason)
        ? inFlightUpload.failed_reason
        : undefined,
    };
  }

  public async failInflightMultiPartUpload({
    uploadId,
    failedReason,
  }: {
    uploadId: UploadId;
    failedReason: MultipartUploadFailedReason;
  }): Promise<InFlightMultiPartUpload> {
    this.log.info("Failing in flight multipart upload...", {
      uploadId,
      failedReason,
    });

    return this.writer.transaction(async (knexTransaction) => {
      // begin by failing the in flight upload
      const updatedInFlightUpload = (
        await knexTransaction<InFlightMultiPartUploadDBResult>(
          tableNames.inFlightMultiPartUpload
        )
          .update({
            failed_reason: failedReason,
          })
          .where({ upload_id: uploadId })
          .returning("*")
      )[0];

      // end by cleaning up all in flight uploads that are past their expires_at date
      const numDeletedRows =
        await knexTransaction<InFlightMultiPartUploadDBResult>(
          tableNames.inFlightMultiPartUpload
        )
          .whereRaw("expires_at < NOW()")
          .del();

      this.log.info(
        `Deleted ${numDeletedRows} in flight uploads past their expired dates.`
      );

      return {
        uploadId: updatedInFlightUpload.upload_id,
        uploadKey: updatedInFlightUpload.upload_key,
        createdAt: updatedInFlightUpload.created_at,
        expiresAt: updatedInFlightUpload.expires_at,
        chunkSize: updatedInFlightUpload.chunk_size
          ? +updatedInFlightUpload.chunk_size
          : undefined,
        failedReason: isMultipartUploadFailedReason(
          updatedInFlightUpload.failed_reason
        )
          ? updatedInFlightUpload.failed_reason
          : undefined,
      };
    });
  }

  public async failFinishedMultiPartUpload({
    uploadId,
    failedReason,
  }: {
    uploadId: UploadId;
    failedReason: MultipartUploadFailedReason;
  }): Promise<FinishedMultiPartUpload> {
    this.log.info("Failing finished multipart upload...", {
      uploadId,
      failedReason,
    });

    return this.writer.transaction(async (knexTransaction) => {
      // begin by failing the finished upload
      const updatedFinishedUpload = (
        await knexTransaction<FinishedMultiPartUploadDBResult>(
          tableNames.finishedMultiPartUpload
        )
          .update({
            failed_reason: failedReason,
          })
          .where({ upload_id: uploadId })
          .returning("*")
      )[0];

      // end by cleaning up all in flight uploads that are past their expires_at date
      const numDeletedRows =
        await knexTransaction<InFlightMultiPartUploadDBResult>(
          tableNames.inFlightMultiPartUpload
        )
          .whereRaw("expires_at < NOW()")
          .del();

      this.log.info(
        `Deleted ${numDeletedRows} in flight uploads past their expired dates.`
      );

      return {
        uploadId: updatedFinishedUpload.upload_id,
        uploadKey: updatedFinishedUpload.upload_key,
        createdAt: updatedFinishedUpload.created_at,
        expiresAt: updatedFinishedUpload.expires_at,
        chunkSize: updatedFinishedUpload.chunk_size
          ? +updatedFinishedUpload.chunk_size
          : undefined,
        finalizedAt: updatedFinishedUpload.finalized_at,
        etag: updatedFinishedUpload.etag,
        dataItemId: updatedFinishedUpload.data_item_id,
        failedReason: isMultipartUploadFailedReason(
          updatedFinishedUpload.failed_reason
        )
          ? updatedFinishedUpload.failed_reason
          : undefined,
      };
    });
  }

  public async getFinalizedMultiPartUpload(
    uploadId: UploadId
  ): Promise<FinishedMultiPartUpload> {
    this.log.debug("Getting finalized multipart upload...", {
      uploadId,
    });

    const finalizedUpload = await this.reader<FinishedMultiPartUploadDBResult>(
      tableNames.finishedMultiPartUpload
    )
      .where({ upload_id: uploadId })
      .first();

    if (!finalizedUpload) {
      this.log.debug("Finalized multipart upload not found!", {
        uploadId,
      });
      throw new MultiPartUploadNotFound(uploadId);
    }

    return {
      uploadId: finalizedUpload.upload_id,
      uploadKey: finalizedUpload.upload_key,
      createdAt: finalizedUpload.created_at,
      expiresAt: finalizedUpload.expires_at,
      finalizedAt: finalizedUpload.finalized_at,
      etag: finalizedUpload.etag,
      dataItemId: finalizedUpload.data_item_id,
      failedReason: isMultipartUploadFailedReason(finalizedUpload.failed_reason)
        ? finalizedUpload.failed_reason
        : undefined,
    };
  }

  public async updateMultipartChunkSize(
    chunkSize: number,
    uploadId: UploadId
  ): Promise<void> {
    this.log.debug("Updating multipart chunk size...", {
      chunkSize,
    });

    await this.writer<InFlightMultiPartUploadDBInsert>(
      tableNames.inFlightMultiPartUpload
    )
      .update({
        chunk_size: chunkSize.toString(),
      })
      .where({ upload_id: uploadId })
      .forUpdate();
  }

  /** DEBUG tool for deleting data items that have had a catastrophic failure (e.g: deleted from S3)  */
  public async deletePlannedDataItem(dataItemId: string): Promise<void> {
    this.log.debug("Deleting planned data item...", {
      dataItemId,
    });

    const dataItem = await this.writer<PlannedDataItemDBResult>(
      tableNames.plannedDataItem
    )
      .where({ data_item_id: dataItemId })
      .del()
      .returning("*");

    logger.info("Deleted planned data item database info", { dataItem });
  }
}

function isMultipartUploadFailedReason(
  reason: string | undefined
): reason is MultipartUploadFailedReason {
  return ["INVALID", "UNDERFUNDED"].includes(reason ?? "");
}
