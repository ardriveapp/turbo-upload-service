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
import knex, { Knex } from "knex";
import pLimit from "p-limit";
import winston from "winston";

import { failedReasons, maxDataItemLimit } from "../../constants";
import logger from "../../logger";
import {
  BundlePlanDBResult,
  FailedBundleDbInsert,
  InsertNewBundleParams,
  NewBundle,
  NewBundleDBInsert,
  NewBundleDBResult,
  NewDataItem,
  NewDataItemDBInsert,
  NewDataItemDBResult,
  PermanentBundleDbInsert,
  PermanentDataItemDBInsert,
  PermanentDataItemDBResult,
  PlanId,
  PlannedDataItem,
  PlannedDataItemDBResult,
  PostedBundle,
  PostedBundleDBResult,
  PostedNewDataItem,
  SeededBundle,
  SeededBundleDBResult,
} from "../../types/dbTypes";
import { TransactionId, W, Winston } from "../../types/types";
import { filterKeysFromObject } from "../../utils/common";
import {
  DataItemExistsWarning,
  PostgresError,
  postgresInsertFailedPrimaryKeyNotUniqueCode,
  postgresTableRowsLockedUniqueCode,
} from "../../utils/errors";
import { Database } from "./database";
import { columnNames, tableNames } from "./dbConstants";
import {
  newBundleDbResultToNewBundleMap,
  newDataItemDbResultToNewDataItemMap,
  plannedDataItemDbResultToPlannedDataItemMap,
  postedBundleDbResultToPostedBundleMap,
  seededBundleDbResultToSeededBundleMap,
} from "./dbMaps";
import { readerConfig, writerConfig } from "./knexConfig";

/** Knex instance connected to a PostgreSQL database */
const pgWriter = knex(writerConfig);
const pgReader = knex(readerConfig);

export class PostgresDatabase implements Database {
  private log: winston.Logger;

  constructor(
    private readonly writer: Knex = pgWriter,
    private readonly reader: Knex = pgReader
  ) {
    this.log = logger.child({ class: this.constructor.name });
  }

  public async insertNewDataItem(
    newDataItem: PostedNewDataItem
  ): Promise<void> {
    this.log.info("Inserting new data item...", {
      dataItem: newDataItem,
    });

    const dataItemExistsWarningMessage = `Data item with ID ${newDataItem.dataItemId} has already been uploaded to this service!`;

    if (await this.dataItemExists(newDataItem.dataItemId)) {
      throw new DataItemExistsWarning(dataItemExistsWarningMessage);
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
        throw new DataItemExistsWarning(dataItemExistsWarningMessage);
      }

      // Log and re throw other unknown errors on insert
      this.log.error("Data Item Insert Failed: ", { error });
      throw error;
    }

    return;
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
    dataStart,
    signatureType,
    failedBundles,
  }: PostedNewDataItem): NewDataItemDBInsert {
    return {
      assessed_winston_price: assessedWinstonPrice.toString(),
      byte_count: byteCount.toString(),
      data_item_id: dataItemId,
      owner_public_address: ownerPublicAddress,
      data_start: dataStart,
      failed_bundles: failedBundles.length > 0 ? failedBundles.join(",") : "",
      signature_type: signatureType,
    };
  }

  public async getNewDataItems(): Promise<NewDataItem[]> {
    this.log.info("Getting new data items from database...");

    try {
      /**
       * Note: Locking will only occur for the duration of this query, it will be released
       * once the query completes.
       */
      const dbResult = await this.writer<NewDataItemDBResult>(
        tableNames.newDataItem
      )
        .orderBy("uploaded_date")
        // Limit this getter to 5 times the amount max data items allowed in a bundle
        .limit(maxDataItemLimit * 5)
        .forUpdate() // lock rows
        .noWait(); // don't wait for fetching locked rows, throws errors

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

  public insertBundlePlan(
    planId: PlanId,
    dataItemIds: TransactionId[]
  ): Promise<void> {
    const PARALLEL_LIMIT = 10;
    const parallelLimit = pLimit(PARALLEL_LIMIT);

    this.log.info("Inserting bundle plan...", {
      planId,
      dataItemIds,
    });

    return this.writer.transaction(async (knexTransaction) => {
      let encounteredEmptyOrLockedDataItem = false;
      const { planned_date } = (
        await knexTransaction<BundlePlanDBResult>(tableNames.bundlePlan)
          .insert({ plan_id: planId })
          .returning("planned_date")
      )[0];

      const newDataItemPromises = dataItemIds.map((data_item_id) =>
        parallelLimit(async () => {
          try {
            /**
             * Delete the existing NewDataItem, deriving existing info
             * Note: 'DELETE' acquires a lock using 'FOR UPDATE', but we want to make sure we do not wait
             * to acquire the lock, so we add both 'FOR UPDATE' and 'NO WAIT' explicitly
             **/
            const [newDataItem] = await knexTransaction<NewDataItemDBResult>(
              tableNames.newDataItem
            )
              .where({ data_item_id })
              .forUpdate() // lock row while we are deleting it
              .noWait() // throw errors if unable to acquire
              .del() // once it is are deleted, it can't be included in another bundle
              .returning("*");
            if (newDataItem) {
              await knexTransaction(tableNames.plannedDataItem).insert({
                ...newDataItem,
                plan_id: planId,
                planned_date,
              });
            } else {
              // the data item has already been assigned a bundle plan and deleted
              encounteredEmptyOrLockedDataItem = true;
            }
          } catch (error) {
            if (
              (error as PostgresError).code ===
              postgresTableRowsLockedUniqueCode
            ) {
              this.log.warn(
                "Data items are locked by another execution...skipping"
              );
              encounteredEmptyOrLockedDataItem = true;
              return;
            }
            throw error;
          }
        })
      );

      await Promise.all(newDataItemPromises);

      // Confirm there are actually data items in the bundled plan, remove if not
      if (encounteredEmptyOrLockedDataItem) {
        const bundledDataItems = await knexTransaction(
          tableNames.plannedDataItem
        ).where({ plan_id: planId });

        if (!bundledDataItems.length) {
          this.log.warn("No data items in bundle plan, removing...", {
            planId,
          });
          // remove empty bundle plan immediately so it doesn't get shared
          await knexTransaction(tableNames.bundlePlan)
            .where({ plan_id: planId })
            .del();
        }
      }
    });
  }

  public async getPlannedDataItemsForPlanId(
    planId: PlanId
  ): Promise<PlannedDataItem[]> {
    this.log.info("Getting planned data items from database...", { planId });

    const plannedDataItemDbResult = await this.writer<PlannedDataItemDBResult>(
      tableNames.plannedDataItem
    ).where({
      plan_id: planId,
    });

    if (plannedDataItemDbResult.length === 0) {
      throw Error(`No planned_data_item found for plan id ${planId}!`);
    }

    return plannedDataItemDbResult.map(
      plannedDataItemDbResultToPlannedDataItemMap
    );
  }

  public insertNewBundle({
    bundleId,
    planId,
    reward,
    headerByteCount,
    payloadByteCount,
    transactionByteCount,
  }: InsertNewBundleParams): Promise<void> {
    this.log.info("Inserting new bundle...", {
      bundleId,
      planId,
      reward: reward.toString(),
    });

    return this.writer.transaction(async (tx) => {
      const { planned_date } = (
        await tx<BundlePlanDBResult>(tableNames.bundlePlan)
          .where({ plan_id: planId })
          .del()
          .returning("*")
      )[0];

      const newBundleInsert: NewBundleDBInsert = {
        bundle_id: bundleId,
        plan_id: planId,
        planned_date,
        reward: reward.toString(),
        header_byte_count: headerByteCount,
        payload_byte_count: payloadByteCount,
        transaction_byte_count: transactionByteCount,
      };

      await tx(tableNames.newBundle).insert(newBundleInsert);
    });
  }

  public async getNextBundleToPostByPlanId(planId: PlanId): Promise<NewBundle> {
    this.log.info("Getting new_bundle from database...", { planId });

    const newBundleDbResult = await this.writer<NewBundleDBResult>(
      tableNames.newBundle
    ).where(columnNames.planId, planId);

    if (newBundleDbResult.length === 0) {
      throw Error(`No new_bundle found for plan id ${planId}!`);
    }

    return newBundleDbResultToNewBundleMap(newBundleDbResult[0]);
  }

  public insertPostedBundle(bundleId: TransactionId): Promise<void> {
    this.log.info("Inserting posted bundle...", {
      bundleId,
    });

    return this.writer.transaction(async (tx) => {
      const newBundleDbResult = (
        await tx<NewBundleDBResult>(tableNames.newBundle)
          .where({ bundle_id: bundleId })
          .del()
          .returning("*")
      )[0];

      await tx(tableNames.postedBundle).insert(newBundleDbResult);
    });
  }

  public async getNextBundleAndDataItemsToSeedByPlanId(
    planId: PlanId
  ): Promise<{
    bundleToSeed: PostedBundle;
    dataItemsToSeed: PlannedDataItem[];
  }> {
    this.log.info("Getting posted bundle from database...", { planId });

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
    this.log.info("Inserting seeded bundle with ID: ", { bundleId });

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

  public async getSeededBundles(limit = 100): Promise<SeededBundle[]> {
    this.log.info("Getting seeded results from database...", {
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

      // Retrieve all the planned data items that were in the seeded bundle entry
      const plannedDataItemDbResult = await dbTx<PlannedDataItemDBResult>(
        tableNames.plannedDataItem
      ).where({ plan_id: planId });

      const promises = [
        dbTx(tableNames.permanentBundle).insert<PermanentBundleDbInsert>({
          ...seededBundleDbResult,
          indexed_on_gql: indexedOnGQL,
          block_height: blockHeight,
        }),
      ];

      for (const dataItem of plannedDataItemDbResult) {
        const data_item_id = dataItem.data_item_id;
        const permanentDataItemInsert: PermanentDataItemDBInsert = {
          ...dataItem,
          bundle_id: seededBundleDbResult.bundle_id,
          block_height: blockHeight.toString(),
        };

        promises.push(
          dbTx(tableNames.plannedDataItem).where({ data_item_id }).del(),
          dbTx(tableNames.permanentDataItem).insert(permanentDataItemInsert)
        );
      }

      await Promise.all(promises);
    });
  }

  public async updateBundleAsDropped(planId: string): Promise<void> {
    await this.writer.transaction(async (dbTx) => {
      // Delete the seeded bundle entity
      const seededBundleDeleteDbResult = (
        await dbTx<SeededBundleDBResult>(tableNames.seededBundle)
          .where({ plan_id: planId })
          .del()
          .returning("*")
      )[0];

      // Retrieve all the planned data items that were in the seeded bundle entity
      const plannedDataItemDbResult = await dbTx<PlannedDataItemDBResult>(
        tableNames.plannedDataItem
      ).where({ plan_id: planId });

      const failedBundleDbInsert: FailedBundleDbInsert = {
        ...seededBundleDeleteDbResult,
        failed_reason: failedReasons.notFound,
      };

      const promises = [
        dbTx(tableNames.failedBundle).insert(failedBundleDbInsert),
      ];

      for (const dataItem of plannedDataItemDbResult) {
        promises.push(
          dbTx(tableNames.plannedDataItem)
            .where({ data_item_id: dataItem.data_item_id })
            .del(),
          dbTx(tableNames.newDataItem).insert(
            filterKeysFromObject(dataItem, ["plan_id", "planned_date"])
          )
        );
      }

      await Promise.all(promises);
    });
  }

  // Migrates new bundle that failed the post bundle job and its planned data items to their failed and unplanned ("new") counterparts, respectively
  public async insertFailedToPostBundle(
    bundleId: TransactionId
  ): Promise<void> {
    // Delete the new bundle entity
    const newBundleDbResult = (
      await this.writer<NewBundleDBResult>(tableNames.newBundle)
        .where({ bundle_id: bundleId })
        .del()
        .returning("*")
    )[0];

    // Retrieve all the planned data items that were in the new bundle entity
    const plannedDataItemDbResult = await this.writer<PlannedDataItemDBResult>(
      tableNames.plannedDataItem
    ).where({ plan_id: newBundleDbResult.plan_id });
    const failedBundleDbInsert: FailedBundleDbInsert = {
      ...newBundleDbResult,
      seeded_date: newBundleDbResult.planned_date,
      posted_date: newBundleDbResult.planned_date,
      failed_reason: failedReasons.failedToPost,
    };

    const promises = [
      // Insert a failed bundle entity
      this.writer<FailedBundleDbInsert>(tableNames.failedBundle).insert(
        failedBundleDbInsert
      ),
    ];

    for (const dataItem of plannedDataItemDbResult) {
      promises.push(
        // Delete planned Data Item
        this.writer(tableNames.plannedDataItem)
          .where({ data_item_id: dataItem.data_item_id })
          .del(),
        // Insert new data item
        this.writer(tableNames.newDataItem).insert(
          filterKeysFromObject(dataItem, ["plan_id", "planned_date"])
        )
      );
    }

    await Promise.all(promises);
  }

  public async getDataItemInfo(dataItemId: string): Promise<
    | {
        status: "new" | "pending" | "permanent";
        assessedWinstonPrice: Winston;
        bundleId?: string | undefined;
      }
    | undefined
  > {
    this.log.info("Getting data item info...", {
      dataItemId,
    });

    const newDataItemDbResult = await this.writer<NewDataItemDBResult>(
      tableNames.newDataItem
    ).where({ data_item_id: dataItemId });
    if (newDataItemDbResult.length > 0) {
      return {
        status: "new",
        assessedWinstonPrice: W(newDataItemDbResult[0].assessed_winston_price),
      };
    }

    const plannedDataItemDbResult = await this.writer<PlannedDataItemDBResult>(
      tableNames.plannedDataItem
    ).where({ data_item_id: dataItemId });
    if (plannedDataItemDbResult.length > 0) {
      return {
        status: "pending",
        assessedWinstonPrice: W(
          plannedDataItemDbResult[0].assessed_winston_price
        ),
      };
    }

    const permanentDataItemDbResult =
      await this.writer<PermanentDataItemDBResult>(
        tableNames.permanentDataItem
      ).where({ data_item_id: dataItemId });
    if (permanentDataItemDbResult.length > 0) {
      return {
        status: "permanent",
        assessedWinstonPrice: W(
          permanentDataItemDbResult[0].assessed_winston_price
        ),
        bundleId: permanentDataItemDbResult[0].bundle_id,
      };
    }

    // Data item not found
    return undefined;
  }

  public async getLastDataItemInBundle(
    plan_id: string
  ): Promise<PlannedDataItem> {
    this.log.info("Getting last data item in bundle ...", {
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
}
