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
import { expect } from "chai";
import { Knex } from "knex";

import { tableNames } from "../../src/arch/db/dbConstants";
import { PostgresDatabase } from "../../src/arch/db/postgres";
import {
  KnexRawResult,
  NewBundleDBInsert,
  NewDataItemDBInsert,
  PermanentDataItemDBInsert,
  PlanId,
  PlannedDataItemDBInsert,
  PostedBundleDbInsert,
  SeededBundleDbInsert,
  Timestamp,
} from "../../src/types/dbTypes";
import { TransactionId } from "../../src/types/types";
import {
  stubBlockHeight,
  stubByteCount,
  stubDates,
  stubOwnerAddress,
  stubWinstonPrice,
} from "../stubs";

export function listTables(pg: Knex): Knex.Raw<KnexRawResult> {
  return pg.raw(
    "select table_name from information_schema.tables where table_schema = 'public' order by table_name"
  );
}

function stubNewDataItemInsert({
  dataItemId,
  uploadedDate = undefined,
  byte_count = stubByteCount.toString(),
}: InsertStubNewDataItemParams): NewDataItemDBInsert & {
  uploaded_date: string | undefined;
} {
  return {
    data_item_id: dataItemId,
    owner_public_address: stubOwnerAddress,
    byte_count,
    assessed_winston_price: stubWinstonPrice.toString(),
    uploaded_date: uploadedDate,
    data_start: 1500,
    signature_type: 1,
    failed_bundles: "",
  };
}

function stubPlannedDataItemInsert({
  dataItemId,
  planId,
  plannedDate = undefined,
}: InsertStubPlannedDataItemParams): PlannedDataItemDBInsert & {
  planned_date: string | undefined;
} {
  return {
    ...stubNewDataItemInsert({ dataItemId }),
    plan_id: planId,
    uploaded_date: stubDates.earliestDate,
    planned_date: plannedDate,
  };
}

function stubPermanentDataItemInsert({
  dataItemId,
  planId,
  bundleId,
}: InsertStubPermanentDataItemParams): PermanentDataItemDBInsert {
  return {
    ...stubPlannedDataItemInsert({ dataItemId, planId }),
    planned_date: stubDates.earliestDate,
    bundle_id: bundleId,
    block_height: stubBlockHeight.toString(),
  };
}

// cspell:disable
const uMguurlBundleTxByteCount = 1905; // cspell:disable

function stubNewBundleInsert({
  bundleId,
  planId,
  signedDate = undefined,
}: InsertStubNewBundleBundleParams): NewBundleDBInsert & {
  signed_date: Timestamp | undefined;
} {
  return {
    bundle_id: bundleId,
    plan_id: planId,
    planned_date: stubDates.earliestDate,
    signed_date: signedDate,
    reward: stubWinstonPrice.toString(),
    header_byte_count: stubByteCount,
    payload_byte_count: stubByteCount,
    transaction_byte_count: uMguurlBundleTxByteCount,
  };
}

function stubPostedBundleInsert({
  bundleId,
  planId,
  postedDate = undefined,
}: InsertStubPostedBundleBundleParams): PostedBundleDbInsert & {
  posted_date: Timestamp | undefined;
} {
  return {
    ...stubNewBundleInsert({ bundleId, planId }),
    signed_date: stubDates.earliestDate,
    posted_date: postedDate,
  };
}

function stubSeededBundleInsert({
  bundleId,
  planId,
  seededDate = undefined,
}: InsertStubSeededBundleParams): SeededBundleDbInsert & {
  seeded_date: Timestamp | undefined;
} {
  return {
    ...stubPostedBundleInsert({ bundleId, planId }),
    posted_date: stubDates.earliestDate,
    seeded_date: seededDate,
  };
}

export class DbTestHelper {
  constructor(public readonly db: PostgresDatabase) {}

  private get knex(): Knex {
    return this.db["writer"];
  }

  public async insertStubNewDataItem(
    insertParams: InsertStubNewDataItemParams
  ): Promise<void> {
    return this.knex(tableNames.newDataItem).insert(
      stubNewDataItemInsert(insertParams)
    );
  }

  public async insertStubPlannedDataItem(
    insertParams: InsertStubPlannedDataItemParams
  ): Promise<void> {
    return this.knex(tableNames.plannedDataItem).insert(
      stubPlannedDataItemInsert(insertParams)
    );
  }

  public async insertStubPermanentDataItem(
    insertParams: InsertStubPermanentDataItemParams
  ): Promise<void> {
    return this.knex(tableNames.permanentDataItem).insert(
      stubPermanentDataItemInsert(insertParams)
    );
  }

  public async insertStubBundlePlan({
    planId,
    dataItemIds = [],
    plannedDate,
  }: InsertStubBundlePlanParams) {
    await Promise.all([
      ...dataItemIds.map((dataItemId) =>
        this.insertStubPlannedDataItem({ dataItemId, planId, plannedDate })
      ),
      this.knex(tableNames.bundlePlan).insert({
        plan_id: planId,
        planned_date: plannedDate,
      }),
    ]);
    return;
  }

  public async insertStubNewBundle({
    bundleId,
    planId,
    signedDate,
    dataItemIds = [],
  }: InsertStubNewBundleBundleParams): Promise<void> {
    await Promise.all([
      dataItemIds.map((dataItemId) =>
        this.insertStubPlannedDataItem({ dataItemId, planId })
      ),
      this.knex(tableNames.newBundle).insert(
        stubNewBundleInsert({ bundleId, planId, signedDate })
      ),
    ]);
    return;
  }

  public async insertStubPostedBundle({
    bundleId,
    planId,
    postedDate,
    dataItemIds = [],
  }: InsertStubPostedBundleBundleParams): Promise<void> {
    await Promise.all([
      dataItemIds.map((dataItemId) =>
        this.insertStubPlannedDataItem({ dataItemId, planId })
      ),
      this.knex(tableNames.postedBundle).insert(
        stubPostedBundleInsert({ bundleId, planId, postedDate })
      ),
    ]);
    return;
  }

  public async insertStubSeededBundle({
    bundleId,
    planId,
    seededDate,
  }: InsertStubSeededBundleParams): Promise<void> {
    return this.knex(tableNames.seededBundle).insert(
      stubSeededBundleInsert({ bundleId, planId, seededDate })
    );
  }

  public async cleanUpBundlePlanInDb({
    planId,
    dataItemIds = [],
  }: {
    planId: PlanId;
    dataItemIds?: TransactionId[];
  }) {
    await Promise.all([
      ...dataItemIds.map((id) =>
        this.cleanUpEntityInDb(tableNames.plannedDataItem, id)
      ),
      this.cleanUpEntityInDb(tableNames.bundlePlan, planId),
    ]);
    return;
  }

  public async cleanUpNewBundleInDb({
    planId,
    dataItemIds,
  }: {
    planId: TransactionId;
    dataItemIds: TransactionId[];
  }) {
    await Promise.all([
      ...dataItemIds.map((id) =>
        this.cleanUpEntityInDb(tableNames.plannedDataItem, id)
      ),

      this.cleanUpEntityInDb(tableNames.newBundle, planId),
    ]);

    await this.knex(tableNames.newBundle).where({ plan_id: planId }).del();
    expect(
      (await this.knex(tableNames.newBundle).where({ plan_id: planId })).length
    ).to.equal(0);
    return;
  }

  public async cleanUpPostedBundleInDb({
    bundleId,
    dataItemIds,
  }: {
    bundleId: TransactionId;
    dataItemIds: TransactionId[];
  }) {
    await Promise.all([
      ...dataItemIds.map((id) =>
        this.cleanUpEntityInDb(tableNames.plannedDataItem, id)
      ),
      this.cleanUpEntityInDb(tableNames.postedBundle, bundleId),
    ]);
    return;
  }

  public async cleanUpSeededBundleInDb({
    bundleId,
    dataItemIds = [],
    bundleTable,
  }: {
    bundleId: TransactionId;
    dataItemIds?: TransactionId[];
    bundleTable: SeededBundleTableNames;
  }) {
    await Promise.all([
      ...dataItemIds.map((id) =>
        this.cleanUpEntityInDb(tableNames.plannedDataItem, id)
      ),
      this.cleanUpEntityInDb(bundleTable, bundleId),
    ]);
    return;
  }

  public async cleanUpEntityInDb(
    tableName: TableNameValues,
    id: TransactionId | PlanId
  ): Promise<void> {
    const dataItemTables: TableNameValues[] = [
      "new_data_item",
      "planned_data_item",
      "permanent_data_item",
    ];

    const where =
      tableName === "bundle_plan"
        ? { plan_id: id }
        : dataItemTables.includes(tableName)
        ? { data_item_id: id }
        : { bundle_id: id };

    await this.knex(tableName).where(where).del();
    expect((await this.knex(tableName).where(where)).length).to.equal(0);
  }
}

interface InsertStubNewDataItemParams {
  dataItemId: TransactionId;
  uploadedDate?: string;
  byte_count?: string;
}

interface InsertStubPlannedDataItemParams
  extends Omit<InsertStubNewDataItemParams, "uploadedDate"> {
  planId: PlanId;
  plannedDate?: string;
}

interface InsertStubPermanentDataItemParams
  extends Omit<InsertStubPlannedDataItemParams, "plannedDate"> {
  bundleId: TransactionId;
  permanentDate?: string;
}

interface InsertStubBundlePlanParams {
  planId: PlanId;
  plannedDate?: Timestamp;
  dataItemIds?: TransactionId[];
}

interface InsertStubNewBundleBundleParams
  extends Omit<InsertStubBundlePlanParams, "plannedDate"> {
  bundleId: TransactionId;
  signedDate?: Timestamp;
}

interface InsertStubPostedBundleBundleParams
  extends Omit<InsertStubNewBundleBundleParams, "signedDate"> {
  postedDate?: Timestamp;
}

interface InsertStubSeededBundleParams
  extends Omit<InsertStubPostedBundleBundleParams, "postedDate"> {
  seededDate?: Timestamp;
}

type TableNameKeys = keyof typeof tableNames;
type TableNameValues = (typeof tableNames)[TableNameKeys];

type SeededBundleTableNames =
  | typeof tableNames.seededBundle
  | typeof tableNames.failedBundle
  | typeof tableNames.permanentBundle;
