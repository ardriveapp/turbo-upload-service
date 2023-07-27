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
import Knex from "knex";

import { tableNames } from "../src/arch/db/dbConstants";
import * as knexConfig from "../src/arch/db/knexfile";
import { Schema } from "../src/arch/db/schema";
import {
  PermanentBundleDbInsert,
  PostedBundleDbInsert,
  SeedResultDBInsert,
  SeedResultDBResult,
  SeededBundleDbInsert,
} from "../src/types/dbTypes";
import { listTables } from "./helpers/dbTestHelpers";
import { expectedColumnInfo } from "./helpers/expectations";
import {
  stubDates,
  stubPlanId,
  stubPlanId2,
  stubPlanId3,
  stubTxId1,
  stubTxId2,
  stubTxId3,
} from "./stubs";

/** Knex instance connected to a PostgreSQL database */
const knex = Knex(knexConfig);

describe("Schema class", () => {
  before(async () => {
    // First, run the latest migrations as the knex CLI would
    await knex.migrate.latest({ directory: "migrations" });
  });

  after(async function () {
    // Run rollback and all migrations so database will be expected
    await Schema.rollback(knex);
    await Schema.create(knex);
    await Schema.migrateToVerify(knex);
    await Schema.migrateToIndexPlanIds(knex);
    await Schema.migrateToPreserveBlockHeight(knex);
    await Schema.migrateToPreserveSigType(knex);

    // Run integration tests after schema tests to avoid race conditions in the test env database

    require("./knex.spec");
    require("./postgres.spec");
    require("./router.int.spec");
    require("./prepare.spec");
    require("./arlocal.int.spec");
    require("./jobs.int.spec");
  });

  it("after running latest knex migrations, all expected tables exists", async () => {
    const allTables = await listTables(knex);

    expect(allTables.rows.map((t) => t.table_name)).to.deep.equal([
      // Tables are returned alphabetized
      "bundle_plan",
      "failed_bundle",
      "knex_migrations",
      "knex_migrations_lock",
      "new_bundle",
      "new_data_item",
      "permanent_bundle",
      "permanent_data_item",
      "planned_data_item",
      "posted_bundle",
      "seeded_bundle",
    ]);
  });

  it("created bundle_plan table has the expected column structure", async () => {
    const columnInfo = await knex("bundle_plan").columnInfo();
    expect(columnInfo).to.deep.equal({
      plan_id,
      planned_date,
    });
  });

  it("created new_bundle table has the expected column structure", async () => {
    const columnInfo = await knex("new_bundle").columnInfo();
    expect(columnInfo).to.deep.equal({
      bundle_id,
      plan_id,
      planned_date,
      reward,
      signed_date,
      header_byte_count,
      transaction_byte_count,
      payload_byte_count,
    });
  });

  it("created posted_bundle table has the expected column structure", async () => {
    const columnInfo = await knex("posted_bundle").columnInfo();
    expect(columnInfo).to.deep.equal({
      bundle_id,
      plan_id,
      planned_date,
      reward,
      signed_date,
      posted_date,
      header_byte_count,
      transaction_byte_count,
      payload_byte_count,
    });
  });

  it("created seeded_bundle table has the expected column structure", async () => {
    const columnInfo = await knex("seeded_bundle").columnInfo();
    expect(columnInfo).to.deep.equal({
      bundle_id,
      plan_id,
      planned_date,
      reward,
      signed_date,
      posted_date,
      seeded_date,
      header_byte_count,
      transaction_byte_count,
      payload_byte_count,
    });
  });

  it("created failed_bundle table has the expected column structure", async () => {
    const columnInfo = await knex("failed_bundle").columnInfo();
    expect(columnInfo).to.deep.equal({
      bundle_id,
      plan_id,
      planned_date,
      reward,
      signed_date,
      posted_date,
      seeded_date,
      failed_date,
      failed_reason,
      header_byte_count,
      transaction_byte_count,
      payload_byte_count,
    });
  });

  it("created permanent_bundle table has the expected column structure", async () => {
    const columnInfo = await knex("permanent_bundle").columnInfo();
    expect(columnInfo).to.deep.equal({
      bundle_id,
      plan_id,
      planned_date,
      reward,
      signed_date,
      block_height,
      posted_date,
      seeded_date,
      permanent_date,
      indexed_on_gql,
      header_byte_count,
      transaction_byte_count,
      payload_byte_count,
    });
  });

  it("created new_data_item table has the expected column structure", async () => {
    const columnInfo = await knex("new_data_item").columnInfo();
    expect(columnInfo).to.deep.equal({
      data_item_id,
      owner_public_address,
      byte_count,
      uploaded_date,
      assessed_winston_price,
      data_start,
      signature_type,
      failed_bundles,
    });
  });

  it("created planned_data_item table has the expected column structure", async () => {
    const columnInfo = await knex("planned_data_item").columnInfo();
    expect(columnInfo).to.deep.equal({
      data_item_id,
      owner_public_address,
      byte_count,
      uploaded_date,
      assessed_winston_price,
      plan_id,
      planned_date,
      data_start,
      signature_type,
      failed_bundles,
    });
  });

  it("created permanent_data_item table has the expected column structure", async () => {
    const columnInfo = await knex("permanent_data_item").columnInfo();
    expect(columnInfo).to.deep.equal({
      data_item_id,
      owner_public_address,
      byte_count,
      uploaded_date,
      assessed_winston_price,
      block_height,
      plan_id,
      planned_date,
      permanent_date,
      bundle_id,
      data_start,
      signature_type,
      failed_bundles,
    });
  });

  it("rollbackFull schema public static method removes all expected tables from a fully migrated database", async () => {
    await Schema.rollbackFull(knex);

    const allTables = await listTables(knex);

    expect(allTables.rows.map((t) => t.table_name)).to.deep.equal([
      "knex_migrations",
      "knex_migrations_lock",
    ]);
  });

  it("verify migration works as expected", async () => {
    await Schema.create(knex);
    const stubDate = stubDates.earliestDate;

    // Add seed_result with a seeded_bundle into the DB
    const seededBundleTxId = stubTxId1;
    const seededBundlePlanId = stubPlanId;

    const seedResultSeededInsert: Omit<SeedResultDBInsert, "plan_id"> = {
      bundle_id: seededBundleTxId,
      seed_result_status: "posted",
    };
    await knex(tableNames.seedResult).insert(seedResultSeededInsert);

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-expect-error : We has a different seeded bundle type during this migration
    const seededBundleInsert: SeededBundleDbInsert = {
      bundle_id: seededBundleTxId,
      plan_id: seededBundlePlanId,
      planned_date: stubDate,
      posted_date: stubDate,
      signed_date: stubDate,
      reward: "0",
    };
    await knex(tableNames.seededBundle).insert(seededBundleInsert);

    // Add seed_result with a posted_bundle into the DB
    const postedBundleTxId = stubTxId2;
    const postedBundlePlanId = stubPlanId2;

    const seedResultPostedInsert: Omit<SeedResultDBInsert, "plan_id"> = {
      bundle_id: postedBundleTxId,
      seed_result_status: "posted",
    };
    await knex(tableNames.seedResult).insert(seedResultPostedInsert);

    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-expect-error : We has a different posted bundle type during this migration
    const postedBundleInsert: PostedBundleDbInsert = {
      bundle_id: postedBundleTxId,
      plan_id: postedBundlePlanId,
      planned_date: stubDate,
      signed_date: stubDate,
      reward: "0",
    };
    await knex(tableNames.postedBundle).insert(postedBundleInsert);

    // Add seed_result with a permanent_bundle into the DB
    const permanentBundleTxId = stubTxId3;
    const permanentBundlePlanId = stubPlanId3;

    const seedResultPermanentInsert: Omit<SeedResultDBInsert, "plan_id"> = {
      bundle_id: permanentBundleTxId,
      seed_result_status: "permanent",
    };
    await knex(tableNames.seedResult).insert(seedResultPermanentInsert);

    const permanentBundleInsert: Omit<
      PermanentBundleDbInsert,
      "indexed_on_gql"
    > = {
      bundle_id: permanentBundleTxId,
      plan_id: permanentBundlePlanId,
      planned_date: stubDate,
      signed_date: stubDate,
      posted_date: stubDate,
      seeded_date: stubDate,
      reward: "0",
    };
    await knex(tableNames.permanentBundle).insert(permanentBundleInsert);

    await Schema.migrateToVerify(knex);

    const seededSeedResult = await knex<SeedResultDBResult>(
      tableNames.seedResult
    ).where({
      bundle_id: seededBundleTxId,
    });
    expect(seededSeedResult[0].plan_id).to.equal(seededBundlePlanId);

    const postedSeedResult = await knex<SeedResultDBResult>(
      tableNames.seedResult
    ).where({
      bundle_id: postedBundleTxId,
    });
    expect(postedSeedResult[0].plan_id).to.equal(postedBundlePlanId);

    const permanentSeedResult = await knex<SeedResultDBResult>(
      tableNames.seedResult
    ).where({
      bundle_id: permanentBundleTxId,
    });
    expect(permanentSeedResult[0].plan_id).to.equal(permanentBundlePlanId);
  });

  const selectPgIndexesRawSql =
    "select tablename, indexname, indexdef from pg_indexes where schemaname = 'public' order by tablename, indexname;";

  const explainAnalyzePlannedDataItemQueryRawSql =
    "explain analyze select * from planned_data_item where plan_id like 'e29ecd3d-7d6d-41e0-9125-cdfb8aadc730';";

  it("index plan ids migration runs without error as expected", async () => {
    const selectIndexesResultBeforeMigration = await knex.raw(
      selectPgIndexesRawSql
    );
    expect(selectIndexesResultBeforeMigration.rowCount).to.equal(12);

    const explainAnalyzeResultBeforeMigration = await knex.raw(
      explainAnalyzePlannedDataItemQueryRawSql
    );
    const queryPlanBeforeMigration = (
      Object.values(explainAnalyzeResultBeforeMigration.rows[0])[0] as string
    ).split(" ");
    expect(
      `${queryPlanBeforeMigration[0]} ${queryPlanBeforeMigration[1]}`
    ).to.equal("Seq Scan");

    await Schema.migrateToIndexPlanIds(knex);

    // Ensure we are now using an Index Scan query plan for planned_data_item lookup by plan_id
    const explainAnalyzeResult = await knex.raw(
      explainAnalyzePlannedDataItemQueryRawSql
    );
    const queryPlan = (
      Object.values(explainAnalyzeResult.rows[0])[0] as string
    ).split(" ");
    expect(`${queryPlan[0]} ${queryPlanBeforeMigration[1]}`).to.equal(
      "Index Scan"
    );

    const selectIndexesResult = await knex.raw(selectPgIndexesRawSql);
    expect(selectIndexesResult.rowCount).to.equal(19);

    // Ensure all indexes exist as expected
    expect(selectIndexesResult.rows).to.deep.include({
      tablename: "planned_data_item",
      indexname: "planned_data_item_plan_id_index",
      indexdef:
        "CREATE INDEX planned_data_item_plan_id_index ON public.planned_data_item USING btree (plan_id)",
    });
    expect(selectIndexesResult.rows).to.deep.include({
      tablename: "permanent_data_item",
      indexname: "permanent_data_item_plan_id_index",
      indexdef:
        "CREATE INDEX permanent_data_item_plan_id_index ON public.permanent_data_item USING btree (plan_id)",
    });
    expect(selectIndexesResult.rows).to.deep.include({
      tablename: "new_bundle",
      indexname: "new_bundle_plan_id_index",
      indexdef:
        "CREATE INDEX new_bundle_plan_id_index ON public.new_bundle USING btree (plan_id)",
    });
    expect(selectIndexesResult.rows).to.deep.include({
      tablename: "posted_bundle",
      indexname: "posted_bundle_plan_id_index",
      indexdef:
        "CREATE INDEX posted_bundle_plan_id_index ON public.posted_bundle USING btree (plan_id)",
    });
    expect(selectIndexesResult.rows).to.deep.include({
      tablename: "seeded_bundle",
      indexname: "seeded_bundle_plan_id_index",
      indexdef:
        "CREATE INDEX seeded_bundle_plan_id_index ON public.seeded_bundle USING btree (plan_id)",
    });
    expect(selectIndexesResult.rows).to.deep.include({
      tablename: "failed_bundle",
      indexname: "failed_bundle_plan_id_index",
      indexdef:
        "CREATE INDEX failed_bundle_plan_id_index ON public.failed_bundle USING btree (plan_id)",
    });
    expect(selectIndexesResult.rows).to.deep.include({
      tablename: "permanent_bundle",
      indexname: "permanent_bundle_plan_id_index",
      indexdef:
        "CREATE INDEX permanent_bundle_plan_id_index ON public.permanent_bundle USING btree (plan_id)",
    });
  });

  it("index plan ids rollback runs without error as expected", async () => {
    await Schema.rollbackFromIndexPlanIds(knex);

    const selectIndexesResult = await knex.raw(selectPgIndexesRawSql);
    expect(selectIndexesResult.rowCount).to.equal(12);

    // Ensure planned_data_item still has the expected shape after rollback
    const columnInfo = await knex("planned_data_item").columnInfo();
    expect(columnInfo).to.deep.equal({
      data_item_id,
      owner_public_address,
      byte_count,
      uploaded_date,
      assessed_winston_price,
      plan_id,
      planned_date,
    });
  });
});

const {
  plan_id,
  planned_date,
  data_item_id,
  bundle_id,
  reward,
  signed_date,
  posted_date,
  owner_public_address,
  byte_count,
  uploaded_date,
  assessed_winston_price,
  seeded_date,
  permanent_date,
  failed_date,
  block_height,
  indexed_on_gql,
  failed_reason,
  header_byte_count,
  payload_byte_count,
  transaction_byte_count,
  data_start,
  signature_type,
  failed_bundles,
} = expectedColumnInfo;
