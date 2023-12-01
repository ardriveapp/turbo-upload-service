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

// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore
import knexConfig from "../src/arch/db/knexfile";
import { listTables } from "./helpers/dbTestHelpers";
import { stubDates } from "./stubs";
import { expectAsyncErrorThrow } from "./test_helpers";

/** Knex instance connected to a PostgreSQL database */
const knex = Knex(knexConfig);

describe("Knex connected to postgreSQL database", () => {
  it("can create and drop tables", async () => {
    const { rowCount: rowCountBefore } = await listTables(knex);
    await knex.schema.createTable("test_table", (t) => {
      t.string("test");
    });

    const { rows, rowCount } = await listTables(knex);
    expect(rowCount).to.equal(rowCountBefore + 1);
    expect(rows.length).to.equal(rowCountBefore + 1);
    expect(rows[rows.length - 1].table_name).to.equal("test_table");

    await knex.schema.dropTable("test_table");

    const { rows: rowsAfter, rowCount: rowCountAfter } = await listTables(knex);
    expect(rowCountAfter).to.equal(rowCountBefore);
    expect(rowsAfter.length).to.equal(rowCountBefore);
  });

  it("can insert, select, and delete from tables", async () => {
    const testTableName = "crud_test_table";
    const testColumnName = "crud_test_column";
    const testColumnName2 = "crud_test_column_2";

    await knex.schema.createTable(testTableName, (t) => {
      t.string(testColumnName);
      t.integer(testColumnName2);
    });

    const testInsert = {
      [testColumnName]: "stub",
      [testColumnName2]: 1337,
    };
    const testInsert2 = {
      [testColumnName]: "stub 2",
      [testColumnName2]: 1338,
    };

    // Insert
    await knex(testTableName).insert(testInsert);
    await knex(testTableName).insert(testInsert2);

    // Select one
    const testItem = await knex(testTableName).where({
      [testColumnName]: "stub",
    });
    expect(testItem).to.deep.equal([testInsert]);

    // Select *
    const testItems = await knex(testTableName);
    expect(testItems.length).to.equal(2);
    expect(testItems).to.deep.equal([testInsert, testInsert2]);

    // Delete
    await knex(testTableName)
      .where({ [testColumnName]: "stub" })
      .del();
    const testItemsAfterDel = await knex(testTableName);
    expect(testItemsAfterDel.length).to.equal(1);
    expect(testItemsAfterDel).to.deep.equal([testInsert2]);
  });

  describe("notNullable() column", async () => {
    const tableName = "not_nullable_test_table";
    const columnName = "best_test_column";
    const notNullableColumn = "not_nullable_test_column";

    before(async () => {
      await knex.schema.createTable(tableName, (t) => {
        t.string(columnName);
        t.string(notNullableColumn).notNullable();
      });
    });

    it("throws an error when inserted as null", async () => {
      await expectAsyncErrorThrow({
        promiseToError: knex(tableName).insert({
          [columnName]: "stub",
          [notNullableColumn]: null,
        }),
        errorMessage:
          'insert into "not_nullable_test_table" ("best_test_column", "not_nullable_test_column") values ($1, $2) - null value in column "not_nullable_test_column" of relation "not_nullable_test_table" violates not-null constraint',
        errorType: "error",
      });
    });

    it("inserts as expected when inserted as not null", async () => {
      expect(await knex(tableName).insert({ [notNullableColumn]: "stub" })).to
        .not.throw;

      expect((await knex(tableName))[0]).to.deep.equal({
        best_test_column: null,
        [notNullableColumn]: "stub",
      });
    });
  });

  describe("length constraints on string columns  when length constraint of a string columns is not met", async () => {
    const tableName = "string_length_test_table";
    const columnName = "string_length_test_column";

    before(async () => {
      await knex.schema.createTable(tableName, (t) => {
        t.string(columnName, 5);
      });
    });

    it("throws an error when string is OVER the maximum length", async () => {
      const stringOverMaximumLength = "123456";

      await expectAsyncErrorThrow({
        promiseToError: knex(tableName).insert({
          [columnName]: stringOverMaximumLength,
        }),
        errorMessage:
          'insert into "string_length_test_table" ("string_length_test_column") values ($1) - value too long for type character varying(5)',
        errorType: "error",
      });
    });

    it("inserts as expected when string is AT the maximum length", async () => {
      const stringAtMaximumLength = "12345";

      expect(
        await knex(tableName).insert({ [columnName]: stringAtMaximumLength })
      ).to.not.throw;
    });

    it("inserts as expected when string is BELOW the maximum length", async () => {
      const stringBelowMaximumLength = "1234";

      expect(
        await knex(tableName).insert({ [columnName]: stringBelowMaximumLength })
      ).to.not.throw;
    });
  });

  it("throws an error if tables are created twice", async () => {
    await knex.schema.createTable("Conflicting_Table", (t) => {
      t.string("1");
    });

    await expectAsyncErrorThrow({
      promiseToError: knex.schema.createTable("Conflicting_Table", (t) => {
        t.string("1");
      }),
      errorMessage:
        'create table "Conflicting_Table" ("1" varchar(255)) - relation "Conflicting_Table" already exists',
      errorType: "error",
    });
  });

  it("can create tables extended from existing tables", async () => {
    const testTableName = "extend_test_table";
    const extendedTestTableName = "extend_test_table_extended";
    const existingColumnName = "existing_column";
    const newColumnName = "new_column";

    await knex.schema.createTable(testTableName, (t) => {
      t.string(existingColumnName);
    });
    await knex.schema.createTableLike(
      extendedTestTableName,
      testTableName,
      (t) => {
        t.string(newColumnName);
      }
    );

    const stubTestExtendedTableInsert = {
      [existingColumnName]: "stub",
      [newColumnName]: "also stub",
    };
    await knex(extendedTestTableName).insert(stubTestExtendedTableInsert);

    const query = await knex(extendedTestTableName);
    expect(query.length).to.equal(1);

    expect(query[0]).to.deep.equal(stubTestExtendedTableInsert);
  });

  it("can order objects as expected using string based timestamp dates", async () => {
    const testTableName = "sorting_test_table";
    await knex.schema.createTable(testTableName, (t) => {
      t.string("id");
      t.timestamp("sorted_by").defaultTo(knex.fn.now());
    });

    await knex(testTableName).insert({
      id: "1",
      sorted_by: stubDates.middleDate,
    });
    await knex(testTableName).insert({
      id: "2",
      sorted_by: stubDates.latestDate,
    });
    await knex(testTableName).insert({
      id: "3",
      sorted_by: stubDates.earliestDate,
    });
    await knex(testTableName).insert({
      id: "4",
      // Resolve to default, which will always be the current date
      sorted_by: undefined,
    });

    const orderedQuery = await knex(testTableName).orderBy("sorted_by");
    expect(orderedQuery.length).to.equal(4);

    // We expect these big int dates to be ordered by earliest to latest
    expect(orderedQuery.map((t) => t.id)).to.deep.equal(["3", "1", "2", "4"]);
  });
});
