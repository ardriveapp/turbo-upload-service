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
import { AxiosResponseHeaders } from "axios";
import { expect } from "chai";

import {
  newBundleDbResultToNewBundleMap,
  newDataItemDbResultToNewDataItemMap,
  postedBundleDbResultToPostedBundleMap,
} from "../../src/arch/db/dbMaps";
import {
  FailedBundle,
  NewBundle,
  NewBundleDBResult,
  NewDataItem,
  NewDataItemDBResult,
  PermanentBundle,
  PermanentDataItem,
  PlanId,
  PlannedDataItem,
  PostedBundle,
  PostedBundleDBResult,
  SeededBundle,
} from "../../src/types/dbTypes";
import { TransactionId } from "../../src/types/types";
import { stubByteCount, stubOwnerAddress, stubWinstonPrice } from "../stubs";

export function assertExpectedHeadersWithContentLength(
  headers: AxiosResponseHeaders,
  contentLength: number
) {
  expect(headers.date).to.exist;
  expect(headers).to.deep.include(
    expectedHeadersWithContentLength(contentLength)
  );
}

const expectedHeadersWithContentLength = (contentLength: number) => {
  // Headers without `date` for deep equality check
  // `date` value is not consistently predictable
  return {
    "content-type": "text/plain; charset=utf-8",
    "content-length": `${contentLength}`,
    connection: "close",
  };
};

interface NewDataItemExpectations {
  expectedDataItemId: TransactionId;
}

interface PlannedDataItemExpectations {
  expectedDataItemId: TransactionId;
  expectedPlanId: PlanId;
}

export function newDataItemExpectations(
  {
    dataItemId,
    ownerPublicAddress,
    byteCount,
    assessedWinstonPrice,
    uploadedDate,
  }: NewDataItem,
  { expectedDataItemId }: NewDataItemExpectations
): void {
  expect(assessedWinstonPrice.toString()).to.equal(stubWinstonPrice.toString());
  expect(byteCount).to.equal(stubByteCount);
  expect(dataItemId).to.equal(expectedDataItemId);
  expect(ownerPublicAddress).to.equal(stubOwnerAddress);
  expect(uploadedDate).to.exist;
}

export function newDataItemDbResultExpectations(
  dbResult: NewDataItemDBResult,
  expectations: NewDataItemExpectations
): void {
  return newDataItemExpectations(
    newDataItemDbResultToNewDataItemMap(dbResult),
    expectations
  );
}

export function plannedDataItemExpectations(
  plannedDataItem: PlannedDataItem,
  expectations: PlannedDataItemExpectations
): void {
  expect(plannedDataItem.planId).to.equal(expectations.expectedPlanId);
  expect(plannedDataItem.plannedDate).to.exist;
  return newDataItemExpectations(plannedDataItem, expectations);
}

export function permanentDataItemExpectations(
  permanentDataItem: PermanentDataItem,
  expectations: PlannedDataItemExpectations
): void {
  expect(permanentDataItem.permanentDate).to.exist;
  return plannedDataItemExpectations(permanentDataItem, expectations);
}

interface NewBundleExpectations {
  expectedBundleId: TransactionId;
  expectedPlanId: PlanId;
}

export function newBundleExpectations(
  { bundleId, planId, plannedDate, reward, signedDate }: NewBundle,
  { expectedBundleId, expectedPlanId }: NewBundleExpectations
) {
  expect(reward.toString()).to.equal(stubWinstonPrice.toString());
  expect(bundleId).to.equal(expectedBundleId);
  expect(planId).to.equal(expectedPlanId);
  expect(plannedDate).to.exist;
  expect(signedDate).to.exist;
}

export function newBundleDbResultExpectations(
  newBundleDbResult: NewBundleDBResult,
  expectations: NewBundleExpectations
) {
  return newBundleExpectations(
    newBundleDbResultToNewBundleMap(newBundleDbResult),
    expectations
  );
}

export function postedBundleExpectations(
  postedBundle: PostedBundle,
  expectations: NewBundleExpectations
) {
  expect(postedBundle.postedDate).to.exist;
  return newBundleExpectations(postedBundle, expectations);
}

export function seededBundleExpectations(
  seededBundle: SeededBundle,
  expectations: NewBundleExpectations
) {
  expect(seededBundle.seededDate).to.exist;
  return postedBundleExpectations(seededBundle, expectations);
}

export function postedBundleDbResultExpectations(
  dbResult: PostedBundleDBResult,
  expectations: NewBundleExpectations
) {
  return postedBundleExpectations(
    postedBundleDbResultToPostedBundleMap(dbResult),
    expectations
  );
}

export function permanentBundleExpectations(
  permanentBundle: PermanentBundle,
  expectations: NewBundleExpectations
) {
  expect(permanentBundle.permanentDate).to.exist;
  return seededBundleExpectations(permanentBundle, expectations);
}

export function failedBundleExpectations(
  failedBundle: FailedBundle,
  expectations: NewBundleExpectations
) {
  expect(failedBundle.failedDate).to.exist;
  return seededBundleExpectations(failedBundle, expectations);
}

const expectedDateColumn = {
  type: "timestamp without time zone",
  maxLength: null,
  nullable: false,
  defaultValue: "CURRENT_TIMESTAMP",
};

const expectedVarCharColumn = ({
  length = 255,
  nullable = false,
  defaultValue = null,
}: {
  length?: number;
  nullable?: boolean;
  defaultValue?: null | string;
}) => {
  return {
    type: "character varying",
    maxLength: length,
    nullable,
    defaultValue,
  };
};

const expectedIntegerColumn = ({
  length = null,
  nullable = false,
  defaultValue = null,
}: {
  length?: number | null;
  nullable?: boolean;
  defaultValue?: null | string;
}) => {
  return {
    type: "integer",
    maxLength: length,
    nullable,
    defaultValue,
  };
};

export const expectedColumnInfo = {
  data_item_id: expectedVarCharColumn({ length: 43 }),
  uploaded_date: expectedDateColumn,
  owner_public_address: expectedVarCharColumn({ length: 43 }),
  byte_count: expectedVarCharColumn({}),
  assessed_winston_price: expectedVarCharColumn({}),
  data_start: expectedIntegerColumn({ nullable: true }),
  signature_type: expectedIntegerColumn({ nullable: true }),
  failed_bundles: expectedVarCharColumn({ nullable: true }),

  plan_id: expectedVarCharColumn({}),
  planned_date: expectedDateColumn,

  signed_date: expectedDateColumn,

  bundle_id: expectedVarCharColumn({ length: 43 }),
  reward: expectedVarCharColumn({}),

  header_byte_count: expectedIntegerColumn({ nullable: true }),
  transaction_byte_count: expectedIntegerColumn({ nullable: true }),
  payload_byte_count: expectedIntegerColumn({ nullable: true }),

  posted_date: expectedDateColumn,
  seeded_date: expectedDateColumn,

  failed_date: expectedDateColumn,
  failed_reason: expectedVarCharColumn({ nullable: true }),

  permanent_date: expectedDateColumn,
  block_height: expectedVarCharColumn({}),
  indexed_on_gql: {
    type: "boolean",
    nullable: true,
    defaultValue: null,
    maxLength: null,
  },
};
