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
export class DataItemExistsWarning extends Error {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  constructor(message: any) {
    super(message);
    this.name = "DataItemExistsWarning";
  }
}

export interface PostgresError {
  code: string;
  constraint: string;
  detail: string;
  file: string;
  length: number;
  line: string;
  name: string;
  routine: string;
  schema: string;
  severity: string;
  table: string;
}

export const postgresInsertFailedPrimaryKeyNotUniqueCode = "23505";
export const postgresTableRowsLockedUniqueCode = "55P03";
