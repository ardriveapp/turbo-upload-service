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
import { Redis } from "ioredis";

export type CacheService = Redis;

export class CacheServiceError extends Error {
  constructor(message: string, public readonly cause?: unknown) {
    super(message);
    this.name = "CacheServiceError";
  }
}

export const stubCacheService: CacheService = {
  get: async (_: string) => {
    return null;
  },
  getBuffer: async (_: string) => {
    return null;
  },
  // @ts-expect-error -- This is a stub
  set: async (_: string, __: string | Buffer | number, ___?: string) => {
    return "OK";
  },
  // @ts-expect-error -- This is a stub
  exists: async (..._: string[]) => {
    return 0;
  },
  // @ts-expect-error -- This is a stub
  del: async (..._: string[]) => {
    return 0;
  },
  status: "ready",
  isCluster: false,
};
