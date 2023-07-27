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
import { Cache, EphemeralCache } from "@alexsasharegan/simple-cache";

import { msPerMinute } from "../constants";

export interface CacheParams {
  cacheCapacity: number;
  cacheTTL?: number;
}
export class PromiseCache<K, V> {
  private cache: Cache<string, Promise<V>>;

  constructor({ cacheCapacity, cacheTTL = msPerMinute * 1 }: CacheParams) {
    this.cache = EphemeralCache<string, Promise<V>>(cacheCapacity, cacheTTL);
  }

  cacheKeyString(key: K): string {
    // Note: This implementation may not sufficiently differentiate keys
    // for certain object types depending on their toJSON implementation
    return typeof key === "string" ? key : JSON.stringify(key);
  }

  put(key: K, value: Promise<V>): Promise<V> {
    this.cache.write(this.cacheKeyString(key), value);
    return value;
  }

  get(key: K): Promise<V> | undefined {
    return this.cache.read(this.cacheKeyString(key));
  }

  remove(key: K): void {
    this.cache.remove(this.cacheKeyString(key));
  }

  clear(): void {
    this.cache.clear();
  }

  size(): number {
    return this.cache.size();
  }
}
