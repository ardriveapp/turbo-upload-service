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

import { PromiseCache } from "./promiseCache";

describe("PromiseCache class", () => {
  it("constructor takes a capacity that is not exceeded by excessive puts", async () => {
    const cache = new PromiseCache<string, string>({ cacheCapacity: 1 });
    cache.put("1", Promise.resolve("one"));
    cache.put("2", Promise.resolve("two"));
    expect(cache.get("1")).to.be.undefined;
    expect(cache.get("2")).to.not.be.undefined;
    expect(await cache.get("2")).to.equal("two");
    expect(cache.size()).to.equal(1);
  });

  it("preserves most requested entries when over capacity", async () => {
    const cache = new PromiseCache<string, string>({ cacheCapacity: 3 });
    cache.put("1", Promise.resolve("one"));
    cache.put("2", Promise.resolve("two"));
    cache.put("3", Promise.resolve("three"));
    cache.get("1");
    cache.get("3");
    cache.put("4", Promise.resolve("four"));
    expect(cache.get("1")).to.not.be.undefined;
    expect(cache.get("2")).to.be.undefined;
    expect(cache.get("3")).to.not.be.undefined;
    expect(cache.get("4")).to.not.be.undefined;
    expect(await cache.get("1")).to.equal("one");
    expect(await cache.get("3")).to.equal("three");
    expect(await cache.get("4")).to.equal("four");
    expect(cache.size()).to.equal(3);
  });

  it("caches and retrieves new entries", async () => {
    const cache = new PromiseCache<string, string>({ cacheCapacity: 1 });
    cache.put("1", Promise.resolve("one"));
    expect(cache.get("1")).to.not.be.undefined;
    expect(await cache.get("1")).to.equal("one");
    expect(cache.size()).to.equal(1);
  });

  it("updates and retrieves existing entries", async () => {
    const cache = new PromiseCache<string, string>({ cacheCapacity: 2 });
    cache.put("1", Promise.resolve("one"));
    cache.put("1", Promise.resolve("uno"));
    expect(cache.get("1")).to.not.be.undefined;
    expect(await cache.get("1")).to.equal("uno");
    expect(cache.size()).to.equal(1);
  });

  it("caches and retrieves different object entries", async () => {
    const cache = new PromiseCache<Record<string, string>, string>({
      cacheCapacity: 2,
    });
    const cacheKey1 = { foo: "bar" };
    const cacheKey2 = { bar: "foo" };
    cache.put(cacheKey1, Promise.resolve("foobar"));
    cache.put(cacheKey2, Promise.resolve("barfoo"));
    expect(cache.get(cacheKey1)).to.not.be.undefined;
    expect(await cache.get(cacheKey1)).to.equal("foobar");
    expect(cache.get(cacheKey2)).to.not.be.undefined;
    expect(await cache.get(cacheKey2)).to.equal("barfoo");
    expect(cache.size()).to.equal(2);
  });

  describe("remove function", () => {
    it("removes a single entry", async () => {
      const cache = new PromiseCache<string, string>({ cacheCapacity: 2 });
      cache.put("1", Promise.resolve("one"));
      cache.put("2", Promise.resolve("two"));
      expect(cache.get("1")).to.not.be.undefined;
      expect(cache.get("2")).to.not.be.undefined;
      cache.remove("2");
      expect(cache.get("2")).to.be.undefined;
      expect(cache.get("1")).to.not.undefined;
      expect(await cache.get("1")).to.equal("one");
      expect(cache.size()).to.equal(1);
    });
  });

  describe("clear function", () => {
    it("purges all entries", async () => {
      const cache = new PromiseCache<string, string>({ cacheCapacity: 1 });
      cache.put("1", Promise.resolve("one"));
      cache.clear();
      expect(cache.get("1")).to.be.undefined;
      expect(cache.size()).to.equal(0);
    });
  });

  describe("size function", () => {
    it("returns the correct entry count", async () => {
      const cache = new PromiseCache<string, string>({ cacheCapacity: 2 });
      cache.put("1", Promise.resolve("one"));
      cache.put("2", Promise.resolve("two"));
      expect(cache.size()).to.equal(2);
    });
  });

  describe("cacheKeyString function", () => {
    it("returns and input string as the same string", async () => {
      const cache = new PromiseCache<string, string>({ cacheCapacity: 1 });
      expect(cache.cacheKeyString("key")).to.equal("key");
      expect(cache.cacheKeyString('{ bad: "json"')).to.equal('{ bad: "json"');
    });

    it("returns an input number as a string", async () => {
      const cache = new PromiseCache<number, string>({ cacheCapacity: 1 });
      expect(cache.cacheKeyString(1)).to.equal("1");
    });

    it("returns an input object as its JSON representation", async () => {
      const cache = new PromiseCache<Record<string, string>, string>({
        cacheCapacity: 1,
      });
      expect(cache.cacheKeyString({ foo: "bar" })).to.equal('{"foo":"bar"}');
    });
  });

  describe("time to live", () => {
    it("purges all entries after ttl", async () => {
      const cache = new PromiseCache<string, string>({
        cacheCapacity: 1,
        cacheTTL: 10,
      });
      cache.put("1", Promise.resolve("one"));
      await new Promise((resolve) => setTimeout(resolve, 15));
      expect(cache.get("1")).to.be.undefined;
      expect(cache.size()).to.equal(0);
    });
  });
});
