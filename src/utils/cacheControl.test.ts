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
import { expect } from "chai";
import { BaseContext } from "koa";
import { stub } from "sinon";

import { W } from "../types/winston";
import { setCacheControlHeadersForDataItemInfo } from "./cacheControl";

describe("setCacheControlHeadersForDataItemInfo", () => {
  const ctxStub = {
    set(_: string, __: string | string[]) {
      _;
    },
  } as Partial<BaseContext>;

  it("should set a short duration cache-control header when no status exists", () => {
    const ctxSpy = stub(ctxStub, "set");
    setCacheControlHeadersForDataItemInfo(ctxStub, undefined);
    expect(ctxSpy.calledOnceWith("Cache-Control", `public, max-age=15`)).to.be
      .true;
  });

  const inputsToOutputsMap = {
    new: 15,
    pending: 15,
    permanent: 86400,
    failed: 15,
  };

  for (const [status, expectedMaxAge] of Object.entries(inputsToOutputsMap)) {
    it(`should set a cache-control header with max-age=${expectedMaxAge} for status '${status}'`, () => {
      const ctxSpy = stub(ctxStub, "set");
      setCacheControlHeadersForDataItemInfo(ctxStub, {
        status: status as "new" | "pending" | "permanent" | "failed",
        assessedWinstonPrice: W(0),
        uploadedTimestamp: 12345,
        owner: "owner",
      });
      expect(
        ctxSpy.calledOnceWith(
          "Cache-Control",
          `public, max-age=${expectedMaxAge}`
        )
      ).to.be.true;
    });
  }
});
