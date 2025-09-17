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
import chai from "chai";
import deepEqualInAnyOrder from "deep-equal-in-any-order";

import {
  DataItemHeader,
  encodeTagsForOptical,
  filterForNestedBundles,
} from "./opticalUtils";

const { expect } = chai;
chai.use(deepEqualInAnyOrder);

describe("The encodeTagsForOptical function", () => {
  it("returns the passed in dataItemHeader when no tags are present", () => {
    const noTagsDataItemHeader = {
      id: "id",
      owner: "owner",
      owner_address: "owner_address",
      signature: "signature",
      target: "target",
      content_type: "content_type",
      data_size: 1234,
      tags: [],
    };
    expect(encodeTagsForOptical(noTagsDataItemHeader)).to.deep.equal(
      noTagsDataItemHeader
    );
  });

  it("returns the passed in dataItemHeader with its tags' name and value values base64url encoded", () => {
    const dataItemHeader = {
      id: "id",
      owner: "owner",
      owner_address: "owner_address",
      signature: "signature",
      target: "target",
      content_type: "content_type",
      data_size: 1234,
      tags: [
        { name: "foo", value: "bar" },
        { name: "fizz", value: "buzz" },
      ],
    };
    expect(encodeTagsForOptical(dataItemHeader)).to.deep.equal({
      id: "id",
      owner: "owner",
      owner_address: "owner_address",
      signature: "signature",
      target: "target",
      content_type: "content_type",
      data_size: 1234,
      tags: [
        { name: "Zm9v", value: "YmFy" },
        { name: "Zml6eg", value: "YnV6eg" },
      ],
    });
  });
});

describe("The filterForNestedBundles function", () => {
  it("returns false when passed a header with no tags", () => {
    const stubDataItemHeader: DataItemHeader = {
      id: "id",
      owner: "owner",
      owner_address: "owner_address",
      signature: "signature",
      target: "target",
      content_type: "content_type",
      data_size: 1234,
      tags: [],
    };

    expect(filterForNestedBundles(stubDataItemHeader)).to.equal(false);
  });

  it("returns false when passed a header that contains only partial tag matches", () => {
    const stubDataItemHeaders: DataItemHeader[] = [
      {
        id: "id",
        owner: "owner",
        owner_address: "owner_address",
        signature: "signature",
        target: "target",
        content_type: "content_type",
        data_size: 1234,
        tags: [{ name: "Bundle-Format", value: "binary" }],
      },
      {
        id: "id",
        owner: "owner",
        owner_address: "owner_address",
        signature: "signature",
        target: "target",
        content_type: "content_type",
        data_size: 1234,
        tags: [{ name: "Bundle-Version", value: "2.0.0" }],
      },
    ];
    for (const testHeader of stubDataItemHeaders) {
      expect(filterForNestedBundles(testHeader)).to.equal(false);
    }
  });

  it("returns true when passed a header that contains all necessary tag matches", () => {
    const stubDataItemHeader: DataItemHeader = {
      id: "id",
      owner: "owner",
      owner_address: "owner_address",
      signature: "signature",
      target: "target",
      content_type: "content_type",
      data_size: 1234,
      tags: [
        { name: "Bundle-Format", value: "binary" },
        { name: "Bundle-Version", value: "2.0.0" },
      ],
    };
    expect(filterForNestedBundles(stubDataItemHeader)).to.equal(true);
  });
});
