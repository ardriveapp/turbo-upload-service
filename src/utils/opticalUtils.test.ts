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
import chai from "chai";
import deepEqualInAnyOrder from "deep-equal-in-any-order";
import { createReadStream } from "fs";
import { stub } from "sinon";

import { FileSystemObjectStore } from "../arch/fileSystemObjectStore";
import logger from "../logger";
import {
  DataItemHeader,
  encodeTagsForOptical,
  filterForNestedBundles,
  getNestedDataItemHeaders,
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

describe("The getNestedDataItemHeaders function", () => {
  const objectStore = new FileSystemObjectStore();
  before(() => {
    stub(objectStore, "getObject").resolves(
      createReadStream("tests/stubFiles/bdiDataItem")
    );
  });

  it("returns correct nested data item headers for a real bundled data item", async () => {
    expect(
      await getNestedDataItemHeaders({
        objectStore,
        logger: logger,
        potentialBDIHeaders: [
          {
            id: "cTbz16hHhGW4HF-uMJ5u8RoCg9atYmyMFWGd-kzhF_Q",
            owner: "owner",
            owner_address: "owner_address",
            signature: "signature",
            target: "target",
            content_type: "content_type",
            data_size: 1234,
            tags: [
              { name: "QnVuZGxlLUZvcm1hdA", value: "YmluYXJ5" },
              { name: "QnVuZGxlLVZlcnNpb24", value: "Mi4wLjA" },
            ],
          },
        ],
      })
    ).to.deep.equalInAnyOrder([
      {
        id: "5SLX-Vuzy90kg2DBv8yrOCfUPF-4KB43fFv5uUwxzYc",
        signature:
          "eqnekVilJFbzVQa2g-AIkX10trRIBEKZV_0tz0zya_XJdcrVATm7_0nWk3VIXXz4-Cykd9BBS0AGbcLmFfohiiaA5knAXGx3WP0bMyCiUR4TgkNIwVnQDtCKRPWdEaFizd-t6PqVy-KIfG0iJoKqE4u0BanBjgSU-R_7-K4pP3g9d3ScKS8vImLAmVfy29ubyE5ubALNl1c0OruSRfig37DT0Vf1ZDfqmllPcRsUrxbXmX2dtcEvDcaotqgcRTu8iRCYeECGOl7hFR50SUAKAQKjJKdGbR_5dFdGLBT2clzwGAHaKdq87-eMseHwbCgT9gPvqHmpBshFGSab8dzwIiNXGLquAxrzL8i3Y2Bl578BPhCIGRM9Vm0T1kiQBECtsORqZVwENI-urKU3BVgGLDmfL9iplNHbPhvKwgEbTaFvi6SMAfbecZSxMF5UzZfpm-m4O2Ba0z6iEoB9iLwEx8t446Bjn3iTLgRpvO5epeYICUjN7cBRelnbgj2No9hHXyea1VYnPUzhmDkiYoatIL7ISXdXr5OY0urjCqzqgLOluA2QvXszEf1kO61SIOblow1i38fvfe06j2txPSlccZTGh_Ug7yntTa6WijlFA0tfWk_AGFtwogLqOob3vD5lhPACkig6UPDonSD646xD4EcQ_sINIGhIlJdMJsF13z4",
        owner:
          "2_E-vc9U6OYFv26YxHNtMSmxsIixQAsFrCQSVW9nEvPHkA2uZ0R7ILwqzqs7DtJoptp5qpar88qtsITQOCcJsSdcISvuJUXVgX_12520bSMhErkGhdOpRI_AWqBJTZwX5dUDf6_jc5JFMHsZar8At2vdq3MTeNJFSh2NoDMFndTkgCZBVrJjtrwXPcFnWZ3C7_DTYwF9-FHVHIQorHuBL5wXJI3gojuBHtuOxWrR61k2V14P05wPKZoJW85hVOzJfd7VECvbMlgfhqpS9Qf7V2pztEdNCOETPN914HaPd4fEAB2BUHFfqt1XTbjEgHADEQHtdwA6sMG17pX1V4POfdnMZEvlK5q7F2LfNYhlkaWV-jUQ36aV6nRRpq9HrAeAwcwLCklqK140axrinMR9gEfKsqCAbbWA92G_Rn88G3y-6F3hw6BPsmNUp8ggW-fzR35YC3BCLNYVwmM5yBQiE1oDoJz1o6q3wR3swHCL_QCvsJ0yDiGWKAt_aHd-58x4lLeFWFRnlGmbMLZ7b6NRhwRvRiAxruYxEGNH6TKr2rVYdLV13CV4frg9V_cf12--rOa8OEYFHcQLqVWBh3z9rA5uO5KMPgkhhRet1pMlTMTa4zcxJ12HeRC4Li297iQh0hf1rYUu4ZGMAEa-ppjhFES67etlN4NlyyONDrFTaLM",
        owner_address: "31LPFYoow2G7j-eSSsrIh8OlNaARZ84-80J-8ba68d8",
        target: "",
        content_type: "application/json",
        data_size: 160,
        tags: [
          { name: "Content-Type", value: "application/json" },
          { name: "ArFS", value: "0.12" },
          { name: "Entity-Type", value: "file" },
          {
            name: "Drive-Id",
            value: "1677890d-5d17-4b70-a1f9-ea3f23ffba30",
          },
          {
            name: "Parent-Folder-Id",
            value: "2e352112-bbe9-4717-9a94-9cdc95a27767",
          },
          {
            name: "File-Id",
            value: "ba300049-f8ee-45a6-92d5-4f384c9b066b",
          },
          { name: "App-Name", value: "ArDrive-App" },
          { name: "App-Platform", value: "Web" },
          { name: "App-Version", value: "2.3.1" },
          { name: "Unix-Time", value: "1687212613" },
        ],
      },
      {
        id: "Is7dfYMRMxxJTiKyxIoJHa-zBD6S58YwhkNEzqq22ww",
        signature:
          "fpRU4294AjPakiTaQuaxgFusemt9wA_9sSxeLzVmpEPN3uVMzZyuMTwdgjcXoLA4lhdplgAXfrUC8cpIyH9hzMWGymEoqYovCrvqngQSEO6k7tuWrDRowaCjwPx2PxtECR41vqF074YVepCRHJ55j1p-J4YQkRudV4Y8UzrfSAUWXhkgBH8Z6MIqF-u9TNSA6cDL5ZZNMzvDccdVLs-ykE--Qf-IkvxoaPdLL41ZbG9C3LbB6IaZSCUBidmuMRcHESUnlofvdPU70gzAsLIZzAg3mqwF8M3FVIOHv4rNFNhyOYOyTLgaJ0KQ7bCNEeUMmaMMEk6mFsrtezy4eoquR6klW8jyrAsh0cbLX0eu-6tu5UDYJdcmcgllgwAti9cJfkwbCe3zvAAfvG_wJvaYN7FRcaipW8DUxfBD0Bab9lNl5DdGYL4oik1OYkyMwlxzrusV3ZjUycZQnUWElMHRX2-Nersj741nIbytAfScG83yiCfS3zXODrysbbc_Tz-Ftp4kR_HcgwhEZbWBhxMLHbvXayDn5gqDqKb81TtxsmL2gkKN6-4A9lhD7L3RdJi96f82iuq-h6P-PMOzkzuCAuy5Gu6J_knDvQ9NrN6L-Kxd3gsJd71GsS2NnuTUUnMGqtXrL55nxoZlYLKGr9O3_t5bj2Py1T3FMbBkqUf4NoE",
        owner:
          "2_E-vc9U6OYFv26YxHNtMSmxsIixQAsFrCQSVW9nEvPHkA2uZ0R7ILwqzqs7DtJoptp5qpar88qtsITQOCcJsSdcISvuJUXVgX_12520bSMhErkGhdOpRI_AWqBJTZwX5dUDf6_jc5JFMHsZar8At2vdq3MTeNJFSh2NoDMFndTkgCZBVrJjtrwXPcFnWZ3C7_DTYwF9-FHVHIQorHuBL5wXJI3gojuBHtuOxWrR61k2V14P05wPKZoJW85hVOzJfd7VECvbMlgfhqpS9Qf7V2pztEdNCOETPN914HaPd4fEAB2BUHFfqt1XTbjEgHADEQHtdwA6sMG17pX1V4POfdnMZEvlK5q7F2LfNYhlkaWV-jUQ36aV6nRRpq9HrAeAwcwLCklqK140axrinMR9gEfKsqCAbbWA92G_Rn88G3y-6F3hw6BPsmNUp8ggW-fzR35YC3BCLNYVwmM5yBQiE1oDoJz1o6q3wR3swHCL_QCvsJ0yDiGWKAt_aHd-58x4lLeFWFRnlGmbMLZ7b6NRhwRvRiAxruYxEGNH6TKr2rVYdLV13CV4frg9V_cf12--rOa8OEYFHcQLqVWBh3z9rA5uO5KMPgkhhRet1pMlTMTa4zcxJ12HeRC4Li297iQh0hf1rYUu4ZGMAEa-ppjhFES67etlN4NlyyONDrFTaLM",
        owner_address: "31LPFYoow2G7j-eSSsrIh8OlNaARZ84-80J-8ba68d8",
        target: "",
        content_type: "image/gif",
        data_size: 51987,
        tags: [
          { name: "App-Name", value: "ArDrive-App" },
          { name: "App-Platform", value: "Web" },
          { name: "App-Version", value: "2.3.1" },
          { name: "Unix-Time", value: "1687212613" },
          { name: "Content-Type", value: "image/gif" },
        ],
      },
    ]);
  });
});
