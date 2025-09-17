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
import { createReadStream } from "fs";

import { InMemoryDataItem } from "../src/bundles/streamingDataItem";
import globalLogger from "../src/logger";
import { DataItemOffsetsInfo } from "../src/types/types";
import {
  deleteDynamoDataItem,
  deleteDynamoOffsetsInfo,
  getDynamoDataItem,
  getDynamoOffsetsInfo,
  putDynamoDataItem,
  putDynamoOffsetsInfo,
} from "../src/utils/dynamoDbUtils";
import { streamToBuffer } from "../src/utils/streamToBuffer";

const stubDataItemPath = "tests/stubFiles/stub1115ByteDataItem";

describe("DynamoDB Utils", () => {
  after(async () => {
    await deleteDynamoDataItem(
      "QpmY8mZmFEC8RxNsgbxSV6e36OF6quIYaPRKzvUco0o",
      globalLogger
    );
    await deleteDynamoOffsetsInfo(
      "QpmY8mZmFEC8RxNsgbxSV6e36OF6quIYaPRKzvUco0o",
      globalLogger
    );
  });

  describe("put/getDynamoDataItem", () => {
    it("should put and get an item into/from DynamoDB", async () => {
      const stubDataItemBuffer = await streamToBuffer(
        createReadStream(stubDataItemPath)
      );
      const inMemoryDataItem = new InMemoryDataItem(stubDataItemBuffer);
      const expectedDataItemId = await inMemoryDataItem.getDataItemId();
      const expectedData = stubDataItemBuffer;
      const payloadBuffer = await streamToBuffer(
        await inMemoryDataItem.getPayloadStream()
      );
      const expectedPayloadStart =
        stubDataItemBuffer.byteLength - payloadBuffer.byteLength;
      const expectedContentType = "application/octet-stream";

      await putDynamoDataItem({
        dataItemId: expectedDataItemId,
        data: expectedData,
        size: stubDataItemBuffer.byteLength,
        payloadStart: expectedPayloadStart,
        contentType: expectedContentType,
        logger: globalLogger,
      });

      const getItemResult = await getDynamoDataItem({
        dataItemId: expectedDataItemId,
        logger: globalLogger,
      });

      expect(getItemResult).to.not.be.null;
      expect(getItemResult?.buffer.equals(expectedData)).to.be.true;
      expect(getItemResult?.info).to.deep.equal({
        payloadDataStart: expectedPayloadStart,
        payloadContentType: expectedContentType,
      });
    });
  });

  describe("put/getDynamoOffsetsInfo", () => {
    it("should put and get offsets info into/from DynamoDB", async () => {
      // Real data item ID with fake numbers
      const offsetsInfo: DataItemOffsetsInfo = {
        dataItemId: "QpmY8mZmFEC8RxNsgbxSV6e36OF6quIYaPRKzvUco0o",
        parentDataItemId: "J40R1BgFSI1_7p25QW49T7P46BePJJnlDrsFGY1YWbM",
        startOffsetInParentDataItemPayload: 5,
        rootBundleId: "35jbLhCGEfXLWe2H3VZr2i7f610kwP8Nkw-bFfx14-E",
        startOffsetInRootBundle: 10,
        rawContentLength: 100,
        payloadContentType: "application/octet-stream",
        payloadDataStart: 20,
      };

      await putDynamoOffsetsInfo({ ...offsetsInfo, logger: globalLogger });

      const getOffsetsResult = await getDynamoOffsetsInfo(
        offsetsInfo.dataItemId,
        globalLogger
      );

      expect(getOffsetsResult).to.not.be.null;
      expect(getOffsetsResult).to.deep.equal(offsetsInfo);
    });
  });
});
