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
import { processStream } from "arbundles";
import { SQSEvent, SQSRecord } from "aws-lambda";
import pLimit from "p-limit";

import { deleteMessages } from "../arch/queues";
import { octetStreamContentType } from "../constants";
import baseLogger from "../logger";
import { ParsedDataItemHeader } from "../types/types";
import {
  getDataItemData,
  getS3ObjectStore,
  putDataItemData,
} from "../utils/objectStoreUtils";

export const handler = async (event: SQSEvent) => {
  const handlerLogger = baseLogger.child({ job: "unbundle-bdi-job" });
  handlerLogger.info("Unbundle BDI job has been triggered.", event);

  const bdiParallelLimit = pLimit(10);
  const objectStore = getS3ObjectStore();

  // Make a best effort to unpack the BDI and stash its nested data items' payloads in S3
  const recordsToDelete: SQSRecord[] = [];
  await Promise.all(
    event.Records.map((record) => {
      const { body: messageBody } = record;
      const bdiIdToUnpack = JSON.parse(messageBody);
      const bdiLogger = handlerLogger.child({ bdiIdToUnpack });
      return bdiParallelLimit(async () => {
        try {
          // Fetch the BDI
          const dataItemReadable = await getDataItemData(
            objectStore,
            bdiIdToUnpack
          );

          bdiLogger.info("Processing BDI stream...");

          // Process it as a bundle and get all the data item info
          const parsedDataItemHeaders = (await processStream(
            dataItemReadable
          )) as ParsedDataItemHeader[];

          bdiLogger.info("Finished processing BDI stream.", {
            parsedDataItemHeaders,
          });

          const nestedDataItemParallelLimit = pLimit(10);
          await Promise.all(
            parsedDataItemHeaders.map((parsedDataItemHeader) => {
              const nestedItemLogger = bdiLogger.child({
                nestedDataItemId: parsedDataItemHeader.id,
              });
              return nestedDataItemParallelLimit(async () => {
                // Discern a content type for the data item if possible
                const contentType =
                  parsedDataItemHeader.tags
                    .filter((tag) => tag.name.toLowerCase() === "content-type")
                    .shift()?.value || octetStreamContentType;

                // Stash the data item payload in the object store
                const payloadStartOffset = parsedDataItemHeader.dataOffset;
                const payloadEndOffset =
                  payloadStartOffset + parsedDataItemHeader.dataSize - 1;
                const rangeString = `bytes=${payloadStartOffset}-${payloadEndOffset}`;

                nestedItemLogger.info("Caching nested data item...", {
                  contentType,
                  rangeString,
                });
                await putDataItemData(
                  objectStore,
                  parsedDataItemHeader.id,
                  contentType,
                  await getDataItemData(objectStore, bdiIdToUnpack, rangeString)
                );
                nestedItemLogger.info("Finished caching nested data item.", {
                  contentType,
                  rangeString,
                });
              });
            })
          );
        } catch (error) {
          bdiLogger.error("Encountered error unpacking bdi", { error });
          return;
        }

        // Successfully unbundled the bdi. Delete this message.
        recordsToDelete.push(record);
      });
    })
  );

  const unhandledRecords = event.Records.filter((record) => {
    !recordsToDelete.includes(record);
  });
  handlerLogger.info("Cleaning up records...", {
    recordsToDelete,
    unhandledRecords,
  });
  deleteMessages(
    "unbundle-bdi",
    recordsToDelete.map((record) => {
      return {
        Id: record.messageId,
        ReceiptHandle: record.receiptHandle,
      };
    })
  );
  if (unhandledRecords.length > 0) {
    throw new Error(`Some messages could not handled!`);
  }
};
