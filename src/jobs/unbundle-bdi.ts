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
import { Message } from "@aws-sdk/client-sqs";
import { processStream } from "arbundles";
import { SQSEvent } from "aws-lambda";
import pLimit from "p-limit";
import winston from "winston";

import { deleteMessages } from "../arch/queues";
import { rawDataItemStartFromParsedHeader } from "../bundles/rawDataItemStartFromParsedHeader";
import baseLogger from "../logger";
import { ParsedDataItemHeader, TransactionId } from "../types/types";
import { payloadContentTypeFromDecodedTags } from "../utils/common";
import {
  getDataItemData,
  getS3ObjectStore,
  putDataItemRaw,
} from "../utils/objectStoreUtils";

export const handler = async (event: SQSEvent) => {
  const handlerLogger = baseLogger.child({ job: "unbundle-bdi-job" });
  await unbundleBDISQSHandler(
    // Map necessary fields from SQSRecord to Message type
    event.Records.map((record) => {
      return {
        MessageId: record.messageId,
        ReceiptHandle: record.receiptHandle,
        Body: record.body,
      };
    }),
    handlerLogger
  );
};

export async function unbundleBDISQSHandler(
  messages: Message[],
  logger: winston.Logger
) {
  const bdiIdsToRecordsMap = messages.reduce((acc, record) => {
    const bdiId = JSON.parse(record.Body ?? "");
    acc[bdiId] = record;
    return acc;
  }, {} as Record<TransactionId, Message>);

  const bdisToUnpack = Object.keys(bdiIdsToRecordsMap);
  const handledBdiIds = await unbundleBDIHandler(bdisToUnpack, logger);

  // Compute unhandledRecords by getting the minusSet of handledBdiIds from bdisToUnpack and then filtering the records
  const recordsToDelete = handledBdiIds.map(
    (bdiId) => bdiIdsToRecordsMap[bdiId]
  );
  const unhandledRecords = messages.filter((record) => {
    !recordsToDelete.includes(record);
  });

  logger.debug("Cleaning up records...", {
    recordsToDelete,
    unhandledRecords,
  });

  void deleteMessages(
    "unbundle-bdi",
    recordsToDelete.map((record) => {
      return {
        Id: record.MessageId,
        ReceiptHandle: record.ReceiptHandle,
      };
    })
  );
  if (unhandledRecords.length > 0) {
    throw new Error(`Some messages could not handled!`);
  }
}

export async function unbundleBDIHandler(
  bdisToUnpack: string[],
  logger: winston.Logger
) {
  logger.info("Go!", { bdisToUnpack });

  const bdiParallelLimit = pLimit(10);
  const objectStore = getS3ObjectStore();

  // Make a best effort to unpack the BDI and stash its nested data items' payloads in the object store
  const handledBdiIds: string[] = [];
  await Promise.all(
    bdisToUnpack.map((bdiIdToUnpack) => {
      const bdiLogger = logger.child({ bdiIdToUnpack });
      return bdiParallelLimit(async () => {
        try {
          // Fetch the BDI
          const dataItemReadable = await getDataItemData(
            objectStore,
            bdiIdToUnpack
          );

          bdiLogger.debug("Processing BDI stream...");

          // Process it as a bundle and get all the data item info
          const parsedDataItemHeaders = (await processStream(
            dataItemReadable
          )) as ParsedDataItemHeader[];

          const nestedIds = parsedDataItemHeaders.map(
            (parsedDataItemHeader) => parsedDataItemHeader.id
          );

          bdiLogger.info("nestedIds", {
            nestedIds,
          });

          const nestedDataItemParallelLimit = pLimit(10);
          await Promise.all(
            parsedDataItemHeaders.map((parsedDataItemHeader) => {
              const { id, tags, dataOffset, dataSize } = parsedDataItemHeader;
              const nestedItemLogger = bdiLogger.child({
                nestedDataItemId: id,
              });
              return nestedDataItemParallelLimit(async () => {
                // Discern a content type for the data item if possible
                const payloadContentType =
                  payloadContentTypeFromDecodedTags(tags);

                // Stash the full raw data item in the object store
                const rawDataItemDataStart =
                  rawDataItemStartFromParsedHeader(parsedDataItemHeader);
                const payloadEndOffset = dataOffset + dataSize - 1; // -1 because the range is INCLUSIVE
                const rangeString = `bytes=${rawDataItemDataStart}-${payloadEndOffset}`;

                nestedItemLogger.debug("Caching nested data item...", {
                  payloadContentType,
                  rangeString,
                });
                await putDataItemRaw(
                  objectStore,
                  id,
                  await getDataItemData(
                    objectStore,
                    bdiIdToUnpack,
                    rangeString
                  ),
                  payloadContentType,
                  dataOffset - rawDataItemDataStart
                );

                nestedItemLogger.debug("Finished caching nested data item.", {
                  payloadContentType,
                  rangeString,
                });
              });
            })
          );
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
        } catch (error: any) {
          const message =
            error instanceof Error ? error.message : `Unknown error: ${error}`;
          bdiLogger.error("Encountered error unpacking bdi", {
            error: message,
            stack: error?.stack,
          });
          return;
        }

        // Take note that we successfully unbundled the bdi
        handledBdiIds.push(bdiIdToUnpack);
      });
    })
  );
  return handledBdiIds;
}
