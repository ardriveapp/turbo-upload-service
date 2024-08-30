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

import { enqueue } from "../arch/queues";
import { rawDataItemStartFromParsedHeader } from "../bundles/rawDataItemStartFromParsedHeader";
import { jobLabels } from "../constants";
import baseLogger from "../logger";
import { ParsedDataItemHeader } from "../types/types";
import { ownerToNormalizedB64Address } from "../utils/base64";
import { payloadContentTypeFromDecodedTags } from "../utils/common";
import {
  getDataItemData,
  getS3ObjectStore,
  putDataItemRaw,
} from "../utils/objectStoreUtils";
import {
  encodeTagsForOptical,
  signDataItemHeader,
} from "../utils/opticalUtils";

export type UnbundleBDIMessageBody = {
  id: string;
  // TODO: Make non-nullable after initial deploy. Existing records will have a string value.
  uploaded_at?: number;
};

// TODO: Remove after initial deploy. Existing records will have a string value.
type IncomingBDIMessageBody = string | UnbundleBDIMessageBody;

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
  const bdisToUnpack: UnbundleBDIMessageBody[] = [];

  messages.forEach((record) => {
    const body: IncomingBDIMessageBody = JSON.parse(record.Body ?? "");

    if (typeof body === "string") {
      bdisToUnpack.push({
        id: body,
      });
    } else if (typeof body === "object" && Object.keys(body).includes("id")) {
      bdisToUnpack.push({
        id: body.id,
        uploaded_at: body.uploaded_at,
      });
    } else {
      logger.error("Invalid message body", { body });
    }
  });

  const handledBdiIds = await unbundleBDIHandler(bdisToUnpack, logger);

  if (bdisToUnpack.length !== handledBdiIds.length) {
    throw new Error(
      `Some BDI records were not handled: ${JSON.stringify(
        bdisToUnpack.filter(
          (bdiToUnpack) => !handledBdiIds.includes(bdiToUnpack.id)
        )
      )}`
    );
  }
}

export async function unbundleBDIHandler(
  bdisToUnpack: UnbundleBDIMessageBody[],
  logger: winston.Logger
) {
  logger.info("Go!", { bdisToUnpack });

  const bdiParallelLimit = pLimit(10);
  const objectStore = getS3ObjectStore();

  // Make a best effort to unpack the BDI and stash its nested data items' payloads in the object store
  const handledBdiIds: string[] = [];
  await Promise.all(
    bdisToUnpack.map(({ id: bdiIdToUnpack, uploaded_at }) => {
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

          const nestedIdsAndTags = parsedDataItemHeaders.map(
            (parsedDataItemHeader) => {
              return {
                id: parsedDataItemHeader.id,
                tags: parsedDataItemHeader.tags.map((tag) => {
                  return {
                    // Clamp the lengths to 64 chars
                    name: tag.name.substring(0, 64),
                    value: tag.value.substring(0, 64),
                  };
                }),
                owner_adddress: ownerToNormalizedB64Address(
                  parsedDataItemHeader.owner
                ),
              };
            }
          );

          bdiLogger.info("nestedIds", {
            nestedIdsAndTags,
          });

          const nestedDataItemParallelLimit = pLimit(10);
          await Promise.all(
            parsedDataItemHeaders.map((parsedDataItemHeader) => {
              const {
                id,
                tags,
                dataOffset,
                dataSize,
                signature,
                target,
                owner,
              } = parsedDataItemHeader;
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

                // TODO: Consider enqueue in batches
                await enqueue(jobLabels.opticalPost, {
                  ...(await signDataItemHeader(
                    encodeTagsForOptical({
                      id,
                      signature,
                      owner,
                      owner_address: ownerToNormalizedB64Address(owner),
                      target: target ?? "",
                      content_type: payloadContentType,
                      data_size: dataSize,
                      tags,
                    })
                  )),
                  uploaded_at: uploaded_at ?? 0, // TODO: Make non-nullable after initial deploy
                });

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
