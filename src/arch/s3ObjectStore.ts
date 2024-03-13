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
import {
  CompleteMultipartUploadCommand,
  CompleteMultipartUploadCommandOutput,
  CopyObjectCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  GetObjectOutput,
  HeadObjectCommand,
  HeadObjectOutput,
  ListPartsCommand,
  PutObjectCommandInput,
  S3Client,
  UploadPartCommand,
  UploadPartCopyCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";
import { AbortController } from "@smithy/abort-controller";
import * as https from "https";
import { Readable } from "multistream";
import pLimit from "p-limit";
import winston from "winston";

import {
  payloadContentTypeS3MetaDataTag,
  payloadDataStartS3MetaDataTag,
} from "../constants";
import globalLogger from "../logger";
import { UploadId } from "../types/types";
import {
  InvalidChunk,
  InvalidChunkSize,
  MultiPartUploadNotFound,
} from "../utils/errors";
import {
  MoveObjectParams,
  ObjectStore,
  ObjectStoreOptions,
  PayloadInfo,
} from "./objectStore";

export const handleS3MultipartUploadError = (
  error: unknown,
  uploadId: UploadId
) => {
  const message = error instanceof Error ? error.message : "Unknown error.";
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  switch ((error as any).Code) {
    case "NoSuchUpload":
      throw new MultiPartUploadNotFound(uploadId);
    case "NotFound":
      throw new MultiPartUploadNotFound(uploadId);
    case "InvalidArgument":
      throw new InvalidChunk(message);
    case "EntityTooSmall":
      throw new InvalidChunkSize();
    default:
      throw error;
  }
};

export class S3ObjectStore implements ObjectStore {
  private s3: S3Client;
  private bucketName: string;
  private logger: winston.Logger;
  private multipartCopyObjectLimitBytes = 1024 * 1024 * 1024 * 5; // 5GiB limit for AWS S3 `CopyObject` operation
  private multipartCopyParallelLimit = 10;

  constructor({
    s3Client = new S3Client({
      requestHandler: new NodeHttpHandler({
        httpsAgent: new https.Agent({
          keepAlive: true,
          timeout: 0,
        }),
        requestTimeout: 0,
      }),
    }),
    // TODO: add otel tracer to track events
    bucketName,
    logger = globalLogger,
  }: {
    s3Client?: S3Client;
    bucketName: string;
    logger?: winston.Logger;
  }) {
    this.s3 = s3Client;
    this.bucketName = bucketName;
    this.logger = logger.child({
      bucketName: this.bucketName,
      objectStore: "S3ObjectStore",
    });
  }

  public async getObjectPayloadInfo(Key: string): Promise<PayloadInfo> {
    try {
      const headObjectResponse: HeadObjectOutput = await this.s3.send(
        new HeadObjectCommand({
          Key,
          Bucket: this.bucketName,
        })
      );

      if (!headObjectResponse.Metadata) {
        throw Error("No object found");
      }
      const payloadDataStart =
        headObjectResponse.Metadata[payloadDataStartS3MetaDataTag];
      const payloadContentType =
        headObjectResponse.Metadata[payloadContentTypeS3MetaDataTag];
      if (!payloadDataStart || !payloadContentType) {
        throw Error("No payload info found");
      }

      return {
        payloadDataStart: +payloadDataStart,
        payloadContentType,
      };
    } catch (error) {
      this.logger.debug(`Failed to get object metadata!`, { error, Key });
      throw error;
    }
  }

  public async putObject(
    Key: string,
    Body: Readable,
    Options: ObjectStoreOptions = {}
  ): Promise<void> {
    if (Body.errored) {
      throw new Error("Object body read stream errored");
    }

    const params: PutObjectCommandInput = {
      Key,
      Body,
      ...this.s3CommandParamsFromOptions(Options),
    };

    const controller = new AbortController();
    let abortError: Error | undefined;

    Body.on("error", (error) => {
      this.logger.error(
        "Aborting put object due to object body stream error",
        error
      );
      abortError = error;
      controller.abort();
    });

    // In order to upload streams, must use Upload instead of PutObjectCommand
    const putObject = new Upload({
      client: this.s3,
      params,
      queueSize: 1, // forces synchronous uploads
      abortController: controller,
    });

    this.logger.debug(`Putting read stream for S3 object...`, {
      Key,
      Bucket: this.bucketName,
    });

    // track upload progress
    putObject.on("httpUploadProgress", (progress) => {
      this.logger.debug("Streaming object to S3...", {
        ...progress,
      });
    });

    try {
      await putObject.done();
    } catch (error) {
      this.logger.error("Failed to put object!", {
        error: error instanceof Error ? error.message : error,
        Key,
        params,
      });
      throw abortError ?? error;
    }
  }

  public async createMultipartUpload(Key: string): Promise<string> {
    try {
      // Step 1: Start the multipart upload and get the upload ID
      const newUploadCommand = new CreateMultipartUploadCommand({
        Bucket: this.bucketName,
        Key,
      });
      const createMultipartUploadResponse = await this.s3.send(
        newUploadCommand
      );
      const uploadId = createMultipartUploadResponse.UploadId;

      if (!uploadId) {
        throw Error("No upload ID returned from S3");
      }
      return uploadId;
    } catch (error) {
      this.logger.error("Failed to create multipart upload!", { error, Key });
      throw error;
    }
  }

  public async uploadPart(
    Key: string,
    Body: Readable,
    uploadId: UploadId,
    partNumber: number,
    ContentLength: number
  ): Promise<string> {
    try {
      this.logger.debug("Uploading part", {
        Key,
        uploadId,
        partNumber,
        ContentLength,
      });

      const uploadPartCommand = new UploadPartCommand({
        UploadId: uploadId,
        Bucket: this.bucketName,
        Key,
        Body,
        PartNumber: partNumber,
        ContentLength,
      });

      const uploadResponse = await this.s3.send(uploadPartCommand);
      if (!uploadResponse.ETag) {
        throw Error("No ETag returned from S3");
      }

      return JSON.parse(uploadResponse.ETag);
    } catch (error) {
      this.logger.error("Failed to upload part!", { error, Key, uploadId });
      return handleS3MultipartUploadError(error, uploadId);
    }
  }

  public async completeMultipartUpload(Key: string, uploadId: UploadId) {
    try {
      this.logger.debug("Completing multipart upload", { Key, uploadId });

      const partsCom = new ListPartsCommand({
        Bucket: this.bucketName,
        Key,
        UploadId: uploadId,
      });
      const partsS3 = await this.s3.send(partsCom);

      const completeMultipartUploadCommand = new CompleteMultipartUploadCommand(
        {
          Bucket: this.bucketName,
          Key,
          UploadId: uploadId,
          MultipartUpload: {
            Parts: partsS3.Parts,
          },
        }
      );

      const uploadResponse = await this.s3.send(completeMultipartUploadCommand);
      if (!uploadResponse.ETag) {
        throw Error("No ETag returned from S3");
      }

      return JSON.parse(uploadResponse.ETag);
    } catch (error) {
      this.logger.error("Failed to complete multipart upload!", {
        error,
        Key,
        uploadId,
      });
      return handleS3MultipartUploadError(error, uploadId);
    }
  }

  public async getObject(
    Key: string,
    Range?: string
  ): Promise<{ readable: Readable; etag: string | undefined }> {
    this.logger.debug(`Getting read stream for S3 object...`, {
      Key,
      Bucket: this.bucketName,
    });

    try {
      const getObjectResponse: GetObjectOutput = await this.s3.send(
        new GetObjectCommand({
          Key,
          Bucket: this.bucketName,
          Range,
        })
      );
      if (!getObjectResponse.Body) {
        throw Error("No object found");
      }
      const readableStream = getObjectResponse.Body as Readable;
      return {
        readable: readableStream.on("error", (err: Error) => {
          this.logger.error(`Failed to stream object!`, { err, Key, Range });
        }),
        etag: getObjectResponse.ETag,
      };
    } catch (error) {
      this.logger.debug(`Failed to get object!`, { error, Key, Range });
      throw error;
    }
  }

  public async getObjectByteCount(Key: string): Promise<number> {
    this.logger.debug(`Getting byte count for S3 object...`, {
      Key,
      Bucket: this.bucketName,
    });
    const getHeadResponse: HeadObjectOutput = await this.s3.send(
      new HeadObjectCommand({
        Key,
        Bucket: this.bucketName,
      })
    );
    return getHeadResponse.ContentLength ?? 0;
  }

  public async removeObject(Key: string): Promise<void> {
    this.logger.debug(`Deleting S3 object...`, {
      Key,
      Bucket: this.bucketName,
    });
    await this.s3.send(
      new DeleteObjectCommand({
        Bucket: this.bucketName,
        Key,
      })
    );
  }

  private s3CommandParamsFromOptions(Options: ObjectStoreOptions): {
    Bucket: string;
    ContentType?: string;
    ContentLength?: number;
    Metadata?: Record<string, string>;
  } {
    const { contentType, contentLength, payloadInfo } = Options;

    return {
      Bucket: this.bucketName,
      ...(contentType ? { ContentType: contentType } : {}),
      ...(contentLength ? { ContentLength: contentLength } : {}),
      ...(payloadInfo
        ? {
            Metadata: {
              [payloadDataStartS3MetaDataTag]: `${payloadInfo.payloadDataStart}`,
              [payloadContentTypeS3MetaDataTag]: payloadInfo.payloadContentType,
            },
          }
        : {}),
    };
  }

  public async moveObject({
    sourceKey,
    destinationKey,
    Options,
  }: MoveObjectParams): Promise<void> {
    const params = {
      ...this.s3CommandParamsFromOptions(Options),
      CopySource: `${this.bucketName}/${sourceKey}`,
      Key: destinationKey,
      MetadataDirective: "REPLACE",
    };

    const fnLogger = this.logger.child({
      sourceKey,
      destinationKey,
      bucket: this.bucketName,
    });

    fnLogger.debug(`Moving S3 object...`, {
      ...params,
    });

    try {
      const headRequest = new HeadObjectCommand({
        Bucket: this.bucketName,
        Key: sourceKey,
      });
      const headResponse = await this.s3.send(headRequest);
      const startTime = Date.now();
      if (
        headResponse.ContentLength &&
        headResponse.ContentLength > this.multipartCopyObjectLimitBytes
      ) {
        fnLogger.debug("Copying large object via multipart copy", {
          contentLength: headResponse.ContentLength,
        });
        await this.copyLargeObject({
          contentLength: headResponse.ContentLength,
          partSize: this.multipartCopyObjectLimitBytes,
          sourceKey,
          destinationKey,
        });
      } else {
        fnLogger.debug(`Copying object directly to source bucket`, {
          contentLength: headResponse.ContentLength,
        });
        await this.s3.send(new CopyObjectCommand(params));
      }
      fnLogger.debug("Successfully copied object...", {
        contentLength: headResponse.ContentLength,
        durationMs: Date.now() - startTime,
      });
      // delete object after copying
      await this.s3.send(
        new DeleteObjectCommand({
          Bucket: this.bucketName,
          Key: sourceKey,
        })
      );
      fnLogger.debug(`Moved S3 object!`, {
        sourceKey,
        destinationKey,
      });
    } catch (error) {
      fnLogger.error(`Failed to move object!`, {
        error,
      });
      throw error;
    }
  }

  public async getMultipartUploadParts(
    Key: string,
    uploadId: UploadId
  ): Promise<
    {
      size: number;
      partNumber: number;
    }[]
  > {
    try {
      const getPartsCommand = new ListPartsCommand({
        Bucket: this.bucketName,
        Key,
        UploadId: uploadId,
      });
      const partsS3 = await this.s3.send(getPartsCommand);
      if (!partsS3.Parts) {
        return [];
      }
      const parts = partsS3.Parts.filter(
        (part) => part && part.PartNumber && part.Size
      ).map((part) => {
        return {
          // we filtered but to avoid type errors do if check
          /* eslint-disable @typescript-eslint/no-non-null-assertion */
          size: +part.Size!,
          partNumber: +part.PartNumber!,
          /* eslint-enable */
        };
      });
      return parts;
    } catch (error) {
      this.logger.debug("Failed to get multipart upload chunks!", {
        error,
        Key,
        uploadId,
      });
      return handleS3MultipartUploadError(error, uploadId);
    }
  }

  // helper function specific to S3 large file copies
  private async copyLargeObject({
    contentLength,
    partSize,
    sourceKey,
    destinationKey,
  }: {
    contentLength: number;
    partSize: number;
    sourceKey: string;
    destinationKey: string;
  }): Promise<CompleteMultipartUploadCommandOutput> {
    // Start the multipart upload to get the upload ID
    const createCommand = new CreateMultipartUploadCommand({
      Bucket: this.bucketName,
      Key: destinationKey,
    });
    const { UploadId } = await this.s3.send(createCommand);
    const fnLogger = this.logger.child({
      sourceKey,
      destinationKey,
      bucket: this.bucketName,
      contentLength,
      uploadId: UploadId,
      partSize,
    });
    fnLogger.debug("Starting large object copy...");
    const uploadPartCommands: UploadPartCopyCommand[] = [];
    const parallelLimit = pLimit(this.multipartCopyParallelLimit); // move 10 parts at a a time

    // create our chunked promises
    for (
      let currentByte = 0;
      currentByte < contentLength;
      currentByte += partSize
    ) {
      const partNumber = Math.floor(currentByte / partSize) + 1;
      const copyRange = `bytes=${currentByte}-${Math.min(
        currentByte + partSize - 1,
        contentLength - 1
      )}`;
      const uploadPartCopyCommand = new UploadPartCopyCommand({
        Bucket: this.bucketName,
        CopySource: `${this.bucketName}/${sourceKey}`,
        Key: destinationKey,
        PartNumber: partNumber,
        UploadId,
        CopySourceRange: copyRange,
      });
      uploadPartCommands.push(uploadPartCopyCommand);
    }
    const parts = await Promise.all(
      uploadPartCommands.map((uploadPartCommand: UploadPartCopyCommand) =>
        parallelLimit(() => {
          fnLogger.debug("Copying part of large object...", {
            ...uploadPartCommand,
          });
          return this.s3.send(uploadPartCommand);
        })
      )
    );
    const partTags = parts.map((part, index) => {
      return {
        ETag: part.CopyPartResult?.ETag,
        PartNumber: index + 1,
      };
    });

    // Complete the multipart upload
    const completeCommand = new CompleteMultipartUploadCommand({
      Bucket: this.bucketName,
      Key: destinationKey,
      UploadId,
      MultipartUpload: { Parts: partTags },
    });
    fnLogger.debug("Completing large object copy...");
    return this.s3.send(completeCommand);
  }

  public async headObject(Key: string): Promise<{
    etag: string | undefined;
    ContentLength: number;
    ContentType: string | undefined;
  }> {
    this.logger.debug(`Heading S3 object...`, {
      Key,
      Bucket: this.bucketName,
    });
    const {
      ETag: etag,
      ContentLength,
      ContentType,
    } = await this.s3.send(
      new HeadObjectCommand({
        Bucket: this.bucketName,
        Key,
      })
    );
    return {
      etag,
      ContentLength: ContentLength ?? 0,
      ContentType,
    };
  }
}
