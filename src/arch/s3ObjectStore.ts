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
import {
  CompleteMultipartUploadCommand,
  CompleteMultipartUploadCommandOutput,
  CopyObjectCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  GetObjectOutput,
  HeadObjectCommand,
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

const awsAccountId = process.env.AWS_ACCOUNT_ID;
const awsCredentials =
  process.env.AWS_ACCESS_KEY_ID !== undefined &&
  process.env.AWS_SECRET_ACCESS_KEY !== undefined
    ? {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
        ...(process.env.AWS_SESSION_TOKEN
          ? {
              sessionToken: process.env.AWS_SESSION_TOKEN,
            }
          : {}),
      }
    : undefined;

/* eslint-disable @typescript-eslint/no-explicit-any*/
export const handleS3MultipartUploadError = (
  error: unknown,
  uploadId: UploadId
) => {
  const message = error instanceof Error ? error.message : "Unknown error.";
  if (error && "Code" in (error as any)) {
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
  } else {
    // fallback to message parsing if AWS SDK does not provide an error code
    switch (true) {
      case message.includes("The specified upload does not exist"):
        throw new MultiPartUploadNotFound(uploadId);
      // TODO: other known AWS error messages that can happen outside of standard error codes
      default:
        throw error;
    }
  }
};
/* eslint-enable @typescript-eslint/no-explicit-any */

// Build a map of bucket regions to their respective clients based on env vars
const endpoint = process.env.AWS_ENDPOINT;
const forcePathStyle = process.env.S3_FORCE_PATH_STYLE;
type BucketRegion = string;
const regionsToClients: Record<BucketRegion, S3Client> = {};

[process.env.DATA_ITEM_BUCKET_REGION, process.env.BACKUP_BUCKET_REGION].forEach(
  (region) => {
    if (!region) return;
    if (!regionsToClients[region]) {
      regionsToClients[region] = new S3Client({
        requestHandler: new NodeHttpHandler({
          httpsAgent: new https.Agent({
            keepAlive: true,
            timeout: 0,
          }),
          requestTimeout: 0,
        }),
        region,
        ...(endpoint
          ? {
              endpoint,
            }
          : {}),
        ...(awsCredentials
          ? {
              credentials: awsCredentials,
            }
          : {}),
        ...(forcePathStyle !== undefined
          ? { forcePathStyle: forcePathStyle === "true" }
          : {}),
      });
    }
  }
);

// Build a map of bucket names to their respective regions based on env vars
type BucketName = string;
const bucketNameToRegionMap: Record<BucketName, BucketRegion> = {};
if (process.env.DATA_ITEM_BUCKET) {
  bucketNameToRegionMap[process.env.DATA_ITEM_BUCKET] =
    process.env.DATA_ITEM_BUCKET_REGION ??
    process.env.AWS_REGION ??
    "us-east-1";
}
if (process.env.BACKUP_DATA_ITEM_BUCKET) {
  bucketNameToRegionMap[process.env.BACKUP_DATA_ITEM_BUCKET] =
    process.env.BACKUP_BUCKET_REGION ?? process.env.AWS_REGION ?? "us-east-1";
}

const defaultS3Client = new S3Client({
  requestHandler: new NodeHttpHandler({
    httpsAgent: new https.Agent({
      keepAlive: true,
      timeout: 0,
    }),
    requestTimeout: 0,
  }),
  region: process.env.AWS_REGION ?? "us-east-1",
  ...(endpoint
    ? {
        endpoint,
      }
    : {}),
  ...(awsCredentials
    ? {
        credentials: awsCredentials,
      }
    : {}),
  ...(forcePathStyle !== undefined
    ? { forcePathStyle: forcePathStyle === "true" }
    : {}),
});

function s3ClientForBucket(bucketName: string): S3Client {
  const region =
    bucketNameToRegionMap[bucketName] ?? process.env.AWS_REGION ?? "us-east-1";
  return regionsToClients[region] ?? defaultS3Client;
}

export class S3ObjectStore implements ObjectStore {
  private bucketName: string;
  private backupBucketName: string | undefined;
  private logger: winston.Logger;
  private multipartCopyObjectLimitBytes = 1024 * 1024 * 1024 * 5; // 5GiB limit for AWS S3 `CopyObject` operation
  private multipartCopyParallelLimit = 10;

  constructor({
    s3Client,
    // TODO: add otel tracer to track events
    bucketName,
    backupBucketName,
    logger = globalLogger,
  }: {
    s3Client?: S3Client;
    bucketName: string;
    backupBucketName?: string;
    logger?: winston.Logger;
  }) {
    if (s3Client) {
      if (typeof s3Client.config.region === "string") {
        regionsToClients[s3Client.config.region] = s3Client;
      } else {
        // We can't await on the call to fetch the region here so just... do our best :(
        regionsToClients[process.env.AWS_REGION ?? "us-east-1"] = s3Client;
      }
    }
    this.bucketName = bucketName;
    this.backupBucketName = backupBucketName;
    this.logger = logger.child({
      bucketName: this.bucketName,
      objectStore: "S3ObjectStore",
    });
  }

  public async getObjectPayloadInfo(Key: string): Promise<PayloadInfo> {
    try {
      const headObjectResponse = await this.headObject(Key);

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
      ...this.s3CommandParamsFromOptions(Options, this.bucketName),
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
      client: s3ClientForBucket(this.bucketName),
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
      let Metadata;
      let Tagging: string | undefined;
      if (awsAccountId) {
        Metadata = {
          uploader: awsAccountId,
        };
        Tagging = `uploader=${encodeURIComponent(awsAccountId)}`;
      }

      // Step 1: Start the multipart upload and get the upload ID
      const newUploadCommand = new CreateMultipartUploadCommand({
        Bucket: this.bucketName,
        Key,
        Metadata,
        Tagging,
      });
      const createMultipartUploadResponse = await s3ClientForBucket(
        this.bucketName
      ).send(newUploadCommand);
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
    this.logger.debug("Uploading part", {
      Key,
      uploadId,
      partNumber,
      ContentLength,
    });

    const attemptUploadPart = async (bucketName: string) => {
      const uploadPartCommand = new UploadPartCommand({
        UploadId: uploadId,
        Bucket: bucketName,
        Key,
        Body,
        PartNumber: partNumber,
        ContentLength,
      });
      return await s3ClientForBucket(bucketName).send(uploadPartCommand);
    };
    try {
      const uploadResponse = await attemptUploadPart(this.bucketName).catch(
        async (error) => {
          if (
            error instanceof Error &&
            error.name === "NoSuchUpload" &&
            this.backupBucketName
          ) {
            return await attemptUploadPart(this.backupBucketName);
          }
          throw error;
        }
      );

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
    this.logger.debug("Completing multipart upload", { Key, uploadId });

    const attemptCompleteMultipartUpload = async (bucketName: string) => {
      const partsCom = new ListPartsCommand({
        Bucket: bucketName,
        Key,
        UploadId: uploadId,
      });
      const partsS3 = await s3ClientForBucket(bucketName).send(partsCom);

      const completeMultipartUploadCommand = new CompleteMultipartUploadCommand(
        {
          Bucket: bucketName,
          Key,
          UploadId: uploadId,
          MultipartUpload: {
            Parts: partsS3.Parts,
          },
        }
      );

      return await s3ClientForBucket(bucketName).send(
        completeMultipartUploadCommand
      );
    };

    try {
      const uploadResponse = await attemptCompleteMultipartUpload(
        this.bucketName
      ).catch(async (error) => {
        if (
          error instanceof Error &&
          error.name === "NoSuchUpload" &&
          this.backupBucketName
        ) {
          return await attemptCompleteMultipartUpload(this.backupBucketName);
        }
        throw error;
      });
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

    const attemptGetObject = async (
      bucketName: string
    ): Promise<GetObjectOutput> => {
      const getObjectResponse = await s3ClientForBucket(bucketName).send(
        new GetObjectCommand({
          Key,
          Bucket: bucketName,
          Range,
        })
      );
      if (!getObjectResponse.Body) {
        throw Error("No object found");
      }
      return getObjectResponse;
    };

    try {
      const getObjectResponse = await attemptGetObject(this.bucketName).catch(
        async (error) => {
          if (
            error instanceof Error &&
            ["NoSuchKey", "AccessDenied"].includes(error.name) &&
            this.backupBucketName
          ) {
            return await attemptGetObject(this.backupBucketName);
          }
          throw error;
        }
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
    const getHeadResponse = await this.headObject(Key);
    return getHeadResponse.ContentLength ?? 0;
  }

  public async removeObject(Key: string): Promise<void> {
    const attemptDeleteObject = async (bucketName: string) => {
      this.logger.info(`Deleting S3 object...`, {
        Key,
        Bucket: bucketName,
      });

      await s3ClientForBucket(bucketName).send(
        new DeleteObjectCommand({
          Bucket: bucketName,
          Key,
        })
      );
    };

    await attemptDeleteObject(this.bucketName).catch(async (error) => {
      if (
        error instanceof Error &&
        error.name === "NotFound" &&
        this.backupBucketName
      ) {
        return await attemptDeleteObject(this.backupBucketName);
      }
      throw error;
    });
  }

  private s3CommandParamsFromOptions(
    Options: ObjectStoreOptions,
    bucket: string
  ): {
    Bucket: string;
    ContentType?: string;
    ContentLength?: number;
    Metadata?: Record<string, string>;
    Tagging?: string;
  } {
    const { contentType, contentLength, payloadInfo } = Options;

    const Metadata: Record<string, string> = {};
    let Tagging: string | undefined;
    if (payloadInfo) {
      Metadata[
        payloadDataStartS3MetaDataTag
      ] = `${payloadInfo.payloadDataStart}`;
      Metadata[payloadContentTypeS3MetaDataTag] =
        payloadInfo.payloadContentType;
    }
    if (awsAccountId) {
      Metadata.uploader = awsAccountId;
      Tagging = `uploader=${encodeURIComponent(awsAccountId)}`;
    }

    return {
      Bucket: bucket,
      ...(contentType ? { ContentType: contentType } : {}),
      ...(contentLength ? { ContentLength: contentLength } : {}),
      ...(Metadata
        ? {
            Metadata,
          }
        : {}),
      Tagging,
    };
  }

  public async moveObject({
    sourceKey,
    destinationKey,
    Options,
  }: MoveObjectParams): Promise<void> {
    const destinationBucketName = this.bucketName;
    let fnLogger = this.logger.child({
      sourceKey,
      destinationKey,
      destinationBucketName,
    });

    const attemptMoveObject = async (sourceBucketName: string) => {
      fnLogger = fnLogger.child({ sourceBucketName });
      const params = {
        ...this.s3CommandParamsFromOptions(Options, destinationBucketName),
        CopySource: `${sourceBucketName}/${encodeURIComponent(sourceKey)}`,
        Key: destinationKey,
        MetadataDirective: "REPLACE",
        TaggingDirective: "COPY",
      };

      fnLogger.debug(`Moving S3 object...`, {
        ...params,
      });
      const headRequest = new HeadObjectCommand({
        Bucket: sourceBucketName,
        Key: sourceKey,
      });
      const headResponse = await s3ClientForBucket(sourceBucketName).send(
        headRequest
      );
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
          sourceBucketName,
          sourceKey,
          destinationBucketName,
          destinationKey,
        });
      } else {
        fnLogger.debug(`Copying object directly to source bucket`, {
          contentLength: headResponse.ContentLength,
        });
        await s3ClientForBucket(destinationBucketName).send(
          new CopyObjectCommand(params)
        );
      }
      fnLogger.debug("Successfully copied object...", {
        contentLength: headResponse.ContentLength,
        durationMs: Date.now() - startTime,
      });
      // delete object after copying
      await s3ClientForBucket(sourceBucketName).send(
        new DeleteObjectCommand({
          Bucket: sourceBucketName,
          Key: sourceKey,
        })
      );
      fnLogger.debug(`Moved S3 object!`, {
        sourceKey,
        destinationKey,
      });
    };
    try {
      await attemptMoveObject(this.bucketName).catch(async (error) => {
        if (
          error instanceof Error &&
          error.name === "NotFound" &&
          this.backupBucketName
        ) {
          return await attemptMoveObject(this.backupBucketName);
        }
        throw error;
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
    const attemptGetMultipartUploadParts = async (
      bucketName: string
    ): Promise<
      {
        size: number;
        partNumber: number;
      }[]
    > => {
      const getPartsCommand = new ListPartsCommand({
        Bucket: bucketName,
        Key,
        UploadId: uploadId,
      });
      const partsS3 = await s3ClientForBucket(bucketName).send(getPartsCommand);
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
    };

    try {
      return attemptGetMultipartUploadParts(this.bucketName).catch(
        async (error) => {
          if (
            error instanceof Error &&
            error.name === "NoSuchUpload" &&
            this.backupBucketName
          ) {
            return await attemptGetMultipartUploadParts(this.backupBucketName);
          }
          throw error;
        }
      );
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
    sourceBucketName,
    sourceKey,
    destinationBucketName,
    destinationKey,
  }: {
    contentLength: number;
    partSize: number;
    sourceBucketName: string;
    sourceKey: string;
    destinationBucketName: string;
    destinationKey: string;
  }): Promise<CompleteMultipartUploadCommandOutput> {
    let Metadata;
    if (awsAccountId) {
      Metadata = {
        uploader: awsAccountId,
      };
    }

    // Start the multipart upload to get the upload ID
    const createCommand = new CreateMultipartUploadCommand({
      Bucket: destinationBucketName,
      Key: destinationKey,
      Metadata,
    });
    const { UploadId } = await s3ClientForBucket(destinationBucketName).send(
      createCommand
    );
    const fnLogger = this.logger.child({
      sourceKey,
      destinationKey,
      bucket: destinationBucketName,
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
        Bucket: destinationBucketName,
        CopySource: `${sourceBucketName}/${encodeURIComponent(sourceKey)}`,
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
          return s3ClientForBucket(destinationBucketName).send(
            uploadPartCommand
          );
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
      Bucket: destinationBucketName,
      Key: destinationKey,
      UploadId,
      MultipartUpload: { Parts: partTags },
    });
    fnLogger.debug("Completing large object copy...");
    return s3ClientForBucket(destinationBucketName).send(completeCommand);
  }

  public async headObject(Key: string): Promise<{
    etag: string | undefined;
    ContentLength: number;
    ContentType: string | undefined;
    Metadata: Record<string, string> | undefined;
  }> {
    this.logger.debug(`Heading S3 object...`, {
      Key,
      Bucket: this.bucketName,
    });

    const attemptHeadObject = async (bucketName: string) => {
      const headObjectResponse = await s3ClientForBucket(bucketName).send(
        new HeadObjectCommand({
          Bucket: bucketName,
          Key,
        })
      );
      if (!headObjectResponse.ETag) {
        throw Error("No ETag returned from S3");
      }
      return {
        etag: headObjectResponse.ETag,
        ContentLength: headObjectResponse.ContentLength ?? 0,
        ContentType: headObjectResponse.ContentType,
        Metadata: headObjectResponse.Metadata,
      };
    };

    return await attemptHeadObject(this.bucketName).catch(async (error) => {
      if (
        error instanceof Error &&
        error.name === "NotFound" &&
        this.backupBucketName
      ) {
        return await attemptHeadObject(this.backupBucketName);
      }
      throw error;
    });
  }
}
