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
  DeleteObjectCommand,
  GetObjectCommand,
  GetObjectOutput,
  HeadObjectCommand,
  HeadObjectOutput,
  S3Client,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import { NodeHttpHandler } from "@aws-sdk/node-http-handler";
import * as https from "https";
import { Readable } from "multistream";

import { octetStreamContentType } from "../constants";
import logger from "../logger";
import { ObjectStore } from "./objectStore";

export class S3ObjectStore implements ObjectStore {
  private s3: S3Client;
  private bucketName: string;

  constructor({
    s3Client = new S3Client({
      requestHandler: new NodeHttpHandler({
        httpsAgent: new https.Agent({
          keepAlive: true,
        }),
      }),
    }),
    bucketName,
  }: {
    s3Client?: S3Client;
    bucketName: string;
  }) {
    this.s3 = s3Client;
    this.bucketName = bucketName;
  }

  public async putObject(
    Key: string,
    Body: Readable,
    Options?: {
      contentType?: string;
      contentLength?: number;
    }
  ): Promise<void> {
    const params = {
      Bucket: this.bucketName,
      Key,
      Body,
      ContentType: Options?.contentType ?? octetStreamContentType,
      // only set ContentLength if provided
      ...(Options?.contentLength
        ? { ContentLength: Options.contentLength }
        : {}),
    };

    // In order to upload streams, must use Upload instead of PutObjectCommand
    const putObject = new Upload({
      client: this.s3,
      params,
      queueSize: 1, // forces synchronous uploads
    });

    logger.info(`Putting read stream for S3 object...`, {
      Key,
      Bucket: this.bucketName,
    });

    // track upload progress
    putObject.on("httpUploadProgress", (progress) => {
      logger.info("Streaming object to S3...", {
        ...progress,
      });
    });

    try {
      await putObject.done();
    } catch (error) {
      logger.error("Failed to put object!", { error, params });
      throw error;
    }
  }

  public async getObject(Key: string, Range?: string): Promise<Readable> {
    logger.info(`Getting read stream for S3 object...`, {
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
      return readableStream.on("error", (err: Error) => {
        logger.error(`Failed to stream object!`, { err, Key, Range });
      });
    } catch (error) {
      logger.error(`Failed to get object!`, { error, Key, Range });
      throw error;
    }
  }

  public async getObjectByteCount(Key: string): Promise<number> {
    logger.info(`Getting byte count for S3 object...`, {
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
    logger.info(`Deleting S3 object...`, {
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
}
