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
import { Tag, byteArrayToLong, deserializeTags } from "arbundles";
import { Readable } from "stream";
import winston from "winston";

import { ownerToAddress, sha256B64Url, toB64Url } from "../utils/base64";
import { createVerifiedDataItemStream } from "./verifyDataItem";

// Takes a Readable stream of data item bytes and provides Promise interfaces for its member data
export class StreamingDataItem {
  private signatureTypePromise: Promise<number>;
  private signatureTypeResolver?: (signatureType: number) => void;
  private signaturePromise: Promise<Buffer>;
  private signatureResolver?: (signature: Buffer) => void;
  private ownerPromise: Promise<Buffer>;
  private ownerResolver?: (owner: Buffer) => void;
  private targetFlagPromise: Promise<number>;
  private targetFlagResolver?: (targetFlag: number) => void;
  private targetPromise: Promise<string | undefined>;
  private targetResolver?: (target: string | undefined) => void; // TODO: Arweave public address?
  private anchorFlagPromise: Promise<number>;
  private anchorFlagResolver?: (anchorFlag: number) => void;
  private anchorPromise: Promise<string | undefined>;
  private anchorResolver?: (anchor: string | undefined) => void;
  private numTagsPromise: Promise<number>;
  private numTagsResolver?: (numTags: number) => void;
  private numTagsBytesPromise: Promise<number>;
  private numTagsBytesResolver?: (numTagsBytes: number) => void;
  private tagsBytesPromise: Promise<Buffer | undefined>;
  private tagsBytesResolver?: (tagsBytes: Buffer | undefined) => void;
  private payloadStreamPromise: Promise<Readable>;
  private payloadStreamResolver?: (payloadStream: Readable) => void;
  private payloadSizePromise: Promise<number>;
  private payloadSizeResolver?: (payloadSize: number) => void;
  private isValidPromise: Promise<boolean>;
  private isValidResolver?: (isValid: boolean) => void;
  private lastError?: Error;

  constructor(
    dataItemStream: Readable,
    private log?: winston.Logger,
    private failOnTagsSpecViolation = true,
    private failOnEmptyStringsInTags = false
  ) {
    this.signatureTypePromise = new Promise((resolve) => {
      this.signatureTypeResolver = resolve;
    });
    this.signaturePromise = new Promise((resolve) => {
      this.signatureResolver = resolve;
    });
    this.ownerPromise = new Promise((resolve) => {
      this.ownerResolver = resolve;
    });
    this.targetFlagPromise = new Promise((resolve) => {
      this.targetFlagResolver = resolve;
    });
    this.targetPromise = new Promise((resolve) => {
      this.targetResolver = resolve;
    });
    this.anchorFlagPromise = new Promise((resolve) => {
      this.anchorFlagResolver = resolve;
    });
    this.anchorPromise = new Promise((resolve) => {
      this.anchorResolver = resolve;
    });
    this.numTagsPromise = new Promise((resolve) => {
      this.numTagsResolver = resolve;
    });
    this.numTagsBytesPromise = new Promise((resolve) => {
      this.numTagsBytesResolver = resolve;
    });
    this.tagsBytesPromise = new Promise((resolve) => {
      this.tagsBytesResolver = resolve;
    });
    this.payloadStreamPromise = new Promise((resolve) => {
      this.payloadStreamResolver = resolve;
    });
    this.payloadSizePromise = new Promise((resolve) => {
      this.payloadSizeResolver = resolve;
    });
    this.isValidPromise = new Promise((resolve) => {
      this.isValidResolver = resolve;
    });

    this.setupDataItemEventEmitter(dataItemStream, log);
  }

  private setupDataItemEventEmitter(
    dataItemStream: Readable,
    log?: winston.Logger
  ) {
    const emitter = createVerifiedDataItemStream(dataItemStream, log);

    // Cache any errors arising from the emitter for later use by promise handlers
    emitter.on("error", (error: Error) => {
      log?.error(`RECEIVED AN EMITTER ERROR!`, error);
      this.lastError = error;
    });

    // Resolve promises with emitted event data
    emitter.once("signatureType", (signatureBuffer: Buffer) => {
      const sigType = byteArrayToLong(signatureBuffer);
      this.signatureTypeResolver?.(sigType);
    });
    emitter.once("signature", (signatureBytes: Buffer) => {
      this.signatureResolver?.(signatureBytes);
    });
    emitter.once("owner", (ownerBytes: Buffer) => {
      this.ownerResolver?.(ownerBytes);
    });
    emitter.once("targetFlag", (targetFlagBuffer: Buffer) => {
      const targetFlag = byteArrayToLong(targetFlagBuffer);
      this.targetFlagResolver?.(targetFlag);
      if (targetFlag === 0) {
        this.targetResolver?.(undefined);
      }
    });
    emitter.once("target", (targetBytes: Buffer) => {
      this.targetResolver?.(toB64Url(targetBytes));
    });
    emitter.once("anchorFlag", (anchorFlagBuffer: Buffer) => {
      const anchorFlag = byteArrayToLong(anchorFlagBuffer);
      this.anchorFlagResolver?.(anchorFlag);
      if (anchorFlag === 0) {
        this.anchorResolver?.(undefined);
      }
    });
    emitter.once("anchor", (anchorBytes: Buffer) => {
      this.anchorResolver?.(anchorBytes.toString());
    });
    emitter.once("numTags", (numTagsBuffer: Buffer) => {
      const numTags = byteArrayToLong(numTagsBuffer);
      this.numTagsResolver?.(numTags);
    });
    emitter.once("numTagsBytes", (numTagsBytesBuffer: Buffer) => {
      const numTagsBytes = byteArrayToLong(numTagsBytesBuffer);
      this.numTagsBytesResolver?.(numTagsBytes);
      if (numTagsBytes === 0) {
        this.tagsBytesResolver?.(undefined);
      }
    });
    emitter.once("tagsBytes", (tagsBytesBuffer: Buffer) => {
      this.tagsBytesResolver?.(tagsBytesBuffer);
    });
    emitter.once("payloadStream", async (payloadStream: Readable) => {
      this.payloadStreamResolver?.(payloadStream);
    });
    emitter.once("payloadSize", async (payloadSize: number) => {
      this.payloadSizeResolver?.(payloadSize);
    });
    emitter.once("isValid", (isValid: boolean) => {
      this.isValidResolver?.(isValid);
    });
  }

  getSignatureType(): Promise<number> {
    return this.signatureTypePromise.then((signatureType) => {
      if (!this.lastError) {
        return signatureType;
      }
      throw this.lastError;
    });
  }

  getSignature(): Promise<string> {
    return this.signaturePromise.then(toB64Url).then((signature) => {
      if (!this.lastError) {
        return signature;
      }
      throw this.lastError;
    });
  }

  getDataItemId(): Promise<string> {
    return this.signaturePromise.then(sha256B64Url).then((dataItemId) => {
      if (!this.lastError) {
        return dataItemId;
      }
      throw this.lastError;
    });
  }

  getOwnerBytes(): Promise<Buffer> {
    return this.ownerPromise.then((owner) => {
      if (!this.lastError) {
        return owner;
      }
      throw this.lastError;
    });
  }

  /**
   * Returns the base64url string representation of the data item's 'owner' bytes
   */
  getOwner(): Promise<string> {
    return this.getOwnerBytes().then((owner) => {
      return owner.toString("base64url");
    });
  }

  /**
   * Returns a normalized representation of the owner address. For Arweave-signed
   * data items, this is the Arweave wallet address. For other chains, it's simply
   * a normalized representation and is the address used by gateway GQL
   */
  getOwnerAddress(): Promise<string> {
    return this.getOwner().then(ownerToAddress);
  }

  /**
   * Returns the base64url string representation of the data item's 'target' bytes
   */
  getTargetFlag(): Promise<number> {
    return this.targetFlagPromise.then((targetFlag) => {
      if (!this.lastError) {
        return targetFlag;
      }
      throw this.lastError;
    });
  }

  getTarget(): Promise<string | undefined> {
    return this.targetPromise.then((target) => {
      if (!this.lastError) {
        return target;
      }
      throw this.lastError;
    });
  }

  /**
   * Returns the base64url string representation of the data item's 'anchor' bytes
   */
  getAnchorFlag(): Promise<number> {
    return this.anchorFlagPromise.then((anchorFlag) => {
      if (!this.lastError) {
        return anchorFlag;
      }
      throw this.lastError;
    });
  }

  getAnchor(): Promise<string | undefined> {
    return this.anchorPromise.then((anchor) => {
      if (!this.lastError) {
        return anchor;
      }
      throw this.lastError;
    });
  }

  getNumTags(): Promise<number> {
    return this.numTagsPromise.then((numTags) => {
      if (this.lastError) {
        throw this.lastError;
      }

      if (numTags > 128) {
        const errMsg = `ANS-104 spec violation! A data item may only contain up to 128 tags! Parsed ${numTags} tags!`;

        if (this.failOnTagsSpecViolation) {
          this.lastError = new Error(errMsg);
          throw this.lastError;
        }
        this.log?.warn(errMsg);
      }

      return numTags;
    });
  }

  getNumTagsBytes(): Promise<number> {
    return this.numTagsBytesPromise.then((numTagsBytes) => {
      if (!this.lastError) {
        return numTagsBytes;
      }
      throw this.lastError;
    });
  }

  getTagsBytes(): Promise<Buffer | undefined> {
    return this.tagsBytesPromise.then((tagsBytes) => {
      if (!this.lastError) {
        return tagsBytes;
      }
      throw this.lastError;
    });
  }

  getTags(): Promise<Tag[]> {
    return this.tagsBytesPromise.then((tagsBytes: Buffer | undefined) => {
      if (!tagsBytes || tagsBytes.length === 0) {
        return [];
      }

      // Remove avsc types
      const tags = JSON.parse(
        JSON.stringify(deserializeTags(tagsBytes || Buffer.from("")))
      ) as { name: string; value: string }[];

      // Assert ANS-104 spec
      for (const tag of tags) {
        const tagNameLength = Buffer.from(tag.name).byteLength;
        if (tagNameLength > 1024) {
          const errMsg = `ANS-104 spec violation! A data item tag name must not exceed 1024 bytes in size! Parsed size was ${tagNameLength}`;
          if (this.failOnTagsSpecViolation) {
            this.lastError = new Error(errMsg);
            throw new Error(errMsg);
          }
          this.log?.warn(errMsg);
        } else if (tag.name === "") {
          const errMsg = `ANS-104 spec violation! A data item tag name may not be an empty string!`;
          if (this.failOnEmptyStringsInTags) {
            this.lastError = new Error(errMsg);
            throw new Error(errMsg);
          }
          this.log?.warn(errMsg);
        }

        const tagValueLength = Buffer.from(tag.value).byteLength;
        if (tagValueLength > 3072) {
          const errMsg = `ANS-104 spec violation! A data item tag value must not exceed 3072 bytes in size! Parsed size was ${tagValueLength}`;
          if (this.failOnTagsSpecViolation) {
            this.lastError = new Error(errMsg);
            throw new Error(errMsg);
          }
          this.log?.warn(errMsg);
        } else if (tag.value === "") {
          const errMsg = `ANS-104 spec violation! A data item tag value may not be an empty string!`;
          if (this.failOnEmptyStringsInTags) {
            this.lastError = new Error(errMsg);
            throw new Error(errMsg);
          }
          this.log?.warn(errMsg);
        }
      }

      return tags;
    });
  }

  getPayloadStream(): Promise<Readable> {
    return this.payloadStreamPromise
      .then((payloadStream) =>
        // TODO: Necessary?
        payloadStream.resume()
      )
      .then((payloadStream) => {
        if (!this.lastError) {
          return payloadStream;
        }
        throw this.lastError;
      });
  }

  // NOTE: Will only resolve if the payloadStream has been fully consumed or an error is thrown
  getPayloadSize(): Promise<number> {
    return this.payloadSizePromise.then((payloadSize) => {
      if (!this.lastError) {
        return payloadSize;
      }
      throw this.lastError;
    });
  }

  isValid(): Promise<boolean> {
    return this.isValidPromise.then((isValid) => {
      if (!this.lastError) {
        return isValid;
      }
      throw this.lastError;
    });
  }
}
