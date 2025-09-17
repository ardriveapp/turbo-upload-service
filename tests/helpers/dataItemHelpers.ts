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
import { ArweaveSigner, DataItem, Tag, createData } from "@dha-team/arbundles";
import { JWKInterface } from "arweave/node/lib/wallet";

export function generateJunkDataItem(
  dataSizeInKB: number,
  jwk: JWKInterface,
  tags: Tag[]
): Buffer {
  const dataAsBuffer = new Uint8Array(dataSizeInKB * 1024).map(() =>
    Math.floor(Math.random() * 256)
  );
  const signer = new ArweaveSigner(jwk);
  return createData(dataAsBuffer, signer, { tags }).getRaw();
}

export async function signDataItem(
  dataItem: Buffer,
  jwk: JWKInterface
): Promise<Buffer> {
  const signer = new ArweaveSigner(jwk);

  const item = new DataItem(dataItem);
  await item.sign(signer);

  return item.getRaw();
}
