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
import { Next } from "koa";

import { Architecture } from "../arch/architecture";
import { KoaContext } from "../server";

export async function architectureMiddleware(
  ctx: KoaContext,
  next: Next,
  arch: Omit<Architecture, "logger">
) {
  ctx.state.database = arch.database;
  ctx.state.objectStore = arch.objectStore;
  ctx.state.cacheService = arch.cacheService;
  ctx.state.paymentService = arch.paymentService;
  ctx.state.getArweaveWallet = arch.getArweaveWallet;
  ctx.state.getEVMDataItemSigningPrivateKey =
    arch.getEVMDataItemSigningPrivateKey;
  ctx.state.arweaveGateway = arch.arweaveGateway;
  ctx.state.pricingService = arch.pricingService;
  ctx.state.x402Service = arch.x402Service;
  ctx.state.tracer = arch.tracer;
  return next();
}
