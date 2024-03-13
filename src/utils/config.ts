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
import { config } from "dotenv";

import { getSSMParameter } from "./getArweaveWallet";

export async function loadConfig() {
  // load any local environment variables
  config();

  if (!["dev", "prod"].includes(process.env.NODE_ENV ?? "")) {
    // Only get AWS secrets in dev or prod environments
    return;
  }

  process.env.HONEYCOMB_API_KEY = await getSSMParameter(
    "honeycomb-events-api-key"
  );
}
