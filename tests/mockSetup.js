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

// eslint-disable-next-line no-undef
const mock = require("mock-require"); // eslint-disable-line @typescript-eslint/no-var-requires

// Mock @coinbase/x402 to avoid ESM conflicts
// Only mock the package functions, not X402Service methods
mock("@coinbase/x402", {
  createFacilitatorConfig: () => ({
    url: "http://mock-facilitator",
    createAuthHeaders: () =>
      Promise.resolve({
        verify: { Authorization: "Bearer mock-token" },
        settle: { Authorization: "Bearer mock-token" },
        supported: { Authorization: "Bearer mock-token" },
        list: {},
      }),
  }),
  facilitator: {
    url: "http://mock-facilitator",
    createAuthHeaders: () => Promise.resolve({}),
  },
});

// Mock x402/verify to provide useFacilitator
mock("x402/verify", {
  useFacilitator: () => ({
    verify: () => Promise.resolve({ success: true }),
    settle: () => Promise.resolve({ success: true }),
    list: () => Promise.resolve([]),
    supported: () => Promise.resolve([]),
  }),
});

// Mock jose package to avoid ESM conflicts
mock("jose", {
  jwtVerify: () => Promise.resolve({ payload: { sub: "test" } }),
  SignJWT: class MockSignJWT {
    setProtectedHeader() {
      return this;
    }
    setIssuedAt() {
      return this;
    }
    setExpirationTime() {
      return this;
    }
    setAudience() {
      return this;
    }
    setSubject() {
      return this;
    }
    sign() {
      return Promise.resolve("mock-jwt-token");
    }
  },
});
