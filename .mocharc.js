"use-strict";

process.env.NODE_ENV ??= "test";
process.env.PORT ??= 1234;
process.env.ARWEAVE_GATEWAY ??= "http://localhost:1984";

// Mocha configuration file
// Reference for options: https://github.com/mochajs/mocha/blob/master/example/config/.mocharc.js
module.exports = {
  extension: ["ts"],
  require: ["ts-node/register/transpile-only", "tests/testSetup.ts"],
  timeout: "10000", // 10 seconds
  parallel: true,
  recursive: true,
};
