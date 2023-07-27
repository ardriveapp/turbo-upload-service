"use-strict";

const esbuild = require("esbuild");
const lambdas = [
  "plan",
  "prepare",
  "post",
  "seed",
  "verify",
  "optical-post",
  "unbundle-bdi",
];

lambdas.forEach((lambda) => {
  esbuild
    .build({
      platform: "node",
      entryPoints: [`lib/jobs/${lambda}.js`],
      target: "node16",
      bundle: true,
      outfile: `lib/jobs/${lambda}-min.js`,
      external: [
        "pg-native",
        "sqlite3",
        "mysql2",
        "oracledb",
        "better-sqlite3",
        "mysql",
        "tedious",
      ],
    })
    .catch(() => process.exit(1));
});
