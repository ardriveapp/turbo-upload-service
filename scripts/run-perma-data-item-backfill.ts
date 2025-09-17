import knex from "knex";

import { getWriterConfig } from "../src/arch/db/knexConfig";
import { backfillPermanentDataItems } from "../src/arch/db/migrator";

(async () => {
  // @ts-ignore
  await backfillPermanentDataItems(knex(getWriterConfig()));
  process.exit(0);
})();
