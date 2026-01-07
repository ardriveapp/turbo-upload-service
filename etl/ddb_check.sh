#!/usr/bin/env bash
set -euo pipefail

# Usage: ./analyze_parquet.sh "./parquet_out/perm_items_*.parquet"
PARQUET_GLOB="${1:-./parquet_out/perm_items_*.parquet}"

duckdb -csv -header <<SQL
WITH src AS (
  SELECT * FROM read_parquet('$PARQUET_GLOB')
),
dups AS (
  SELECT data_item_id, COUNT(*) AS cnt
  FROM src
  GROUP BY 1
  HAVING COUNT(*) > 1
)
SELECT 'distinct_bundle_id'            AS metric, COUNT(DISTINCT bundle_id)            AS value FROM src
UNION ALL SELECT 'distinct_data_item_id',         COUNT(DISTINCT data_item_id)         FROM src
UNION ALL SELECT 'distinct_owner_public_address', COUNT(DISTINCT owner_public_address) FROM src
UNION ALL SELECT 'distinct_plan_id',              COUNT(DISTINCT plan_id)              FROM src
UNION ALL SELECT 'duplicate_id_keys',             COUNT(*)                             FROM dups
UNION ALL SELECT 'duplicate_rows_excess',         COALESCE(SUM(cnt - 1), 0)            FROM dups
UNION ALL SELECT 'max_uploaded_date_epoch_ms',    CAST(EPOCH(MAX(uploaded_date))*1000 AS BIGINT) FROM src
UNION ALL SELECT 'min_uploaded_date_epoch_ms',    CAST(EPOCH(MIN(uploaded_date))*1000 AS BIGINT) FROM src
UNION ALL SELECT 'total_rows',                    COUNT(*)                             FROM src
ORDER BY metric;
SQL
