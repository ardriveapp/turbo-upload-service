#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./analyze_parquet_s3.sh "s3://my-bucket/backfill/perm_items_*.parquet"
# (You can also pass a local glob; this script works for both.)
PARQUET_GLOB="${1:?Provide an S3 or local glob, e.g. s3://bucket/prefix/perm_items_*.parquet}"

# Optional env for DuckDB S3 config (DuckDB also picks up standard AWS env automatically)
#   S3_REGION=us-east-1
#   S3_USE_SSL=true|false
#   S3_URL_STYLE=path|vhost
S3_REGION="${S3_REGION:-}"
S3_USE_SSL="${S3_USE_SSL:-true}"
S3_URL_STYLE="${S3_URL_STYLE:-path}"

duckdb -csv -header <<SQL
INSTALL httpfs;
LOAD httpfs;

-- Configure S3 if provided (DuckDB will also use AWS_* env or profile/IMDS)
SET s3_use_ssl='${S3_USE_SSL}';
${S3_REGION:+SET s3_region='${S3_REGION}';}
${S3_URL_STYLE:+SET s3_url_style='${S3_URL_STYLE}';}

WITH src AS (
  SELECT * FROM read_parquet('${PARQUET_GLOB}')
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
