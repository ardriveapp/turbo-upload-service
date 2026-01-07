#!/usr/bin/env bash
set -euo pipefail

#FILE="parquet_out/perm_items_public.permanent_data_items_pre_12_2023_20230615T090209Z_v_qeQ_uk-20230620T150245Z_mNGzWbk4_rows-1000.parquet"
FILE="${1:?usage: $0 <parquet-file>}"
base="$(basename "$FILE")"

# Parse schema.table right before the start timestamp token
# Pattern: ..._<schema>.<table>_<startTs>_...
# Allow underscores in table by capturing greedily up to "_<timestamp>"
if [[ "$base" =~ ^.*_([^.]+)\.(.+)_([0-9]{8}T[0-9]{6}Z)_ ]]; then
  PG_SCHEMA_FROM_NAME="${BASH_REMATCH[1]}"
  PG_TABLE_FROM_NAME="${BASH_REMATCH[2]}"
else
  echo "❌ Could not parse schema.table from filename: $base" >&2
  exit 1
fi

# Parse key range & row count (anchored at end)
if [[ "$base" =~ _([0-9]{8}T[0-9]{6}Z)_([A-Za-z0-9_-]+)-([0-9]{8}T[0-9]{6}Z)_([A-Za-z0-9_-]+)_rows-([0-9]+)\.parquet$ ]]; then
  START_TS="${BASH_REMATCH[1]}"
  START_ID8="${BASH_REMATCH[2]}"
  END_TS="${BASH_REMATCH[3]}"
  END_ID8="${BASH_REMATCH[4]}"
  N_EXPECTED="${BASH_REMATCH[5]}"
else
  echo "❌ Could not parse key range from filename: $base" >&2
  exit 1
fi

# Safe defaults (so set -u doesn’t explode) + allow env overrides
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-postgres}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-}"   # optional; leave empty if you use trust/SSL or .pgpass
PG_SCHEMA="${PG_SCHEMA-${PG_SCHEMA_FROM_NAME}}"
PG_TABLE="${PG_TABLE-${PG_TABLE_FROM_NAME}}"

echo "→ Comparing Parquet to Postgres (composite-range)"
echo "  schema.table : ${PG_SCHEMA}.${PG_TABLE}"
echo "  file         : ${FILE}"

duckdb -csv -header <<SQL
INSTALL postgres_scanner;
LOAD postgres_scanner;

-- helpers (DuckDB 1.4.x needs VARCHAR -> from_base64)
CREATE OR REPLACE MACRO b64url_to_blob(s) AS
  from_base64(
    replace(replace(CAST(s AS VARCHAR), '-', '+'), '_', '/')
    || repeat('=', (4 - (length(CAST(s AS VARCHAR)) % 4)) % 4)
  );
CREATE OR REPLACE MACRO blob_to_b64url(b) AS
  replace(replace(regexp_replace(to_base64(b), '=+$', ''), '+','-'), '/','_');

-- Parquet contents (keys only for set ops)
CREATE OR REPLACE TEMP VIEW f AS
SELECT uploaded_date, data_item_id
FROM read_parquet('${FILE}');

-- Exact composite bounds from the Parquet file
CREATE OR REPLACE TEMP VIEW first AS
SELECT uploaded_date AS sdt, data_item_id AS sid
FROM f ORDER BY uploaded_date, data_item_id
LIMIT 1;

CREATE OR REPLACE TEMP VIEW last AS
SELECT uploaded_date AS edt, data_item_id AS eid
FROM f ORDER BY uploaded_date DESC, data_item_id DESC
LIMIT 1;

-- Slice Postgres using the SAME composite range
CREATE OR REPLACE TEMP VIEW p AS
SELECT
  uploaded_date,
  b64url_to_blob(data_item_id) AS data_item_id
FROM postgres_scan(
       'host=${PGHOST} port=${PGPORT} dbname=${PGDATABASE} user=${PGUSER} ${PGPASSWORD:+password=${PGPASSWORD}}',
       '${PG_SCHEMA}', '${PG_TABLE}'
     ),
     first, last
WHERE ( uploaded_date > first.sdt
        OR (uploaded_date = first.sdt AND b64url_to_blob(data_item_id) >= first.sid) )
  AND ( uploaded_date < last.edt
        OR (uploaded_date = last.edt  AND b64url_to_blob(data_item_id) <= last.eid) );

-- Mirror f for clarity (already keys only)
CREATE OR REPLACE TEMP VIEW pq AS
SELECT uploaded_date, data_item_id FROM f;

-- Counts
SELECT 'pg_count' AS src, COUNT(*) AS n FROM p
UNION ALL
SELECT 'pq_count', COUNT(*) FROM pq;

-- Symmetric difference (should be zero on both)
SELECT 'pg_minus_pq' AS diff, COUNT(*) AS n
FROM (
  SELECT uploaded_date, data_item_id FROM p
  EXCEPT
  SELECT uploaded_date, data_item_id FROM pq
)
UNION ALL
SELECT 'pq_minus_pg' AS diff, COUNT(*) AS n
FROM (
  SELECT uploaded_date, data_item_id FROM pq
  EXCEPT
  SELECT uploaded_date, data_item_id FROM p
);

-- Uncomment to see a few mismatches for debugging:
-- SELECT 'PG_NOT_IN_PQ' AS kind,
--        uploaded_date, blob_to_b64url(data_item_id) AS id
-- FROM (
--   SELECT uploaded_date, data_item_id FROM p
--   EXCEPT
--   SELECT uploaded_date, data_item_id FROM pq
-- )
-- ORDER BY uploaded_date, data_item_id
-- LIMIT 10;
--
-- SELECT 'PQ_NOT_IN_PG' AS kind,
--        uploaded_date, blob_to_b64url(data_item_id) AS id
-- FROM (
--   SELECT uploaded_date, data_item_id FROM pq
--   EXCEPT
--   SELECT uploaded_date, data_item_id FROM p
-- )
-- ORDER BY uploaded_date, data_item_id
-- LIMIT 10;
SQL
