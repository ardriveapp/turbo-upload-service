#!/usr/bin/env bash
set -euo pipefail

# TODO: Add hive partitioning for S3 uploads (year -> month -> half of month)
# Try to get postgres row count for accounting of total rows expected BEFORE truncation/deletion after flushes

# --- Config (override via env or CLI) ---
PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-postgres}"
PGUSER="${PGUSER:-postgres}"
PGPASSWORD="${PGPASSWORD:-}"   # optional; leave empty if you use trust/SSL or .pgpass
PARTITION_TABLE="${1:-public.permanent_data_items_pre_12_2023}"

# ingestion & flush sizes
CHUNK_SIZE="${CHUNK_SIZE:-10000}"          # rows per pull from Postgres
FLUSH_THRESHOLD="${FLUSH_THRESHOLD:-20000}" # rows per Parquet file

DUCKDB_FILE="${DUCKDB_FILE:-export.duckdb}"
DUCKDB_SCHEMA="${DUCKDB_SCHEMA:-turbo}"
# Use a separate normalized table so you can inspect raw vs normalized if needed
DUCKDB_TABLE_RAW="${DUCKDB_TABLE_RAW:-permanent_data_items_raw}"
DUCKDB_TABLE_NORM="${DUCKDB_TABLE_NORM:-permanent_data_items_norm}"

PARQUET_DIR="${PARQUET_DIR:-./parquet_out}"
PARQUET_PREFIX="${PARQUET_PREFIX:-perm_items}"
mkdir -p "${PARQUET_DIR}"

# DDL for recreating the buffer table (used after each flush to reclaim space)
read -r -d '' TABLE_DDL <<EOF || true
DROP TABLE IF EXISTS ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM};
CREATE TABLE ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM} (
  data_item_id           BLOB PRIMARY KEY,
  owner_public_address   BLOB NOT NULL,
  byte_count             BIGINT NOT NULL,
  uploaded_date          TIMESTAMP NOT NULL,
  assessed_winston_price DECIMAL(20,0) NOT NULL,
  plan_id                UUID NOT NULL,
  planned_date           TIMESTAMP NOT NULL,
  bundle_id              BLOB NOT NULL,
  permanent_date         TIMESTAMP NOT NULL,
  block_height           INTEGER NOT NULL,
  data_start             INTEGER,
  signature_type         SMALLINT,
  failed_bundles         BLOB[],
  content_type           VARCHAR,
  premium_feature_type   VARCHAR,
  deadline_height        INTEGER
);
EOF

# --- S3 options (all optional). If BUCKET is empty -> S3 disabled. ---
S3_BUCKET="${S3_BUCKET:-}"               # e.g. my-bucket
S3_PREFIX="${S3_PREFIX:-etl/perm_items}" # s3 key prefix (no leading slash)
S3_REGION="${S3_REGION:-}"               # optional; if empty, rely on AWS defaults
S3_USE_SSL="${S3_USE_SSL:-true}"         # true|false
S3_LAYOUT="${S3_LAYOUT:-hive-date}"      # "hive-date" (default), "by-partition", or "flat"
S3_RESUME="${S3_RESUME:-1}"              # 1=allow resume-from-S3 if enabled
S3_MAX_KEYS="${S3_MAX_KEYS:-2000}"       # list-objects page size

# Build a DuckDB/Postgres connection string. Omit password if not provided.
PG_CONN="host=${PGHOST} port=${PGPORT} dbname=${PGDATABASE} user=${PGUSER}"
[[ -n "${PGPASSWORD}" ]] && PG_CONN="${PG_CONN} password=${PGPASSWORD}"

# Split schema + table for nicer logging
if [[ "${PARTITION_TABLE}" == *.* ]]; then
  PG_SCHEMA="${PARTITION_TABLE%%.*}"
  PG_TABLE="${PARTITION_TABLE#*.}"
else
  PG_SCHEMA="public"
  PG_TABLE="${PARTITION_TABLE}"
fi

APPNAME="duckdb_etl:${PG_SCHEMA}.${PG_TABLE}"
PG_CONN="${PG_CONN} application_name=${APPNAME}"

echo "↪ Source: ${PG_SCHEMA}.${PG_TABLE}"
echo "↪ DuckDB: ${DUCKDB_FILE} (schema ${DUCKDB_SCHEMA}, table ${DUCKDB_TABLE_NORM})"
echo "↪ Outdir: ${PARQUET_DIR} (threshold=${FLUSH_THRESHOLD}, chunk=${CHUNK_SIZE})"
if [[ -n "${S3_BUCKET}" ]]; then
  echo "↪ S3: s3://${S3_BUCKET}/${S3_PREFIX} (resume=${S3_RESUME}, layout=${S3_LAYOUT})"
else
  echo "↪ S3: disabled (set S3_BUCKET to enable upload & S3 resume)"
fi
echo "↪ Resume strategy: latest_s3_key (STRICT=${S3_STRICT_LAYOUT:-1}, FALLBACK=${S3_RESUME_FALLBACK:-0})"
[[ "${S3_RESUME_DEBUG:-${S3_DEBUG:-0}}" == "1" ]] && echo "↪ S3 resume debug logging is ON"

# Helper to run duckdb and emit CSV (easy parsing)
duckcsv() {
  duckdb -csv -header "${DUCKDB_FILE}" "$@"
}

# Compose S3 key for a local parquet filename
s3_key_for() {
  local fpath="$1"
  local fname; fname="$(basename "$fpath")"
  if [[ "${S3_LAYOUT}" == "by-partition" ]]; then
    printf "%s/%s.%s/%s" "${S3_PREFIX}" "${PG_SCHEMA}" "${PG_TABLE}" "${fname}"

  elif [[ "${S3_LAYOUT}" == "hive-date" ]]; then
    # Try to parse year/month/half from table name: permanent_data_items_MM_YYYY_HH
    local year="" month="" half=""
    if [[ "${PG_TABLE}" =~ ^permanent_data_items_([0-9]{2})_([0-9]{4})_([0-9]{2})$ ]]; then
      month="${BASH_REMATCH[1]}"
      year="${BASH_REMATCH[2]}"
      half="${BASH_REMATCH[3]}"
    else
      # Fallback: parse from the file name's END timestamp (…-YYYYmmddTHHMMSSZ_ENDID…)
      # Example file suffix: ..._{startTS}_{startId8}-{endTS}_{endId8}_rows-N.parquet
      if [[ "$fname" =~ -([0-9]{8})T([0-9]{6})Z_ ]]; then
        local ymd="${BASH_REMATCH[1]}"
        year="${ymd:0:4}"
        month="${ymd:4:2}"
        local day="${ymd:6:2}"
        if (( 10#$day < 15 )); then half="01"; else half="02"; fi
      fi
    fi

    if [[ -n "$year" && -n "$month" && -n "$half" ]]; then
      printf "%s/year=%s/month=%s/half=%s/%s" "${S3_PREFIX}" "${year}" "${month}" "${half}" "${fname}"
    else
      # If we can't parse, fall back to partition layout so we don't drop files on the floor.
      printf "%s/%s.%s/%s" "${S3_PREFIX}" "${PG_SCHEMA}" "${PG_TABLE}" "${fname}"
    fi

  else
    # flat layout
    printf "%s/%s" "${S3_PREFIX}" "${fname}"
  fi
}

# Parse end cursor (end_ts, end_id) from our standard filename.
# Returns "YYYY-mm-dd HH:MM:SS.ffffff,<base64url-id43>" on stdout, or empty if no match.
parse_end_cursor_from_key() {
  local key="$1"
  # match ..._{startTS}_{startId8}-{endTS}_{endId8}_rows-N.parquet
  if [[ "$key" =~ _([0-9]{8})T([0-9]{6})Z_([A-Za-z0-9_-]{8})_rows-[0-9]+\.parquet$ ]]; then
    local ymd="${BASH_REMATCH[1]}" hms="${BASH_REMATCH[2]}"
    local end_ts="${ymd:0:4}-${ymd:4:2}-${ymd:6:2} ${hms:0:2}:${hms:2:2}:${hms:4:2}.000000"
    local end_id8="${BASH_REMATCH[3]}"
    # we only have 8 chars here; that's OK—we only use the ts as the main boundary
    # but prefer full id when available (we’ll try a second regex that captures full 43-char id)
  fi
  # fuller pattern capturing the full end id (43) if name was emitted with it
  if [[ "$key" =~ -([0-9]{8})T([0-9]{6})Z_([A-Za-z0-9_-]{8})_rows-[0-9]+\.parquet$ ]]; then
    local ymd="${BASH_REMATCH[1]}" hms="${BASH_REMATCH[2]}"
    local end_ts="${ymd:0:4}-${ymd:4:2}-${ymd:6:2} ${hms:0:2}:${hms:2:2}:${hms:4:2}.000000"
    local end_id8="${BASH_REMATCH[3]}"
    echo "${end_ts},${end_id8}"
    return 0
  fi
  return 1
}

# Upload local parquet to S3 (overwrite allowed) and delete local file on success
upload_and_delete_local() {
  local fpath="$1"
  local key; key="$(s3_key_for "$fpath")"
  local uri="s3://${S3_BUCKET}/${key}"
  echo "↪ Uploading to ${uri}"

  # up to 3 attempts
  local attempt=1
  while (( attempt <= 3 )); do
    if aws s3 cp "$fpath" "$uri" --only-show-errors; then
      echo "↪ Uploaded OK; removing local file ${fpath}"
      rm -f "$fpath"
      return 0
    fi
    echo "⚠️  Upload attempt ${attempt} failed; retrying..."
    sleep $(( attempt * 2 ))
    ((attempt++))
  done

  echo "❌ Failed to upload ${fpath} after retries"
  return 1
}

# Find latest parquet key in S3 by filename (end_ts, end_id) for *this* table only.
# Honors S3_LAYOUT: hive-date | by-partition | flat
# Env toggles:
#   S3_RESUME_DEBUG=1      -> verbose logging of listing/filtering/selection
#   S3_STRICT_LAYOUT=1     -> no fallback to the "other" layout
#   S3_RESUME_FALLBACK=1   -> allow fallback to the other layout if primary empty
latest_s3_key() {
  local debug="${S3_RESUME_DEBUG:-${S3_DEBUG:-0}}"
  _log() { [[ "$debug" == "1" ]] && echo "S3-RESUME: $*" >&2 || true; }

  local prefix_root="${S3_PREFIX#/}"   # strip leading slash if any
  local table_token="${PARQUET_PREFIX}_${PG_SCHEMA}.${PG_TABLE}_"

  # --- pager: list keys for a prefix (recursively) ---
  _list_keys() {
    local lpref="$1"
    local token="" tmp_json
    tmp_json="$(mktemp -t s3list.XXXX.json)"
    while :; do
      local args=(--bucket "${S3_BUCKET}" --prefix "${lpref}" --max-keys "${S3_MAX_KEYS}")
      [[ -n "${S3_REGION:-}" ]] && args+=(--region "${S3_REGION}")
      [[ -n "${token}" ]] && args+=(--continuation-token "${token}")

      if ! aws s3api list-objects-v2 "${args[@]}" --output json > "${tmp_json}" 2>/dev/null; then
        _log "list-objects-v2 FAILED for prefix='${lpref}'"
        rm -f "${tmp_json}"
        return 1
      fi
      jq -r '.Contents[]?.Key' < "${tmp_json}"
      token="$(jq -r '.NextContinuationToken // ""' < "${tmp_json}")"
      [[ "$(jq -r '.IsTruncated' < "${tmp_json}")" != "true" ]] && break
    done
    rm -f "${tmp_json}"
  }

  # Given keys on stdin, keep only those with our table_token and extract latest
  _latest_from_keys() {
    local tmp_all tmp_keep tmp_idx
    tmp_all="$(mktemp -t s3keys.all.XXXX.txt)"
    tmp_keep="$(mktemp -t s3keys.keep.XXXX.txt)"
    tmp_idx="$(mktemp -t s3idx.XXXX.txt)"
    : > "$tmp_keep"; : > "$tmp_idx"

    cat > "$tmp_all"

    # Filter by basename starting with table token
    local total kept
    total="$(wc -l < "$tmp_all" | tr -d ' ')"
    while IFS= read -r k; do
      [[ -z "$k" ]] && continue
      local base="${k##*/}"
      if [[ "$base" == ${table_token}* ]]; then
        echo "$k" >> "$tmp_keep"
      fi
    done < "$tmp_all"
    kept="$(wc -l < "$tmp_keep" | tr -d ' ')"
    _log "listed=${total} kept_for_table=${kept} token='${table_token}'"

    # Optional: show a few examples
    if [[ "$debug" == "1" ]]; then
      _log "first 5 listed:"
      head -n 5 "$tmp_all" | sed 's/^/  /' >&2 || true
      _log "first 5 kept:"
      head -n 5 "$tmp_keep" | sed 's/^/  /' >&2 || true
    fi

    # Extract (end_ts, end_id)
    local any=0
    while IFS= read -r k; do
      [[ -z "$k" ]] && continue
      local base="${k##*/}"
      if [[ "$base" =~ -([0-9]{8}T[0-9]{6})Z_([A-Za-z0-9_-]{8})_rows-[0-9]+\.parquet$ ]]; then
        any=1
        # printf '%s %s %s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "$k" >> "$tmp_idx"
        printf '%s\t%s\t%s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "$k" >> "$tmp_idx"
      else
        _log "discard(no-suffix-match): $k"
      fi
    done < "$tmp_keep"

    if [[ "$any" -eq 0 ]]; then
      _log "no files matched the expected suffix pattern for table"
      rm -f "$tmp_all" "$tmp_keep" "$tmp_idx"
      return 1
    fi

    # Pick max by (end_ts, end_id)
    #local latest_line latest_key
    #latest_line="$(LC_ALL=C sort -s -k1,1 -k2,2 "$tmp_idx" | tail -n 1)"
    #latest_key="${latest_line##* }"
    #_log "selected(latest): end_ts='${latest_line%% *}' end_id='${latest_line#* }'; key='${latest_key}'"
    local latest_line latest_key
    latest_line="$(LC_ALL=C sort -t$'\t' -k1,1 -k2,2 "$tmp_idx" | tail -n 1)"
    latest_key="$(cut -f3 <<< "$latest_line")"
    local latest_ts latest_id
    latest_ts="$(cut -f1 <<< "$latest_line")"
    latest_id="$(cut -f2 <<< "$latest_line")"
    _log "selected(latest): end_ts='${latest_ts}' end_id='${latest_id}'; key='${latest_key}'"

    rm -f "$tmp_all" "$tmp_keep" "$tmp_idx"
    echo "$latest_key"
    return 0
  }

  # Choose primary prefix based on S3_LAYOUT
  local primary_prefix=""
  case "${S3_LAYOUT}" in
    hive-date)    primary_prefix="${prefix_root}/year=" ;;
    by-partition) primary_prefix="${prefix_root}/${PG_SCHEMA}.${PG_TABLE}/" ;;
    *)            primary_prefix="${prefix_root}/" ;;  # flat/unknown
  esac
  _log "LAYOUT=${S3_LAYOUT} STRICT=${S3_STRICT_LAYOUT:-0} FALLBACK=${S3_RESUME_FALLBACK:-0}"
  _log "primary_prefix=s3://${S3_BUCKET}/${primary_prefix}"

  local latest_primary=""
  latest_primary="$(_list_keys "${primary_prefix}" | _latest_from_keys || true)"
  if [[ -n "${latest_primary}" ]]; then
    _log "winner(primary)=s3://${S3_BUCKET}/${latest_primary}"
    echo "${latest_primary}"
    return 0
  fi

  # Fallback only if allowed and layout differs
  if [[ "${S3_STRICT_LAYOUT:-0}" != "1" && "${S3_RESUME_FALLBACK:-0}" == "1" ]]; then
    local fb_prefix=""
    case "${S3_LAYOUT}" in
      hive-date)    fb_prefix="${prefix_root}/${PG_SCHEMA}.${PG_TABLE}/" ;;
      by-partition) fb_prefix="${prefix_root}/year=" ;;
      *)            fb_prefix="" ;;
    esac
    if [[ -n "${fb_prefix}" ]]; then
      _log "fallback_prefix=s3://${S3_BUCKET}/${fb_prefix}"
      local latest_fb=""
      latest_fb="$(_list_keys "${fb_prefix}" | _latest_from_keys || true)"
      if [[ -n "${latest_fb}" ]]; then
        _log "winner(fallback)=s3://${S3_BUCKET}/${latest_fb}"
        echo "${latest_fb}"
        return 0
      fi
    fi
  fi

  _log "no resume key found for ${PG_SCHEMA}.${PG_TABLE}"
  echo ""
  return 0
}

# Upload any leftover local parquet files for THIS partition dir/prefix.
upload_pending_locals() {
  [[ -z "${S3_BUCKET}" ]] && return 0
  shopt -s nullglob
  local f
  for f in "${PARQUET_DIR}/${PARQUET_PREFIX}_${PG_SCHEMA}.${PG_TABLE}_"*.parquet; do
    echo "↪ Found existing local parquet pending upload: ${f}"
    upload_and_delete_local "${f}" || {
      echo "⚠️  Could not upload pending file (leaving on disk): ${f}"
    }
  done
  shopt -u nullglob
}

# Try to upload any parquet files left from earlier failed uploads
upload_pending_locals

# 1) Ensure DB exists & macros/tables present (idempotent/cheap)
duckdb "${DUCKDB_FILE}" <<SQL
INSTALL postgres_scanner;
LOAD postgres_scanner;

ATTACH '${PG_CONN}' AS pg (TYPE postgres);

-- enable reading S3 parquet directly if configured
INSTALL httpfs;
LOAD httpfs;
INSTALL aws; LOAD aws;
CREATE OR REPLACE SECRET s3_auth (
  TYPE s3,
  PROVIDER credential_chain,
  REFRESH auto
);

-- Optional S3 settings; DuckDB will also pick up standard AWS env automatically.
SET s3_use_ssl='${S3_USE_SSL}';
${S3_REGION:+SET s3_region='${S3_REGION}';}

CREATE SCHEMA IF NOT EXISTS ${DUCKDB_SCHEMA};

-- Converters and guards
CREATE OR REPLACE MACRO b64url_to_blob(s) AS
  from_base64(replace(replace(s, '-', '+'), '_', '/') || repeat('=', (4 - (length(s) % 4)) % 4));
CREATE OR REPLACE MACRO blob_to_b64url(b) AS
  replace(replace(regexp_replace(to_base64(b), '=+$', ''), '+','-'), '/','_');

-- helper for an explicitly-typed empty list
CREATE OR REPLACE MACRO empty_blob_list() AS (LIST_VALUE()::BLOB[]);

CREATE OR REPLACE MACRO parse_failed_bundles(s) AS (
  CASE
    WHEN s IS NULL OR trim(CAST(s AS VARCHAR)) = '' THEN empty_blob_list()
    ELSE
      list_transform(
        list_filter(
          str_split(CAST(s AS VARCHAR), ','),
          x -> length(trim(x)) > 0
               AND regexp_full_match(trim(x), '^[A-Za-z0-9_-]{43}$')
        ),
        x -> b64url_to_blob(trim(x))
      )
  END
);

CREATE OR REPLACE MACRO to_int(v) AS (
  CASE WHEN v IS NULL THEN NULL
       WHEN regexp_full_match(trim(CAST(v AS VARCHAR)), '^[+-]?[0-9]+$') THEN CAST(v AS INTEGER)
       ELSE NULL END
);
CREATE OR REPLACE MACRO to_smallint(v) AS (
  CASE WHEN v IS NULL THEN NULL
       WHEN regexp_full_match(trim(CAST(v AS VARCHAR)), '^[+-]?[0-9]+$') THEN CAST(v AS SMALLINT)
       ELSE NULL END
);
CREATE OR REPLACE MACRO to_bigint(v) AS (
  CASE WHEN v IS NULL THEN NULL
       WHEN regexp_full_match(trim(CAST(v AS VARCHAR)), '^[+-]?[0-9]+$') THEN CAST(v AS BIGINT)
       ELSE NULL END
);
CREATE OR REPLACE MACRO to_dec20_0(v) AS (
  CASE WHEN v IS NULL THEN NULL
       WHEN regexp_full_match(trim(CAST(v AS VARCHAR)), '^[+-]?[0-9]+$') THEN CAST(v AS DECIMAL(20,0))
       ELSE NULL END
);

-- Mirror PG schema (0 rows) for reference/debugging
CREATE TABLE IF NOT EXISTS ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_RAW} AS
SELECT * FROM postgres_scan('${PG_CONN}', '${PG_SCHEMA}', '${PG_TABLE}') LIMIT 0;

-- Buffer table (normalized) with a PK to dedupe accidentally repeated pulls
CREATE TABLE IF NOT EXISTS ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM} (
  data_item_id           BLOB PRIMARY KEY,
  owner_public_address   BLOB NOT NULL,
  byte_count             BIGINT NOT NULL,
  uploaded_date          TIMESTAMP NOT NULL,
  assessed_winston_price DECIMAL(20,0) NOT NULL,
  plan_id                UUID NOT NULL,
  planned_date           TIMESTAMP NOT NULL,
  bundle_id              BLOB NOT NULL,
  permanent_date         TIMESTAMP NOT NULL,
  block_height           INTEGER NOT NULL,
  data_start             INTEGER,
  signature_type         SMALLINT,
  failed_bundles         BLOB[],
  content_type           VARCHAR,
  premium_feature_type   VARCHAR,
  deadline_height        INTEGER
);
SQL

# 2) Determine cursor (uploaded_date, data_item_id as base64url text)
CURSOR_SET=0
CURSOR_DT=""
CURSOR_ID=""

# Prefer last row from DuckDB buffer if it has rows
rows_in_db=$(duckcsv "SELECT COUNT(*) AS n FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM};" | tail -n +2)
if [[ "${rows_in_db}" != "0" ]]; then
  IFS=',' read -r CURSOR_DT CURSOR_ID < <(duckcsv "SELECT strftime(uploaded_date, '%Y-%m-%d %H:%M:%S.%f') AS dt, blob_to_b64url(data_item_id) AS id
     FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
     ORDER BY uploaded_date DESC, data_item_id DESC LIMIT 1;" | tail -n +2)
  echo "↪ Parsed cursor dt='${CURSOR_DT}' id='${CURSOR_ID}' (source=DuckDB)"
  if [[ ! "${CURSOR_ID}" =~ ^[A-Za-z0-9_-]{43}$ ]]; then
    echo "❌ Bad CURSOR_ID parsed: '${CURSOR_ID}'" >&2
    exit 1
  fi
  CURSOR_SET=1
  echo "↪ Resume from DuckDB buffer: (${CURSOR_DT}, ${CURSOR_ID})"
else
  # If buffer is empty, try the newest parquet file
  last_file=""
  if compgen -G "${PARQUET_DIR}/${PARQUET_PREFIX}*.parquet" > /dev/null; then
    # newest by mtime
    last_file=$(ls -t "${PARQUET_DIR}/${PARQUET_PREFIX}"*.parquet | head -n 1)
  fi
  if [[ -n "${last_file}" ]]; then
    IFS=',' read -r CURSOR_DT CURSOR_ID < <(duckcsv "SELECT strftime(uploaded_date, '%Y-%m-%d %H:%M:%S.%f'), blob_to_b64url(data_item_id)
     FROM read_parquet('${last_file}')
     ORDER BY uploaded_date DESC, data_item_id DESC LIMIT 1;" | tail -n +2)
    echo "↪ Parsed cursor dt='${CURSOR_DT}' id='${CURSOR_ID}' (source=Local)"
    if [[ ! "${CURSOR_ID}" =~ ^[A-Za-z0-9_-]{43}$ ]]; then
      echo "❌ Bad CURSOR_ID parsed: '${CURSOR_ID}'" >&2
      exit 1
    fi
    if [[ -n "${CURSOR_DT}" && -n "${CURSOR_ID}" ]]; then
      CURSOR_SET=1
      echo "↪ Resume from Parquet ${last_file}: (${CURSOR_DT}, ${CURSOR_ID})"
    fi
  fi

  # If still no cursor and S3 resume is enabled, use strict/verbose latest_s3_key
  if [[ "${CURSOR_SET}" -eq 0 && -n "${S3_BUCKET}" && "${S3_RESUME}" -eq 1 ]]; then
    # Optional knobs to avoid cross-layout surprises
    : "${S3_STRICT_LAYOUT:=1}"
    : "${S3_RESUME_FALLBACK:=0}"

    echo "↪ S3 resume search using latest_s3_key (layout=${S3_LAYOUT}, strict=${S3_STRICT_LAYOUT}, fallback=${S3_RESUME_FALLBACK})"
    key="$(latest_s3_key || true)"
    if [[ -n "${key:-}" && "${key}" != "None" ]]; then
      s3uri="s3://${S3_BUCKET}/${key}"
      echo "↪ S3 resume candidate: ${s3uri}"

      # DEBUG: Check what's actually in the file
      if [[ "${S3_RESUME_DEBUG:-0}" == "1" ]]; then
        echo "DEBUG: Inspecting S3 file structure..." >&2
        {
          duckdb -csv -noheader "${DUCKDB_FILE}" <<SQL
INSTALL httpfs; LOAD httpfs;
INSTALL aws; LOAD aws;
CREATE OR REPLACE SECRET s3_auth (
  TYPE s3,
  PROVIDER credential_chain,
  REFRESH auto
);
${S3_REGION:+SET s3_region='${S3_REGION}';}
SELECT COUNT(*) as row_count FROM read_parquet('${s3uri}');
SQL
        } 2>&1 | tail -1 >&2 || echo "DEBUG: Failed to get row count" >&2
        
        echo "DEBUG: Sample row from S3 file:" >&2
        {
          duckdb -csv "${DUCKDB_FILE}" <<SQL
INSTALL httpfs; LOAD httpfs;
INSTALL aws; LOAD aws;
CREATE OR REPLACE SECRET s3_auth (
  TYPE s3,
  PROVIDER credential_chain,
  REFRESH auto
);
${S3_REGION:+SET s3_region='${S3_REGION}';}
SELECT 
  uploaded_date,
  data_item_id,
  typeof(uploaded_date) as dt_type,
  typeof(data_item_id) as id_type
FROM read_parquet('${s3uri}')
ORDER BY uploaded_date DESC, data_item_id DESC
LIMIT 1;
SQL
        } 2>&1 | tail -5 >&2 || echo "DEBUG: Failed to get sample row" >&2
      fi

      # Peek last row to derive a hard cursor (dt,id) from the actual data
      # Use a temporary output file to capture both stdout and stderr
      cursor_tmp="$(mktemp -t s3cursor.XXXX.csv)"
      cursor_err="$(mktemp -t s3cursor_err.XXXX.txt)"
      
      echo "↪ Attempting to read cursor from S3 file..." >&2
      
      if ! duckdb "${DUCKDB_FILE}" > "$cursor_tmp" 2> "$cursor_err" <<SQL
.mode csv
.header off
INSTALL httpfs; LOAD httpfs;
INSTALL aws; LOAD aws;
CREATE OR REPLACE SECRET s3_auth (
  TYPE s3,
  PROVIDER credential_chain,
  REFRESH auto
);
${S3_REGION:+SET s3_region='${S3_REGION}';}
CREATE OR REPLACE MACRO blob_to_b64url(b) AS
  replace(replace(regexp_replace(to_base64(b), '=+\$', ''), '+','-'), '/','_');
SELECT strftime(uploaded_date, '%Y-%m-%d %H:%M:%S.%f'),
       blob_to_b64url(data_item_id)
FROM read_parquet('${s3uri}')
ORDER BY uploaded_date DESC, data_item_id DESC
LIMIT 1;
SQL
      then
        echo "❌ FATAL: Failed to read cursor from S3 resume file" >&2
        echo "❌ S3 URI: ${s3uri}" >&2
        echo "❌ DuckDB errors:" >&2
        cat "$cursor_err" >&2
        echo "" >&2
        echo "❌ DuckDB output:" >&2
        cat "$cursor_tmp" >&2
        rm -f "$cursor_tmp" "$cursor_err"
        exit 1
      fi
      
      rm -f "$cursor_err"

      # Debug: show what's in the temp file
      if [[ "${S3_RESUME_DEBUG:-0}" == "1" ]]; then
        echo "DEBUG: Raw cursor_tmp content:" >&2
        cat "$cursor_tmp" >&2
        echo "---" >&2
      fi

      # Extract just the CSV data line (skip any DuckDB status messages)
      # Use a simple while loop to read and filter
      CURSOR_DT=""
      CURSOR_ID=""
      while IFS=',' read -r dt_part id_part; do
        # Only accept lines that start with a 4-digit year
        if [[ "$dt_part" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2} ]]; then
          CURSOR_DT="$dt_part"
          CURSOR_ID="${id_part%$'\n'}"  # Strip trailing newline
          CURSOR_ID="${CURSOR_ID%$'\r'}"  # Strip trailing carriage return if present
          break
        fi
      done < "$cursor_tmp"

      echo "↪ Parsed cursor from S3 file: (${CURSOR_DT}, ${CURSOR_ID})"
      
      # Validate cursor before continuing
      if [[ -z "${CURSOR_DT}" || -z "${CURSOR_ID}" ]]; then
        echo "❌ FATAL: Failed to extract cursor from S3 file" >&2
        echo "❌ S3 URI: ${s3uri}" >&2
        echo "❌ Raw DuckDB output:" >&2
        cat "$cursor_tmp" >&2
        rm -f "$cursor_tmp"
        exit 1
      fi
      
      if [[ ! "${CURSOR_ID}" =~ ^[A-Za-z0-9_-]{8,43}$ ]]; then
        echo "❌ FATAL: S3-derived CURSOR_ID looks malformed: '${CURSOR_ID}'" >&2
        echo "❌ Expected: 8-43 character base64url string" >&2
        echo "❌ Got length: ${#CURSOR_ID}" >&2
        echo "❌ S3 URI: ${s3uri}" >&2
        echo "❌ Raw DuckDB output:" >&2
        cat "$cursor_tmp" >&2
        rm -f "$cursor_tmp"
        exit 1
      fi
      
      rm -f "$cursor_tmp"
      
      CURSOR_SET=1
      echo "↪ S3 resume cursor=(${CURSOR_DT}, ${CURSOR_ID})"
    else
      echo "↪ No S3 resume key discovered by latest_s3_key."
    fi
  fi
fi

# 3) Main loop: pull chunks and flush when threshold reached
while :; do
  if [[ "${CURSOR_SET}" -eq 1 && -n "${CURSOR_DT}" && -n "${CURSOR_ID}" ]]; then
    FILTER="WHERE uploaded_date > TIMESTAMP '${CURSOR_DT}'
            OR (uploaded_date = TIMESTAMP '${CURSOR_DT}' AND data_item_id > '${CURSOR_ID}')"
  else
    FILTER=""
  fi

  if [[ "${CURSOR_SET}" -eq 1 && -n "${CURSOR_DT}" && -n "${CURSOR_ID}" ]]; then
    WHERE_SQL="WHERE (uploaded_date, data_item_id) > (TIMESTAMP ''${CURSOR_DT}'', ''${CURSOR_ID}'')"
  else
    WHERE_SQL=""
  fi

  # One DuckDB invocation per chunk: build src → insert → report → drop src
  # Strip header row with tail -n +2 so read gets pure values
  IFS=',' read -r pulled bufcount new_cursor_dt new_cursor_id <<EOF
$(duckcsv "
  -- ensure attached foreign DB alias
  DETACH DATABASE IF EXISTS pg;
  ATTACH '${PG_CONN}' AS pg (TYPE postgres);

  CREATE OR REPLACE TEMP TABLE src_raw AS
  SELECT
    data_item_id,                -- base64url string (raw)
    owner_public_address,
    byte_count,
    uploaded_date,
    assessed_winston_price,
    plan_id,
    planned_date,
    bundle_id,
    permanent_date,
    block_height,
    data_start,
    signature_type,
    failed_bundles,
    content_type,
    premium_feature_type,
    deadline_height
  FROM postgres_query('pg', 'SELECT
      data_item_id,
      owner_public_address,
      byte_count,
      uploaded_date,
      assessed_winston_price,
      plan_id,
      planned_date,
      bundle_id,
      permanent_date,
      block_height,
      data_start,
      signature_type,
      failed_bundles,
      content_type,
      premium_feature_type,
      deadline_height
    FROM ${PG_SCHEMA}.${PG_TABLE}
    ${WHERE_SQL}
    ORDER BY uploaded_date, data_item_id
    LIMIT ${CHUNK_SIZE}');

CREATE OR REPLACE TEMP TABLE src AS
  SELECT
    b64url_to_blob(data_item_id)           AS data_item_id,
    b64url_to_blob(owner_public_address)   AS owner_public_address,
    to_bigint(byte_count)                  AS byte_count,
    uploaded_date                          AS uploaded_date,
    to_dec20_0(assessed_winston_price)     AS assessed_winston_price,
    TRY_CAST(plan_id AS UUID)              AS plan_id,
    planned_date                           AS planned_date,
    b64url_to_blob(bundle_id)              AS bundle_id,
    permanent_date                         AS permanent_date,
    to_int(block_height)                   AS block_height,
    to_int(data_start)                     AS data_start,
    to_smallint(signature_type)            AS signature_type,
    parse_failed_bundles(failed_bundles)   AS failed_bundles,
    NULLIF(content_type, '')               AS content_type,
    NULLIF(premium_feature_type, '')       AS premium_feature_type,
    to_int(deadline_height)                AS deadline_height
  FROM src_raw;

INSERT OR IGNORE INTO ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
SELECT * FROM src;

SELECT
  (SELECT COUNT(*) FROM src_raw)                                        AS pulled,
  (SELECT COUNT(*) FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM})          AS bufcount,
  (SELECT strftime(uploaded_date, '%Y-%m-%d %H:%M:%S.%f')
     FROM src_raw ORDER BY uploaded_date DESC, data_item_id DESC LIMIT 1)  AS cursor_dt,
  (SELECT data_item_id
     FROM src_raw ORDER BY uploaded_date DESC, data_item_id DESC LIMIT 1)  AS cursor_id;

DROP TABLE src;
DROP TABLE src_raw;
" | tail -n +2)
EOF

  # Default/validate the numeric fields so arithmetic doesn’t blow up
  pulled="${pulled:-0}"
  bufcount="${bufcount:-0}"
  [[ "$pulled" =~ ^[0-9]+$ ]]   || pulled=0
  [[ "$bufcount" =~ ^[0-9]+$ ]] || bufcount=0

  if (( pulled == 0 )); then
    echo "↪ No more rows from Postgres. Done pulling."
    break
  fi

  CURSOR_SET=1
  CURSOR_DT="${new_cursor_dt}"
  CURSOR_ID="${new_cursor_id}"
  echo "↪ Pulled ${pulled} rows; buffer now ${bufcount}; cursor=(${CURSOR_DT}, ${CURSOR_ID})"

if (( bufcount >= FLUSH_THRESHOLD )); then
  if [[ "${S3_LAYOUT}" == "hive-date" && "${HIVE_SPLIT:-1}" -eq 1 ]]; then
    # 2A) Split buffer rows by year/month/half and write one Parquet per group
    IFS=$'\n' read -r -d '' -a groups < <(duckcsv "
      SELECT
        strftime(uploaded_date, '%Y') AS yyyy,
        strftime(uploaded_date, '%m') AS mm,
        CASE WHEN CAST(strftime(uploaded_date, '%d') AS INTEGER) < 15 THEN '01' ELSE '02' END AS hh,
        COUNT(*) AS n
      FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
      GROUP BY 1,2,3
      ORDER BY 1,2,3;" | tail -n +2 && printf '\0')

    for g in "${groups[@]}"; do
      [[ -z "$g" ]] && continue
      IFS=',' read -r GY GM GH GN <<< "$g"

      # Build filename info for this group
      IFS=',' read -r g_start_ts g_end_ts g_start_id g_end_id g_rows <<EOF
$(duckcsv "
WITH grp AS (
  SELECT * FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
  WHERE strftime(uploaded_date, '%Y')='${GY}'
    AND strftime(uploaded_date, '%m')='${GM}'
    AND (CASE WHEN CAST(strftime(uploaded_date, '%d') AS INTEGER) < 15 THEN '01' ELSE '02' END)='${GH}'
),
first_row AS (
  SELECT uploaded_date, blob_to_b64url(data_item_id) AS id
  FROM grp ORDER BY uploaded_date, data_item_id LIMIT 1
),
last_row AS (
  SELECT uploaded_date, blob_to_b64url(data_item_id) AS id
  FROM grp ORDER BY uploaded_date DESC, data_item_id DESC LIMIT 1
)
SELECT
  strftime((SELECT uploaded_date FROM first_row) AT TIME ZONE 'UTC','%Y%m%dT%H%M%S')||'Z',
  strftime((SELECT uploaded_date FROM last_row)  AT TIME ZONE 'UTC','%Y%m%dT%H%M%S')||'Z',
  substr((SELECT id FROM first_row),1,8),
  substr((SELECT id FROM last_row),1,8),
  (SELECT COUNT(*) FROM grp);
" | tail -n +2)
EOF

      outfile="${PARQUET_DIR}/${PARQUET_PREFIX}_${PG_SCHEMA}.${PG_TABLE}_${g_start_ts}_${g_start_id}-${g_end_ts}_${g_end_id}_rows-${g_rows}.parquet"
      echo "↪ Flushing group year=${GY} month=${GM} half=${GH} rows=${g_rows} → ${outfile}"

      duckdb "${DUCKDB_FILE}" <<SQL
COPY (
  SELECT * FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
  WHERE strftime(uploaded_date, '%Y')='${GY}'
    AND strftime(uploaded_date, '%m')='${GM}'
    AND (CASE WHEN CAST(strftime(uploaded_date, '%d') AS INTEGER) < 15 THEN '01' ELSE '02' END)='${GH}'
) TO '${outfile}' (FORMAT PARQUET, COMPRESSION ZSTD);
SQL

      if [[ -n "${S3_BUCKET}" ]]; then
        upload_and_delete_local "${outfile}" || echo "⚠️  Leaving local file: ${outfile}"
      fi
    done

    # Clear buffer once all groups are flushed
    # Drop and recreate table to truly reclaim space
    duckdb "${DUCKDB_FILE}" <<SQL
${TABLE_DDL}
FORCE CHECKPOINT;
SQL

  else
    # 2B) Original single-file flush for non-hive layouts
    IFS=',' read -r start_ts end_ts start_id end_id row_count <<EOF
$(duckcsv "
  WITH ranked AS (
    SELECT
      uploaded_date,
      blob_to_b64url(data_item_id) AS id_b64,
      ROW_NUMBER() OVER (ORDER BY uploaded_date, data_item_id) AS rn_first,
      ROW_NUMBER() OVER (ORDER BY uploaded_date DESC, data_item_id DESC) AS rn_last
    FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
  )
  SELECT
    strftime((SELECT uploaded_date FROM ranked WHERE rn_first = 1) AT TIME ZONE 'UTC','%Y%m%dT%H%M%S')||'Z',
    strftime((SELECT uploaded_date FROM ranked WHERE rn_last  = 1) AT TIME ZONE 'UTC','%Y%m%dT%H%M%S')||'Z',
    substr((SELECT id_b64 FROM ranked WHERE rn_first = 1),1,8),
    substr((SELECT id_b64 FROM ranked WHERE rn_last  = 1),1,8),
    (SELECT COUNT(*) FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM});
" | tail -n +2)
EOF

    outfile="${PARQUET_DIR}/${PARQUET_PREFIX}_${PG_SCHEMA}.${PG_TABLE}_${start_ts}_${start_id}-${end_ts}_${end_id}_rows-${row_count}.parquet"
    echo "↪ Flushing ${row_count} rows to ${outfile}"

    duckdb "${DUCKDB_FILE}" <<SQL
COPY (SELECT * FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM})
TO '${outfile}' (FORMAT PARQUET, COMPRESSION ZSTD);
${TABLE_DDL}
FORCE CHECKPOINT;
SQL

    if [[ -n "${S3_BUCKET}" ]]; then
      upload_and_delete_local "${outfile}" || echo "⚠️  Leaving local file due to upload failure: ${outfile}"
    fi
  fi
fi
done

# Final drain flush (if any rows remain below threshold)
remaining=$(duckcsv "SELECT COUNT(*) FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM};" | tail -n +2)
[[ "$remaining" =~ ^[0-9]+$ ]] || remaining=0
if (( remaining > 0 )); then
  if [[ "${S3_LAYOUT}" == "hive-date" && "${HIVE_SPLIT:-1}" -eq 1 ]]; then
    # Final drain with Hive split: one file per (year, month, half)
    IFS=$'\n' read -r -d '' -a groups < <(duckcsv "
      SELECT
        strftime(uploaded_date, '%Y') AS yyyy,
        strftime(uploaded_date, '%m') AS mm,
        CASE WHEN CAST(strftime(uploaded_date, '%d') AS INTEGER) < 15 THEN '01' ELSE '02' END AS hh,
        COUNT(*) AS n
      FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
      GROUP BY 1,2,3
      ORDER BY 1,2,3;" | tail -n +2 && printf '\0')

    for g in "${groups[@]}"; do
      [[ -z "$g" ]] && continue
      IFS=',' read -r GY GM GH GN <<< "$g"

      # Build deterministic filename parts for this group
      IFS=',' read -r g_start_ts g_end_ts g_start_id g_end_id g_rows <<EOF
$(duckcsv "
WITH grp AS (
  SELECT * FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
  WHERE strftime(uploaded_date, '%Y')='${GY}'
    AND strftime(uploaded_date, '%m')='${GM}'
    AND (CASE WHEN CAST(strftime(uploaded_date, '%d') AS INTEGER) < 15 THEN '01' ELSE '02' END)='${GH}'
),
first_row AS (
  SELECT uploaded_date, blob_to_b64url(data_item_id) AS id
  FROM grp ORDER BY uploaded_date, data_item_id LIMIT 1
),
last_row AS (
  SELECT uploaded_date, blob_to_b64url(data_item_id) AS id
  FROM grp ORDER BY uploaded_date DESC, data_item_id DESC LIMIT 1
)
SELECT
  strftime((SELECT uploaded_date FROM first_row) AT TIME ZONE 'UTC','%Y%m%dT%H%M%S')||'Z',
  strftime((SELECT uploaded_date FROM last_row)  AT TIME ZONE 'UTC','%Y%m%dT%H%M%S')||'Z',
  substr((SELECT id FROM first_row),1,8),
  substr((SELECT id FROM last_row),1,8),
  (SELECT COUNT(*) FROM grp);
" | tail -n +2)
EOF

      outfile="${PARQUET_DIR}/${PARQUET_PREFIX}_${PG_SCHEMA}.${PG_TABLE}_${g_start_ts}_${g_start_id}-${g_end_ts}_${g_end_id}_rows-${g_rows}.parquet"
      echo "↪ Final flush group year=${GY} month=${GM} half=${GH} rows=${g_rows} → ${outfile}"

      duckdb "${DUCKDB_FILE}" <<SQL
COPY (
  SELECT * FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
  WHERE strftime(uploaded_date, '%Y')='${GY}'
    AND strftime(uploaded_date, '%m')='${GM}'
    AND (CASE WHEN CAST(strftime(uploaded_date, '%d') AS INTEGER) < 15 THEN '01' ELSE '02' END)='${GH}'
) TO '${outfile}' (FORMAT PARQUET, COMPRESSION ZSTD);
SQL

      if [[ -n "${S3_BUCKET}" ]]; then
        upload_and_delete_local "${outfile}" || {
          echo "⚠️  Leaving local file due to upload failure: ${outfile}"
        }
      fi
    done

    # Clear any remaining rows now that all groups are flushed
    # Drop and recreate table to truly reclaim space
    duckdb "${DUCKDB_FILE}" <<SQL
${TABLE_DDL}
FORCE CHECKPOINT;
SQL

  else
    # Original single-file final drain (non-hive or no split)
    IFS=',' read -r start_ts end_ts start_id end_id row_count <<EOF
$(duckcsv "
  WITH bounds AS (
    SELECT MIN(uploaded_date) AS min_dt, MAX(uploaded_date) AS max_dt
    FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
  ),
  first_row AS (
    SELECT uploaded_date, blob_to_b64url(data_item_id) AS id
    FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
    ORDER BY uploaded_date, data_item_id
    LIMIT 1
  ),
  last_row AS (
    SELECT uploaded_date, blob_to_b64url(data_item_id) AS id
    FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM}
    ORDER BY uploaded_date DESC, data_item_id DESC
    LIMIT 1
  )
  SELECT
    strftime(first_row.uploaded_date AT TIME ZONE 'UTC', '%Y%m%dT%H%M%S') || 'Z',
    strftime(last_row.uploaded_date  AT TIME ZONE 'UTC', '%Y%m%dT%H%M%S') || 'Z',
    substr(first_row.id, 1, 8),
    substr(last_row.id,  1, 8),
    (SELECT COUNT(*) FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM});
" | tail -n +2)
EOF

    outfile="${PARQUET_DIR}/${PARQUET_PREFIX}_${PG_SCHEMA}.${PG_TABLE}_${start_ts}_${start_id}-${end_ts}_${end_id}_rows-${row_count}.parquet"
    echo "↪ Final flush ${row_count} rows to ${outfile}"
    duckdb "${DUCKDB_FILE}" <<SQL
COPY (SELECT * FROM ${DUCKDB_SCHEMA}.${DUCKDB_TABLE_NORM})
TO '${outfile}' (FORMAT PARQUET, COMPRESSION ZSTD);
${TABLE_DDL}
FORCE CHECKPOINT;
SQL

    if [[ -n "${S3_BUCKET}" ]]; then
      upload_and_delete_local "${outfile}" || {
        echo "⚠️  Leaving local file due to upload failure: ${outfile}"
      }
    fi
  fi
fi

echo "✅ Completed partition: ${PG_SCHEMA}.${PG_TABLE}"

# Note: CHECKPOINT and VACUUM already run after each flush, so no extra cleanup needed here
# The controller script will remove the entire state directory if CLEANUP_ON_SUCCESS=1
