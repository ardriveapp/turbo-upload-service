#!/usr/bin/env bash
set -euo pipefail

# -------- Config (override via env/CLI) --------
# Partitions source (choose one):
#   1) CLI args: ./etl_controller.sh schema.t1 schema.t2 ...
#   2) PARTITIONS env: comma or newline separated
#   3) PARTITIONS_FILE env: path to a file with one partition per line
RETRIES="${RETRIES:-2}"               # total attempts per partition (1 + retries-1)
RETRY_BACKOFF_SEC="${RETRY_BACKOFF_SEC:-5}"

ETL="${ETL:-./pg_to_parquet_etl.sh}"                # path to your ETL script
STATE_DIR="${STATE_DIR:-./etl_state}"     # per-partition duckdb files
OUT_ROOT="${OUT_ROOT:-./parquet_out}" # parent for per-partition parquet dirs

mkdir -p "${STATE_DIR}" "${OUT_ROOT}"

now_ts() { date -u +"%Y-%m-%dT%H:%M:%SZ"; }
part_slug() { local p="$1"; echo "${p//./_}"; }  # schema.table -> schema_table

# -------- Collect partitions --------
declare -a PARTS=()
if (( "$#" > 0 )); then
  PARTS=("$@")
elif [[ -n "${PARTITIONS:-}" ]]; then
  IFS=$'\n,' read -r -d '' -a PARTS < <(printf '%s\0' "${PARTITIONS}")
elif [[ -n "${PARTITIONS_FILE:-}" ]]; then
  PARTS=()
  # Read file safely, even if the last line has no newline
  while IFS= read -r line || [[ -n "$line" ]]; do
    # strip trailing CR (for files with Windows line endings)
    line=${line%$'\r'}

    # skip blanks and comments
    [[ "$line" =~ ^[[:space:]]*$ || "$line" =~ ^[[:space:]]*# ]] && continue

    # allow comma-separated entries on a line
    IFS=',' read -r -a items <<< "$line"
    for it in "${items[@]}"; do
      # trim leading/trailing whitespace
      it="$(echo -n "$it" | xargs)"
      [[ -n "$it" ]] && PARTS+=("$it")
    done
  done < "$PARTITIONS_FILE"
else
  echo "No partitions provided. Supply as CLI args, PARTITIONS env, or PARTITIONS_FILE." >&2
  exit 2
fi

echo "Running ${#PARTS[@]} partition(s) serially. Retries=${RETRIES}"

declare -a OKS=()
declare -a FAILS=()

for part in "${PARTS[@]}"; do
  part="$(echo -n "${part}" | xargs)"   # trim
  [[ -z "${part}" ]] && continue

  slug="$(part_slug "${part}")"
  p_state="${STATE_DIR}/${part}"
  p_out="${OUT_ROOT}/${part}"
  duck="${p_state}/export.duckdb"

  mkdir -p "${p_state}" "${p_out}"

  echo
  echo "===== $(now_ts) BEGIN ${part} ====="
  attempt=1
  while (( attempt <= RETRIES )); do
    echo "[$(now_ts)] ${part} attempt ${attempt}/${RETRIES}"

    # Per-partition isolation via env overrides
    DUCKDB_FILE="${duck}" \
    PARQUET_DIR="${p_out}" \
    "${ETL}" "${part}" && {
      echo "[$(now_ts)] SUCCESS ${part}"
      OKS+=("${part}")
      break
    }

    rc=$?
    echo "[$(now_ts)] FAIL ${part} (rc=${rc})"
    if (( attempt == RETRIES )); then
      echo "[$(now_ts)] GAVE UP ${part}"
      FAILS+=("${part}")
      break
    fi
    sleep "${RETRY_BACKOFF_SEC}"
    ((attempt++))
  done
  echo "===== $(now_ts) END ${part} ====="
done

echo
echo "===== SUMMARY ====="
echo "Succeeded: ${#OKS[@]}"
printf '  - %s\n' "${OKS[@]:-}" | sed '/^- $/d' || true
echo "Failed:    ${#FAILS[@]}"
printf '  - %s\n' "${FAILS[@]:-}" | sed '/^- $/d' || true

# Non-zero exit if any failures
(( ${#FAILS[@]} > 0 )) && exit 1 || exit 0
