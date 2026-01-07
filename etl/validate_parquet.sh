#!/usr/bin/env bash
set -euo pipefail

# validate_parquet.sh
# Validates a single Parquet file by comparing filename metadata checksums
# against actual file contents (start/end timestamps, IDs, and row count)
#
# Usage:
#   ./validate_parquet.sh <file_path_or_s3_url>
#
# Examples:
#   ./validate_parquet.sh ./parquet_out/file.parquet
#   ./validate_parquet.sh s3://bucket/prefix/file.parquet

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
TEMP_DIR="${TEMP_DIR:-/tmp/parquet_validation}"
DUCKDB_BIN="${DUCKDB_BIN:-duckdb}"
FORCE_DOWNLOAD=false

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS] <file_path_or_s3_url>

Validates a Parquet file by comparing filename checksums against actual contents.

Arguments:
  file_path_or_s3_url   Local file path or S3 URL (s3://bucket/key)

Options:
  -f, --force           Force re-download from S3 even if file exists locally
  -h, --help            Show this help message

Environment Variables:
  TEMP_DIR              Directory for temporary files (default: /tmp/parquet_validation)
  DUCKDB_BIN            Path to DuckDB binary (default: duckdb)

Examples:
  $0 ./parquet_out/perm_items_public.permanent_data_items_01_2023_01_20230101T000000Z_AbCdEfGh-20230115T235959Z_XyZaBcDe_rows-20000.parquet
  $0 s3://my-bucket/etl/perm_items/year=2023/month=01/half=01/file.parquet
  $0 --force s3://my-bucket/etl/perm_items/year=2023/month=01/half=01/file.parquet

Exit Codes:
  0 - Validation passed
  1 - Validation failed or error occurred
EOF
}

cleanup() {
    local exit_code=$?
    if [ -n "${TEMP_FILE:-}" ] && [ -f "$TEMP_FILE" ]; then
        if [ $exit_code -eq 0 ]; then
            log_info "Cleaning up temporary file: $TEMP_FILE"
            rm -f "$TEMP_FILE"
        else
            log_warning "Validation failed. Temporary file preserved: $TEMP_FILE"
        fi
    fi
}

trap cleanup EXIT

parse_filename_metadata() {
    local filename="$1"
    local basename
    basename=$(basename "$filename")
    
    # Extract metadata using regex
    # Pattern: {prefix}_{schema}.{table}_{start_ts}_{start_id8}-{end_ts}_{end_id8}_rows-{count}.parquet
    if [[ $basename =~ _([0-9]{8}T[0-9]{6}Z)_([A-Za-z0-9_-]{8})-([0-9]{8}T[0-9]{6}Z)_([A-Za-z0-9_-]{8})_rows-([0-9]+)\.parquet$ ]]; then
        FILENAME_START_TS="${BASH_REMATCH[1]}"
        FILENAME_START_ID8="${BASH_REMATCH[2]}"
        FILENAME_END_TS="${BASH_REMATCH[3]}"
        FILENAME_END_ID8="${BASH_REMATCH[4]}"
        FILENAME_ROW_COUNT="${BASH_REMATCH[5]}"
        return 0
    else
        log_error "Filename does not match expected pattern"
        log_error "Expected: {prefix}_{schema}.{table}_{start_ts}_{start_id8}-{end_ts}_{end_id8}_rows-{count}.parquet"
        log_error "Got: $basename"
        return 1
    fi
}

validate_parquet_file() {
    local file_path="$1"
    
    log_info "Validating file: $file_path"
    
    # Parse filename metadata
    if ! parse_filename_metadata "$file_path"; then
        return 1
    fi
    
    log_info "Filename metadata:"
    log_info "  Start timestamp: $FILENAME_START_TS"
    log_info "  Start ID (first 8): $FILENAME_START_ID8"
    log_info "  End timestamp: $FILENAME_END_TS"
    log_info "  End ID (first 8): $FILENAME_END_ID8"
    log_info "  Row count: $FILENAME_ROW_COUNT"
    echo ""
    
    # Create DuckDB query to validate
    log_info "Reading Parquet file with DuckDB..."
    
    # Use a temporary database file
    local temp_db="${TEMP_DIR}/validate_$$.duckdb"
    mkdir -p "$TEMP_DIR"
    
    # Run validation query
    local validation_result
    validation_result=$("$DUCKDB_BIN" "$temp_db" <<EOF
.mode line

-- Query actual data from parquet
-- Use same ORDER BY logic as ETL: uploaded_date, data_item_id (with tiebreaker)
-- Convert base64 to base64url by replacing + with -, / with _, and removing padding =
WITH first_row AS (
    SELECT uploaded_date, data_item_id
    FROM read_parquet('$file_path')
    ORDER BY uploaded_date ASC, data_item_id ASC
    LIMIT 1
),
last_row AS (
    SELECT uploaded_date, data_item_id
    FROM read_parquet('$file_path')
    ORDER BY uploaded_date DESC, data_item_id DESC
    LIMIT 1
)
SELECT 
    (SELECT COUNT(*) FROM read_parquet('$file_path')) as actual_row_count,
    strftime((SELECT uploaded_date FROM first_row), '%Y%m%dT%H%M%SZ') as actual_start_ts,
    strftime((SELECT uploaded_date FROM last_row), '%Y%m%dT%H%M%SZ') as actual_end_ts,
    substring(replace(replace(rtrim(base64((SELECT data_item_id FROM first_row)), '='), '+', '-'), '/', '_'), 1, 8) as actual_start_id8,
    substring(replace(replace(rtrim(base64((SELECT data_item_id FROM last_row)), '='), '+', '-'), '/', '_'), 1, 8) as actual_end_id8;
EOF
)
    
    # Clean up temp database
    rm -f "$temp_db" "${temp_db}.wal"
    
    # Parse DuckDB output
    local actual_row_count actual_start_ts actual_end_ts actual_start_id8 actual_end_id8
    actual_row_count=$(echo "$validation_result" | grep "actual_row_count" | awk '{print $NF}')
    actual_start_ts=$(echo "$validation_result" | grep "actual_start_ts" | awk '{print $NF}')
    actual_end_ts=$(echo "$validation_result" | grep "actual_end_ts" | awk '{print $NF}')
    actual_start_id8=$(echo "$validation_result" | grep "actual_start_id8" | awk '{print $NF}')
    actual_end_id8=$(echo "$validation_result" | grep "actual_end_id8" | awk '{print $NF}')
    
    log_info "Actual file contents:"
    log_info "  Start timestamp: $actual_start_ts"
    log_info "  Start ID (first 8): $actual_start_id8"
    log_info "  End timestamp: $actual_end_ts"
    log_info "  End ID (first 8): $actual_end_id8"
    log_info "  Row count: $actual_row_count"
    echo ""
    
    # Perform validation checks
    local validation_passed=true
    
    log_info "Validation checks:"
    
    # Check row count
    if [ "$FILENAME_ROW_COUNT" = "$actual_row_count" ]; then
        log_success "✓ Row count matches: $FILENAME_ROW_COUNT"
    else
        log_error "✗ Row count mismatch: filename=$FILENAME_ROW_COUNT, actual=$actual_row_count"
        validation_passed=false
    fi
    
    # Check start timestamp
    if [ "$FILENAME_START_TS" = "$actual_start_ts" ]; then
        log_success "✓ Start timestamp matches: $FILENAME_START_TS"
    else
        log_error "✗ Start timestamp mismatch: filename=$FILENAME_START_TS, actual=$actual_start_ts"
        validation_passed=false
    fi
    
    # Check end timestamp
    if [ "$FILENAME_END_TS" = "$actual_end_ts" ]; then
        log_success "✓ End timestamp matches: $FILENAME_END_TS"
    else
        log_error "✗ End timestamp mismatch: filename=$FILENAME_END_TS, actual=$actual_end_ts"
        validation_passed=false
    fi
    
    # Check start ID
    if [ "$FILENAME_START_ID8" = "$actual_start_id8" ]; then
        log_success "✓ Start ID matches: $FILENAME_START_ID8"
    else
        log_error "✗ Start ID mismatch: filename=$FILENAME_START_ID8, actual=$actual_start_id8"
        validation_passed=false
    fi
    
    # Check end ID
    if [ "$FILENAME_END_ID8" = "$actual_end_id8" ]; then
        log_success "✓ End ID matches: $FILENAME_END_ID8"
    else
        log_error "✗ End ID mismatch: filename=$FILENAME_END_ID8, actual=$actual_end_id8"
        validation_passed=false
    fi
    
    echo ""
    
    if [ "$validation_passed" = true ]; then
        log_success "======================================"
        log_success "ALL VALIDATION CHECKS PASSED"
        log_success "======================================"
        return 0
    else
        log_error "======================================"
        log_error "VALIDATION FAILED"
        log_error "======================================"
        return 1
    fi
}

# Main script
main() {
    # Parse options
    local input=""
    
    while [ $# -gt 0 ]; do
        case "$1" in
            -f|--force)
                FORCE_DOWNLOAD=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            -*)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                if [ -z "$input" ]; then
                    input="$1"
                else
                    log_error "Multiple file arguments provided"
                    usage
                    exit 1
                fi
                shift
                ;;
        esac
    done
    
    # Check if input was provided
    if [ -z "$input" ]; then
        log_error "No file path or S3 URL provided"
        usage
        exit 1
    fi
    
    # Check if DuckDB is available
    if ! command -v "$DUCKDB_BIN" &> /dev/null; then
        log_error "DuckDB not found. Please install DuckDB or set DUCKDB_BIN environment variable."
        log_error "Install with: brew install duckdb  (macOS)"
        exit 1
    fi
    
    local file_to_validate=""
    local is_s3=false
    
    # Determine if input is S3 URL or local file
    if [[ $input == s3://* ]]; then
        is_s3=true
        log_info "Detected S3 URL: $input"
        
        # Check if AWS CLI is available
        if ! command -v aws &> /dev/null; then
            log_error "AWS CLI not found. Required for S3 downloads."
            log_error "Install with: brew install awscli  (macOS)"
            exit 1
        fi
        
        # Create temp directory
        mkdir -p "$TEMP_DIR"
        
        # Extract filename from S3 URL
        local s3_filename
        s3_filename=$(basename "$input")
        TEMP_FILE="${TEMP_DIR}/${s3_filename}"
        
        # Check if file already exists locally
        if [ -f "$TEMP_FILE" ] && [ "$FORCE_DOWNLOAD" = false ]; then
            log_info "File already exists locally: $TEMP_FILE"
            log_info "Skipping download (use --force to re-download)"
        else
            # Download from S3
            if [ "$FORCE_DOWNLOAD" = true ] && [ -f "$TEMP_FILE" ]; then
                log_info "Force flag set. Re-downloading from S3..."
            else
                log_info "Downloading from S3 to: $TEMP_FILE"
            fi
            
            if ! aws s3 cp "$input" "$TEMP_FILE"; then
                log_error "Failed to download file from S3"
                exit 1
            fi
            log_success "Download complete"
        fi
        
        file_to_validate="$TEMP_FILE"
    else
        # Local file
        if [ ! -f "$input" ]; then
            log_error "File not found: $input"
            exit 1
        fi
        file_to_validate="$input"
    fi
    
    # Validate the file
    if validate_parquet_file "$file_to_validate"; then
        exit 0
    else
        exit 1
    fi
}

# Run main function
main "$@"
