#!/usr/bin/env bash
set -euo pipefail

# validate_all_parquet.sh
# Controller script to validate multiple Parquet files using validate_parquet.sh
#
# Usage:
#   ./validate_all_parquet.sh <file1> <file2> <file3> ...
#   ./validate_all_parquet.sh s3://bucket/prefix/
#   FILES_LIST=./files.txt ./validate_all_parquet.sh
#
# Examples:
#   ./validate_all_parquet.sh file1.parquet file2.parquet
#   ./validate_all_parquet.sh s3://my-bucket/etl/perm_items/
#   ./validate_all_parquet.sh s3://my-bucket/etl/perm_items/year=2023/
#   FILES_LIST=my_files.txt ./validate_all_parquet.sh

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Configuration
VALIDATOR="${VALIDATOR:-./validate_parquet.sh}"
FILES_LIST="${FILES_LIST:-}"
CONTINUE_ON_ERROR="${CONTINUE_ON_ERROR:-1}"
FORCE_DOWNLOAD="${FORCE_DOWNLOAD:-0}"
MAX_PARALLEL="${MAX_PARALLEL:-1}"
S3_REGION="${S3_REGION:-}"

# Counters
TOTAL_FILES=0
PASSED_FILES=0
FAILED_FILES=0
declare -a FAILED_LIST=()

# Trap handler for cleanup
cleanup_on_interrupt() {
    echo "" >&2
    log_info "Script interrupted. Printing summary of completed validations..."
    print_summary
    exit 130  # Standard exit code for SIGINT
}

# Set up trap for Ctrl+C and other signals (but not EXIT - main() handles normal exit)
trap cleanup_on_interrupt INT TERM

# Functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*" >&2
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*" >&2
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*" >&2
}

log_header() {
    echo -e "${CYAN}${BOLD}$*${NC}" >&2
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS] <file_or_prefix...>

Controller script to validate multiple Parquet files against their filename checksums.

Arguments:
  file_or_prefix...     One or more local files, S3 URLs, or S3 prefixes to validate
                        - Local files: ./path/to/file.parquet
                        - S3 files: s3://bucket/path/to/file.parquet
                        - S3 prefix: s3://bucket/prefix/ (recursively finds all .parquet files)

Options:
  -f, --force           Force re-download from S3 even if files exist locally
  -p, --parallel N      Run N validations in parallel (default: 1, sequential)
  -c, --continue        Continue validation even if files fail (default: true)
  --stop-on-error       Stop on first validation failure
  -h, --help            Show this help message

Environment Variables:
  FILES_LIST            Path to file containing list of files/prefixes (one per line)
  VALIDATOR             Path to validate_parquet.sh script (default: ./validate_parquet.sh)
  CONTINUE_ON_ERROR     Continue on errors: 1=yes, 0=no (default: 1)
  FORCE_DOWNLOAD        Force S3 re-download: 1=yes, 0=no (default: 0)
  MAX_PARALLEL          Maximum parallel validations (default: 1)
  S3_REGION             AWS region for S3 operations (optional)
  S3_LIST_DEBUG         Enable verbose S3 listing debug output: 1=yes, 0=no (default: 0)

Examples:
  # Validate specific files
  $0 file1.parquet file2.parquet s3://bucket/file3.parquet

  # Validate all files under an S3 prefix
  $0 s3://my-bucket/etl/perm_items/

  # Validate with parallel execution
  $0 --parallel 4 s3://my-bucket/etl/perm_items/year=2023/

  # Use a file list
  echo "file1.parquet" > files.txt
  echo "s3://bucket/file2.parquet" >> files.txt
  FILES_LIST=files.txt $0

  # Force re-download and stop on first error
  $0 --force --stop-on-error s3://bucket/prefix/

Exit Codes:
  0 - All validations passed
  1 - One or more validations failed
  2 - Invalid arguments or setup error
EOF
}

# Check if S3 prefix or file
is_s3_url() {
    [[ "$1" == s3://* ]]
}

# Check if argument looks like a prefix (ends with / or doesn't end with .parquet)
is_s3_prefix() {
    local url="$1"
    # If it ends with /, it's definitely a prefix
    [[ "$url" == */ ]] && return 0
    # If it ends with .parquet, it's a file
    [[ "$url" == *.parquet ]] && return 1
    # Otherwise, treat as a prefix (e.g., s3://bucket/prefix or s3://bucket/year=2023)
    return 0
}

# List all .parquet files under an S3 prefix
list_s3_parquet_files() {
    local s3_prefix="$1"
    local bucket key_prefix
    local debug="${S3_LIST_DEBUG:-0}"
    
    [[ "$debug" == "1" ]] && log_info "DEBUG: list_s3_parquet_files called with: $s3_prefix"
    
    # Parse s3://bucket/prefix
    bucket="${s3_prefix#s3://}"
    bucket="${bucket%%/*}"
    key_prefix="${s3_prefix#s3://${bucket}/}"
    
    [[ "$debug" == "1" ]] && log_info "DEBUG: After parsing - bucket='$bucket' key_prefix='$key_prefix'"
    
    # Ensure key_prefix ends with / if not empty, for proper prefix matching
    if [[ -n "$key_prefix" && "$key_prefix" != */ ]]; then
        key_prefix="${key_prefix}/"
    fi
    
    [[ "$debug" == "1" ]] && log_info "DEBUG: After normalization - key_prefix='$key_prefix'"
    
    log_info "Listing .parquet files under s3://${bucket}/${key_prefix}"
    
    local args=(--bucket "$bucket" --prefix "$key_prefix")
    [[ -n "${S3_REGION}" ]] && args+=(--region "${S3_REGION}")
    
    [[ "$debug" == "1" ]] && log_info "DEBUG: bucket='$bucket' key_prefix='$key_prefix'"
    [[ "$debug" == "1" ]] && log_info "DEBUG: Starting pagination loop..."
    
    local temp_json temp_keys
    temp_json="$(mktemp -t s3list.XXXX.json)"
    temp_keys="$(mktemp -t s3keys.XXXX.txt)"
    
    [[ "$debug" == "1" ]] && log_info "DEBUG: Created temp files: json=$temp_json keys=$temp_keys"
    
    local token=""
    local page=0
    
    [[ "$debug" == "1" ]] && log_info "DEBUG: About to enter while loop..."
    
    while :; do
        ((page++)) || true
        [[ "$debug" == "1" ]] && log_info "DEBUG: Entered loop, page=$page"
        local page_args=("${args[@]}")
        [[ -n "$token" ]] && page_args+=(--continuation-token "$token")
        
        [[ "$debug" == "1" ]] && log_info "Page $page: Running aws s3api list-objects-v2 --bucket $bucket --prefix $key_prefix"
        
        local aws_error
        aws_error="$(mktemp -t s3err.XXXX.txt)"
        if ! aws s3api list-objects-v2 "${page_args[@]}" --output json > "$temp_json" 2> "$aws_error"; then
            log_error "Failed to list objects in s3://${bucket}/${key_prefix}"
            if [[ -s "$aws_error" ]]; then
                log_error "AWS CLI error output:"
                cat "$aws_error" >&2
            fi
            rm -f "$temp_json" "$temp_keys" "$aws_error"
            return 1
        fi
        rm -f "$aws_error"
        
        # Extract .parquet files and append to temp file
        local page_parquet_count
        page_parquet_count=$(jq -r '.Contents[]?.Key | select(endswith(".parquet"))' < "$temp_json" | tee -a "$temp_keys" | wc -l | tr -d ' ')
        [[ "$debug" == "1" ]] && log_info "Page $page: found $page_parquet_count .parquet files"
        
        local is_truncated
        is_truncated="$(jq -r '.IsTruncated // false' < "$temp_json")"
        token="$(jq -r '.NextContinuationToken // ""' < "$temp_json")"
        
        [[ "$debug" == "1" ]] && log_info "Page $page: IsTruncated=$is_truncated, has_token=$([[ -n "$token" ]] && echo yes || echo no)"
        
        [[ "$is_truncated" != "true" ]] && break
        ((page++))
    done
    
    rm -f "$temp_json"
    
    # Ensure all writes to temp_keys are complete
    sync 2>/dev/null || true
    
    # Count and output files
    [[ "$debug" == "1" ]] && log_info "DEBUG: About to output files from $temp_keys"
    [[ "$debug" == "1" ]] && log_info "DEBUG: temp_keys file size: $(wc -l < "$temp_keys" 2>/dev/null | tr -d ' ') lines"
    [[ "$debug" == "1" ]] && log_info "DEBUG: First 3 keys in temp file:"
    [[ "$debug" == "1" ]] && head -n 3 "$temp_keys" >&2 2>/dev/null || true
    [[ "$debug" == "1" ]] && log_info "DEBUG: Outputting all files with cat and sed..."
    
    local count=0
    if [[ -s "$temp_keys" ]]; then
        # Use sed to prepend bucket URL to each line and output all at once
        sed "s|^|s3://${bucket}/|" "$temp_keys"
        count=$(wc -l < "$temp_keys" | tr -d ' ')
    fi
    
    [[ "$debug" == "1" ]] && log_info "DEBUG: Output $count files to stdout"
    
    rm -f "$temp_keys"
    
    if [ $count -eq 0 ]; then
        log_warning "No .parquet files found under s3://${bucket}/${key_prefix}"
    else
        log_info "Found $count .parquet file(s)"
    fi
}

# Validate a single file
# When running in parallel, writes result to temp file instead of modifying global vars
validate_file() {
    local file="$1"
    local result_file="$2"  # optional: temp file for parallel results
    local force_flag=""
    
    [[ "$FORCE_DOWNLOAD" -eq 1 ]] && force_flag="--force"
    
    echo ""
    log_header "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log_header "Validating: $file"
    log_header "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    
    if "$VALIDATOR" $force_flag "$file"; then
        if [[ -n "$result_file" ]]; then
            echo "PASS|$file" >> "$result_file"
        else
            ((PASSED_FILES++))
        fi
        return 0
    else
        if [[ -n "$result_file" ]]; then
            echo "FAIL|$file" >> "$result_file"
        else
            ((FAILED_FILES++))
            FAILED_LIST+=("$file")
        fi
        return 1
    fi
}

# Print summary report
print_summary() {
    echo ""
    echo ""
    log_header "════════════════════════════════════════════════════════════════"
    log_header "                     VALIDATION SUMMARY                          "
    log_header "════════════════════════════════════════════════════════════════"
    echo ""
    echo -e "  ${BOLD}Total files:${NC}    $TOTAL_FILES"
    echo -e "  ${GREEN}${BOLD}Passed:${NC}         $PASSED_FILES"
    echo -e "  ${RED}${BOLD}Failed:${NC}         $FAILED_FILES"
    echo ""
    
    if [ $FAILED_FILES -gt 0 ]; then
        log_error "Failed validations:"
        for f in "${FAILED_LIST[@]}"; do
            echo "    - $f"
        done
        echo ""
    fi
    
    if [ $FAILED_FILES -eq 0 ]; then
        log_success "════════════════════════════════════════════════════════════════"
        log_success "           ALL VALIDATIONS PASSED SUCCESSFULLY! ✓              "
        log_success "════════════════════════════════════════════════════════════════"
    else
        log_error "════════════════════════════════════════════════════════════════"
        log_error "        SOME VALIDATIONS FAILED - SEE DETAILS ABOVE             "
        log_error "════════════════════════════════════════════════════════════════"
    fi
    echo ""
}

# Main script
main() {
    local -a files_to_validate=()
    
    # Parse arguments
    while [ $# -gt 0 ]; do
        case "$1" in
            -h|--help)
                usage
                exit 0
                ;;
            -f|--force)
                FORCE_DOWNLOAD=1
                shift
                ;;
            -p|--parallel)
                MAX_PARALLEL="$2"
                shift 2
                ;;
            -c|--continue)
                CONTINUE_ON_ERROR=1
                shift
                ;;
            --stop-on-error)
                CONTINUE_ON_ERROR=0
                shift
                ;;
            -*)
                log_error "Unknown option: $1"
                usage
                exit 2
                ;;
            *)
                files_to_validate+=("$1")
                shift
                ;;
        esac
    done
    
    # Check if validator script exists
    if [ ! -f "$VALIDATOR" ]; then
        log_error "Validator script not found: $VALIDATOR"
        log_error "Set VALIDATOR environment variable or ensure validate_parquet.sh is in the current directory"
        exit 2
    fi
    
    if [ ! -x "$VALIDATOR" ]; then
        log_error "Validator script is not executable: $VALIDATOR"
        log_error "Run: chmod +x $VALIDATOR"
        exit 2
    fi
    
    # Check for AWS CLI if we might need it
    if [ ${#files_to_validate[@]} -gt 0 ]; then
        for arg in "${files_to_validate[@]}"; do
            if is_s3_url "$arg"; then
                if ! command -v aws &> /dev/null; then
                    log_error "AWS CLI not found but S3 URLs provided"
                    log_error "Install with: brew install awscli  (macOS)"
                    exit 2
                fi
                break
            fi
        done
    fi
    
    # Collect files from FILES_LIST if provided
    if [ -n "$FILES_LIST" ]; then
        if [ ! -f "$FILES_LIST" ]; then
            log_error "FILES_LIST file not found: $FILES_LIST"
            exit 2
        fi
        log_info "Reading files from: $FILES_LIST"
        while IFS= read -r line; do
            # Skip empty lines and comments
            [[ -z "$line" || "$line" == \#* ]] && continue
            files_to_validate+=("$line")
        done < "$FILES_LIST"
    fi
    
    # Check if we have any files to validate
    if [ ${#files_to_validate[@]} -eq 0 ]; then
        log_error "No files or prefixes provided"
        usage
        exit 2
    fi
    
    log_info "Starting validation process"
    log_info "Validator: $VALIDATOR"
    log_info "Continue on error: $CONTINUE_ON_ERROR"
    log_info "Force download: $FORCE_DOWNLOAD"
    log_info "Max parallel: $MAX_PARALLEL"
    echo ""
    
    # Expand S3 prefixes to individual files
    local -a expanded_files=()
    for item in "${files_to_validate[@]}"; do
        if is_s3_url "$item" && is_s3_prefix "$item"; then
            # S3 prefix - list all parquet files under it
            log_info "Expanding S3 prefix: $item"
            local file_count=0
            while IFS= read -r file; do
                if [[ -n "$file" ]]; then
                    expanded_files+=("$file")
                    ((file_count++)) || true
                fi
            done < <(list_s3_parquet_files "$item")
            log_info "Added $file_count files from prefix expansion"
        else
            # Individual file (local or S3)
            expanded_files+=("$item")
        fi
    done
    
    TOTAL_FILES=${#expanded_files[@]}
    
    if [ $TOTAL_FILES -eq 0 ]; then
        log_warning "No files found to validate"
        exit 0
    fi
    
    log_info "Found $TOTAL_FILES file(s) to validate"
    echo ""
    
    # Validate files
    if [ "$MAX_PARALLEL" -eq 1 ]; then
        # Sequential validation
        for file in "${expanded_files[@]}"; do
            if ! validate_file "$file" ""; then
                if [ "$CONTINUE_ON_ERROR" -eq 0 ]; then
                    log_error "Stopping on first failure (--stop-on-error)"
                    print_summary
                    exit 1
                fi
            fi
        done
    else
        # Parallel validation using background jobs
        log_info "Running up to $MAX_PARALLEL validations in parallel"
        
        # Create temp file for collecting results (and keep it for debugging)
        local results_file
        results_file="$(mktemp -t validation_results.XXXX.txt)"
        log_info "Results will be saved to: $results_file"
        
        local -a pids=()
        local -a pid_files=()
        
        for file in "${expanded_files[@]}"; do
            # Wait if we've reached max parallel jobs
            while [ ${#pids[@]} -ge $MAX_PARALLEL ]; do
                # Check each PID to see if it's finished
                local -a new_pids=()
                local -a new_pid_files=()
                for i in "${!pids[@]}"; do
                    if kill -0 "${pids[$i]}" 2>/dev/null; then
                        # Still running
                        new_pids+=("${pids[$i]}")
                        new_pid_files+=("${pid_files[$i]}")
                    else
                        # Finished - collect exit status
                        wait "${pids[$i]}" || true
                    fi
                done
                # Update arrays, handling empty case
                if [ ${#new_pids[@]} -gt 0 ]; then
                    pids=("${new_pids[@]}")
                    pid_files=("${new_pid_files[@]}")
                else
                    pids=()
                    pid_files=()
                fi
                
                # If still at max, sleep briefly before checking again
                [ ${#pids[@]} -ge $MAX_PARALLEL ] && sleep 0.1
            done
            
            # Start validation in background
            validate_file "$file" "$results_file" &
            pids+=($!)
            pid_files+=("$file")
        done
        
        # Wait for remaining jobs
        if [ ${#pids[@]} -gt 0 ]; then
            for pid in "${pids[@]}"; do
                wait "$pid" || true
            done
        fi
        
        # Process results from temp file
        if [[ -s "$results_file" ]]; then
            while IFS='|' read -r status file; do
                if [[ "$status" == "PASS" ]]; then
                    ((PASSED_FILES++))
                elif [[ "$status" == "FAIL" ]]; then
                    ((FAILED_FILES++))
                    FAILED_LIST+=("$file")
                fi
            done < "$results_file"
        fi
        
        log_info "Results file preserved at: $results_file"
        log_info "To analyze: grep '^FAIL' $results_file"
    fi
    
    # Print summary
    print_summary
    
    # Exit with appropriate code
    if [ $FAILED_FILES -gt 0 ]; then
        exit 1
    else
        exit 0
    fi
}

# Run main function
main "$@"
