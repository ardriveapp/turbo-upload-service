# PostgreSQL to Parquet ETL Scripts

This directory contains scripts for exporting PostgreSQL data to Parquet format with optional S3 upload.

## Overview

### Scripts

#### `pg_to_parquet_etl.sh`

The main ETL script that:

- Connects to a PostgreSQL database and extracts data from permanent data item partitions
- Uses DuckDB as an intermediate buffer for data transformation and normalization
- Converts data to compressed Parquet format (ZSTD compression)
- Optionally uploads Parquet files to S3 with Hive-style partitioning
- Supports resuming from previous runs via DuckDB buffer, local Parquet files, or S3

#### `etl_controller.sh`

A wrapper script that:

- Orchestrates ETL runs across multiple partitions serially
- Manages separate state directories and output directories per partition
- Handles retries with configurable backoff
- Provides a summary report of successes and failures

#### `validate_parquet.sh`

A validation script that:

- Verifies Parquet file integrity by checking embedded checksums in filenames
- Validates row count, start/end timestamps, and start/end data item IDs
- Supports both local files and S3 URLs (downloads automatically)
- Uses DuckDB to read Parquet files and extract actual values
- Converts between base64 and base64url encoding for ID validation

#### `validate_all_parquet.sh`

A batch validation controller that:

- Validates multiple Parquet files in parallel
- Automatically expands S3 prefixes to find all .parquet files
- Supports parallel execution with configurable job limits
- Preserves detailed results to temp files for later analysis
- Provides summary reports of passed/failed validations

## Quick Start

### Single Partition Export

```bash
# Basic usage
./pg_to_parquet_etl.sh public.permanent_data_items_01_2023_01

# With custom configuration
PGHOST=my-db.amazonaws.com \
CHUNK_SIZE=5000 \
FLUSH_THRESHOLD=50000 \
S3_BUCKET=my-bucket \
./pg_to_parquet_etl.sh public.permanent_data_items_01_2023_01
```

### Multiple Partitions Export

```bash
# Using CLI arguments
./etl_controller.sh public.table1 public.table2 public.table3

# Using a partitions file
PARTITIONS_FILE=./partitions.txt ./etl_controller.sh

# Using environment variable
PARTITIONS="public.table1,public.table2" ./etl_controller.sh
```

### Validate Parquet Files

```bash
# Validate a single file
./validate_parquet.sh file.parquet

# Validate an S3 file
./validate_parquet.sh s3://bucket/path/file.parquet

# Validate all files under an S3 prefix
./validate_all_parquet.sh s3://bucket/prefix/

# Validate with parallel execution (4 jobs at once)
./validate_all_parquet.sh --parallel 4 s3://bucket/prefix/

# Force re-download from S3
./validate_all_parquet.sh --force s3://bucket/prefix/
```

## Configuration Options

### pg_to_parquet_etl.sh

#### PostgreSQL Connection

- `PGHOST`: PostgreSQL host (default: upload-service-dev-2.c78cj9lqymhu.us-east-1.rds.amazonaws.com)
- `PGPORT`: PostgreSQL port (default: 5432)
- `PGDATABASE`: Database name (default: postgres)
- `PGUSER`: Username (default: postgres)
- `PGPASSWORD`: Password (optional; use .pgpass or IAM auth if omitted)

#### Processing Parameters

- `CHUNK_SIZE`: Rows to fetch per pull from PostgreSQL (default: 10000)
- `FLUSH_THRESHOLD`: Rows to accumulate before writing a Parquet file (default: 20000)

#### DuckDB Configuration

- `DUCKDB_FILE`: Path to DuckDB file (default: export.duckdb)
- `DUCKDB_SCHEMA`: DuckDB schema name (default: turbo)
- `DUCKDB_TABLE_RAW`: Raw data table name (default: permanent_data_items_raw)
- `DUCKDB_TABLE_NORM`: Normalized data table name (default: permanent_data_items_norm)

#### Parquet Output

- `PARQUET_DIR`: Output directory for Parquet files (default: ./parquet_out)
- `PARQUET_PREFIX`: Prefix for Parquet filenames (default: perm_items)

#### S3 Upload (Optional)

- `S3_BUCKET`: S3 bucket name (leave empty to disable S3 upload)
- `S3_PREFIX`: S3 key prefix (default: etl/perm_items)
- `S3_REGION`: AWS region (optional; uses AWS defaults if omitted)
- `S3_USE_SSL`: Use SSL for S3 (default: true)
- `S3_LAYOUT`: S3 layout style (default: hive-date)
  - `hive-date`: Organizes files as `year=YYYY/month=MM/half=HH/`
  - `by-partition`: Organizes by schema and table
  - `flat`: No hierarchical structure
- `S3_RESUME`: Enable resume from S3 (default: 1)
- `S3_MAX_KEYS`: S3 list page size (default: 2000)

#### Resume & Debug Options

- `S3_RESUME_DEBUG`: Enable verbose S3 resume logging (default: 0)
- `S3_STRICT_LAYOUT`: Enforce strict layout matching for resume (default: 1)
- `S3_RESUME_FALLBACK`: Allow fallback to alternate layout (default: 0)
- `HIVE_SPLIT`: Split by year/month/half for hive-date layout (default: 1)

### etl_controller.sh

- `RETRIES`: Total attempts per partition (default: 2)
- `RETRY_BACKOFF_SEC`: Seconds between retry attempts (default: 5)
- `ETL`: Path to the ETL script (default: ./pg_to_parquet_etl.sh)
- `STATE_DIR`: Directory for per-partition DuckDB state files (default: ./etl_state)
- `OUT_ROOT`: Parent directory for Parquet output (default: ./parquet_out)
- `PARTITIONS`: Comma or newline separated partition list (alternative to CLI args)
- `PARTITIONS_FILE`: Path to file containing partition names, one per line

### validate_parquet.sh

- `TEMP_DIR`: Directory for temporary S3 downloads (default: ./temp)
- `S3_REGION`: AWS region for S3 operations (optional)

### validate_all_parquet.sh

- `VALIDATOR`: Path to validate_parquet.sh script (default: ./validate_parquet.sh)
- `FILES_LIST`: Path to file containing list of files/prefixes (one per line)
- `CONTINUE_ON_ERROR`: Continue on errors: 1=yes, 0=no (default: 1)
- `FORCE_DOWNLOAD`: Force S3 re-download: 1=yes, 0=no (default: 0)
- `MAX_PARALLEL`: Maximum parallel validations (default: 1)
- `S3_REGION`: AWS region for S3 operations (optional)
- `S3_LIST_DEBUG`: Enable verbose S3 listing debug output: 1=yes, 0=no (default: 0)

## Parquet File Validation

The validation scripts verify data integrity by checking that the checksums embedded in Parquet filenames match the actual file contents.

### What Gets Validated

Each Parquet file has metadata encoded in its filename:

- **Row count**: Total number of records in the file
- **Start timestamp**: Earliest `uploaded_date` in the file
- **End timestamp**: Latest `uploaded_date` in the file
- **Start ID**: First 8 characters (base64url) of the earliest `data_item_id`
- **End ID**: First 8 characters (base64url) of the latest `data_item_id`

The validator:

1. Parses these values from the filename
2. Reads the Parquet file using DuckDB
3. Queries the actual min/max values with proper ordering (by `uploaded_date`, then `data_item_id` as tiebreaker)
4. Converts between base64 and base64url encoding as needed
5. Compares all five checksums

### Single File Validation

```bash
# Validate a local file
./validate_parquet.sh ./parquet_out/file.parquet

# Validate an S3 file (auto-downloads to TEMP_DIR)
./validate_parquet.sh s3://bucket/path/file.parquet

# Force re-download even if local copy exists
./validate_parquet.sh --force s3://bucket/path/file.parquet
```

**Exit codes:**

- `0`: All checksums match
- `1`: One or more checksums don't match
- `2`: Invalid arguments or file not found

### Batch Validation

```bash
# Validate all files under an S3 prefix
./validate_all_parquet.sh s3://bucket/prefix/year=2024/

# Parallel validation (4 concurrent jobs)
./validate_all_parquet.sh --parallel 4 s3://bucket/prefix/

# Continue even if some files fail
./validate_all_parquet.sh --continue s3://bucket/prefix/

# Stop on first failure
./validate_all_parquet.sh --stop-on-error s3://bucket/prefix/

# Validate specific files
./validate_all_parquet.sh file1.parquet file2.parquet s3://bucket/file3.parquet

# Use a file list
echo "file1.parquet" > files.txt
echo "s3://bucket/file2.parquet" >> files.txt
FILES_LIST=files.txt ./validate_all_parquet.sh
```

**Exit codes:**

- `0`: All validations passed
- `1`: One or more validations failed
- `2`: Invalid arguments or setup error

### Results Preservation

When running batch validations with `validate_all_parquet.sh`:

- Results are saved to `/tmp/validation_results.XXXX.txt`
- Each line is either `PASS|filename` or `FAIL|filename`
- The temp file is preserved after completion for later analysis
- The script outputs the file location at the start and end of the run

To analyze results later:

```bash
# Find the most recent results file
ls -t /tmp/validation_results.*.txt | head -1

# Count passes and failures
grep -c '^PASS' /tmp/validation_results.XXXX.txt
grep -c '^FAIL' /tmp/validation_results.XXXX.txt

# List failed files
grep '^FAIL' /tmp/validation_results.XXXX.txt
```

### When to Validate

Validation is recommended:

1. **Before dropping partitions**: Ensure exported Parquet data is complete and correct
2. **After ETL runs**: Verify data integrity before considering the export successful
3. **Periodically**: Spot-check archived data to ensure no corruption over time
4. **After S3 migrations**: Verify data survived bucket transfers or policy changes

## Resume Behavior

The ETL script supports resuming interrupted exports in the following order:

1. **DuckDB buffer**: If the local DuckDB file contains rows, resume from the last row
2. **Local Parquet files**: If Parquet files exist locally, resume from the newest file
3. **S3**: If S3 is configured and enabled, find the latest file in S3 and resume from there

### Important Notes on Resuming

- **Resuming is possible** from the local DuckDB file between runs
- **Recommended practice**: Wipe the DuckDB file (`.duckdb` and `.duckdb.wal`) between fresh runs to avoid unintended resume behavior
- The script automatically removes the DuckDB file when clearing the buffer after each flush
- For clean starts, delete or move the state directory: `rm -rf ./etl_state/<partition>/`

## Parallel Execution

To run multiple ETL processes in parallel for better performance:

### Requirements for Parallel Runs

1. **Use different partition input files** or partition lists for each process
2. **Use different STATE_DIR values** to avoid DuckDB file conflicts
3. Each process will automatically use separate output directories per partition

### Example: Parallel Execution

```bash
# Terminal 1 - Process partitions 1-10
PARTITIONS_FILE=./partitions_1-10.txt \
STATE_DIR=./etl_state_1 \
OUT_ROOT=./parquet_out \
./etl_controller.sh &

# Terminal 2 - Process partitions 11-20
PARTITIONS_FILE=./partitions_11-20.txt \
STATE_DIR=./etl_state_2 \
OUT_ROOT=./parquet_out \
./etl_controller.sh &

# Terminal 3 - Process partitions 21-30
PARTITIONS_FILE=./partitions_21-30.txt \
STATE_DIR=./etl_state_3 \
OUT_ROOT=./parquet_out \
./etl_controller.sh &

# Wait for all to complete
wait
```

### Parallel Execution Tips

- Monitor system resources (CPU, memory, network) to determine optimal parallelism
- Each ETL process will create its own DuckDB instance and network connections
- S3 uploads are automatically retried (up to 3 attempts per file)
- The controller script handles per-partition isolation automatically

## Output Format

Parquet filenames encode metadata about their contents:

```
{prefix}_{schema}.{table}_{start_ts}_{start_id8}-{end_ts}_{end_id8}_rows-{count}.parquet
```

Example:

```
perm_items_public.permanent_data_items_01_2023_01_20230101T000000Z_AbCdEfGh-20230115T235959Z_XyZaBcDe_rows-20000.parquet
```

## S3 Layouts

### hive-date (default)

Partitions by date extracted from table name or file timestamp:

```
s3://bucket/prefix/year=2023/month=01/half=01/file.parquet
s3://bucket/prefix/year=2023/month=01/half=02/file.parquet
```

### by-partition

Organizes by schema and table:

```
s3://bucket/prefix/public.permanent_data_items_01_2023_01/file.parquet
```

### flat

No hierarchical structure:

```
s3://bucket/prefix/file.parquet
```

## Troubleshooting

### Resume Issues

If the script resumes from an unexpected position:

- Check for existing DuckDB files in `STATE_DIR/<partition>/`
- Check for local Parquet files in `PARQUET_DIR`
- Set `S3_RESUME_DEBUG=1` for verbose S3 resume logging
- Wipe state between runs: `rm -rf ${STATE_DIR}/<partition>/`

### DuckDB File Conflicts

If running parallel processes:

- Ensure each process uses a unique `STATE_DIR`
- Never point multiple processes at the same DuckDB file

### Upload Failures

If S3 uploads fail:

- Local Parquet files are preserved for retry
- On the next run, pending files are uploaded before new data is processed
- Check AWS credentials and permissions

### Memory Usage

If encountering memory issues:

- Reduce `CHUNK_SIZE` (smaller PostgreSQL pulls)
- Reduce `FLUSH_THRESHOLD` (more frequent Parquet writes)
- Monitor DuckDB file size growth

### Validation Failures

If Parquet validation fails:

- Check if the file was being written during validation (retry after completion)
- Verify S3 download completed successfully (use `--force` to re-download)
- Check for transient network errors during initial download
- If persistent, the file may have been written with incorrect metadata - consider re-exporting that partition

## Safe Partition Dropping Workflow

Before dropping historical partitions from PostgreSQL, follow this workflow to ensure data safety:

### 1. Export Partitions to Parquet

```bash
# Export specific partitions
./etl_controller.sh public.permanent_data_items_01_2024_01 public.permanent_data_items_01_2024_02

# Or use a partitions file
PARTITIONS_FILE=./partitions_to_drop.txt ./etl_controller.sh
```

### 2. Validate All Exported Data

```bash
# Validate all files for the exported year
./validate_all_parquet.sh --parallel 4 s3://your-bucket/etl/perm_items/year=2024/

# Check results
echo "Total: $(wc -l < /tmp/validation_results.*.txt)"
echo "Passed: $(grep -c '^PASS' /tmp/validation_results.*.txt)"
echo "Failed: $(grep -c '^FAIL' /tmp/validation_results.*.txt)"

# Investigate any failures
grep '^FAIL' /tmp/validation_results.*.txt
```

### 3. Verify Data Completeness

```bash
# Count rows in PostgreSQL partition
psql -c "SELECT COUNT(*) FROM public.permanent_data_items_01_2024_01;"

# Count rows across all Parquet files for that partition
# (Sum the row counts from filenames, or query DuckDB)
```

### 4. Drop Partitions (Only After Validation Passes)

```sql
-- Drop the partition (WARNING: This is destructive!)
DROP TABLE public.permanent_data_items_01_2024_01;

-- Or detach if you want to keep the table temporarily
ALTER TABLE permanent_data_items DETACH PARTITION permanent_data_items_01_2024_01;
```

### 5. Monitor Database Performance

After dropping partitions:

- Monitor query performance on the parent table
- Check that indexes are still being used effectively
- Verify that table statistics are up to date (run `ANALYZE` if needed)

## Data Schema

The ETL normalizes data with the following schema:

- `data_item_id` (BLOB, PK): Base64url-encoded data item identifier
- `owner_public_address` (BLOB): Base64url-encoded owner address
- `byte_count` (BIGINT): Size in bytes
- `uploaded_date` (TIMESTAMP): Upload timestamp
- `assessed_winston_price` (DECIMAL(20,0)): Price in winston
- `plan_id` (UUID): Plan identifier
- `planned_date` (TIMESTAMP): Planning timestamp
- `bundle_id` (BLOB): Base64url-encoded bundle identifier
- `permanent_date` (TIMESTAMP): Permanence timestamp
- `block_height` (INTEGER): Block height
- `data_start` (INTEGER, nullable): Data start offset
- `signature_type` (SMALLINT, nullable): Signature type indicator
- `failed_bundles` (BLOB[], nullable): Array of failed bundle IDs
- `content_type` (VARCHAR, nullable): Content MIME type
- `premium_feature_type` (VARCHAR, nullable): Premium feature indicator
- `deadline_height` (INTEGER, nullable): Deadline block height

## Dependencies

- **bash**: Shell script execution
- **DuckDB**: Data transformation and Parquet generation
- **AWS CLI**: S3 upload (optional)
- **PostgreSQL**: Source database

## Exit Codes

### pg_to_parquet_etl.sh

- `0`: Success
- `1`: Runtime error

### etl_controller.sh

- `0`: All partitions succeeded
- `1`: One or more partitions failed
- `2`: No partitions provided
