# ---- base runtime ----
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions
ENV PATH=/usr/local/bin:$PATH

ARG DUCKDB_VERSION=v1.4.1
ARG AWSCLI_ZIP_URL="https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip"

# Common tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl unzip jq tzdata bash coreutils procps \
 && rm -rf /var/lib/apt/lists/*

# ---- DuckDB CLI v1.4.1 ----
RUN set -eux; \
  cd /tmp; \
  curl -fsSL -o duckdb_cli.zip "https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip"; \
  unzip duckdb_cli.zip; \
  install -m 0755 duckdb /usr/local/bin/duckdb; \
  rm -f duckdb_cli.zip duckdb

# ---- Preinstall DuckDB extensions (offline-friendly) ----
RUN set -eux; \
  mkdir -p "${DUCKDB_EXTENSION_DIRECTORY}"; \
  duckdb /tmp/bootstrap.duckdb -cmd "INSTALL postgres_scanner; INSTALL httpfs;"; \
  rm -f /tmp/bootstrap.duckdb

# ---- AWS CLI v2 ----
RUN set -eux; \
  curl -fsSL -o /tmp/awscliv2.zip "${AWSCLI_ZIP_URL}"; \
  unzip -q /tmp/awscliv2.zip -d /tmp; \
  /tmp/aws/install; \
  rm -rf /tmp/aws /tmp/awscliv2.zip

# ---- app files ----
WORKDIR /app
# Copy your scripts (ensure these files exist next to the Dockerfile)
COPY pg_to_parquet_etl.sh etl_controller.sh partitions.txt /app/
RUN chmod +x /app/*.sh

# ---- non-root user ----
RUN useradd -m -u 10001 worker && chown -R worker:worker /app /opt/duckdb
USER worker

# Sensible defaults for ETL config (all overrideable)
ENV CHUNK_SIZE=75000 \
    FLUSH_THRESHOLD=1000000 \
    S3_USE_SSL=true

# Run the controller by default
ENTRYPOINT ["/app/etl_controller.sh"]
# Partitions can be provided by CLI args, PARTITIONS env, or PARTITIONS_FILE
CMD []
