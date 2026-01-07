#!/usr/bin/env bash

# LocalStack provisioning script for the upload service
# This script sets up the necessary AWS resources for integration testing:
# - S3 buckets for data item storage
# - SQS queues for job processing
# - DynamoDB tables for caching and offset tracking
# - Secrets Manager for sensitive configuration

# Provide for running locally, if desired
if [[ "$ENSURE_DOCKER" == "true" ]]; then
    if ! command -v docker &>/dev/null; then
        echo "Docker is not installed. Please install Docker."
        exit 1
    fi

    # Ensure that the Docker daemon is up and running
    if ! docker info &>/dev/null; then
        echo "Docker daemon is not running. Please start Docker."
        exit 1
    fi

    localstack_container_name="localstack"

    # Check if there are any running containers with the name "localstack"
    if docker ps | grep -q "$localstack_container_name"; then
        echo "LocalStack container is running."
    else
        echo "LocalStack container is not running."
        exit 1
    fi
fi

echo "Provisioning LocalStack resources..."

# AWS CLI profile for LocalStack
profile="${AWS_PROFILE:-localstack}"
endpoint_url="${ENDPOINT_URL:-http://localstack:4566}"

# Check if DynamoDB service is available
check_dynamodb_available() {
    if aws --endpoint-url=$endpoint_url --profile=$profile dynamodb list-tables &>/dev/null; then
        echo "DynamoDB service is available."
        return 0
    else
        echo "DynamoDB service is not available in LocalStack."
        return 1
    fi
}

check_s3_bucket_exists() {
    bucket_name=$1
    # Use the head-bucket command to check if the bucket exists
    if aws --endpoint-url=$endpoint_url --profile=$profile s3api head-bucket --bucket "$bucket_name" &>/dev/null; then
        return 0 # The bucket exists
    else
        return 1 # The bucket does not exist or an error occurred
    fi
}

create_s3_bucket() {
    bucket_name=$1
    if check_s3_bucket_exists $bucket_name; then
        echo "Bucket $bucket_name already exists."
    else
        aws --endpoint-url=$endpoint_url --profile=$profile s3 mb s3://$bucket_name || exit 1
        echo "Bucket $bucket_name created."
    fi
}

check_sqs_queue_exists() {
    queue_name=$1
    # Attempt to retrieve the queue URL and check if the command was successful
    if aws --endpoint-url=$endpoint_url --profile=$profile sqs get-queue-url --queue-name $queue_name --output text --query 'QueueUrl' &>/dev/null; then
        return 0 # The queue exists
    else
        return 1 # The queue does not exist or an error occurred
    fi
}

create_dlq() {
    dlq_name="${1}-dlq"
    retention_period=1209600 # 14 days in seconds
    echo "Creating dead letter queue '$dlq_name' with retention period of $retention_period seconds."
    aws --endpoint-url=$endpoint_url --profile=$profile sqs create-queue \
        --queue-name $dlq_name \
        --attributes MessageRetentionPeriod=$retention_period || exit 1
    echo "Dead letter queue $dlq_name created."
}

get_queue_arn() {
    queue_name=$1
    aws --endpoint-url=$endpoint_url --profile=$profile sqs get-queue-attributes \
        --queue-url "http://localhost:4566/000000000000/$queue_name" \
        --attribute-names QueueArn --query 'Attributes.QueueArn' --output text
}

create_sqs_queue() {
    queue_name=$1
    max_receive_count=$2
    visibility_timeout=$3
    delay_seconds=$4
    message_retention_seconds=$5

    if check_sqs_queue_exists $queue_name; then
        echo "Queue $queue_name already exists."
    else
        # Create the Dead Letter Queue first
        create_dlq $queue_name
        dlq_arn=$(get_queue_arn "${queue_name}-dlq")

        # Start building the attributes JSON
        attributes="{"
        attributes+="\"VisibilityTimeout\": \"$visibility_timeout\","
        attributes+="\"DelaySeconds\": \"$delay_seconds\","
        attributes+="\"RedrivePolicy\": \"{\\\"maxReceiveCount\\\": \\\"$max_receive_count\\\", \\\"deadLetterTargetArn\\\": \\\"$dlq_arn\\\"}\""

        # Conditionally add MessageRetentionPeriod if provided
        if [[ -n "$message_retention_seconds" ]]; then
            attributes+=", \"MessageRetentionPeriod\": \"$message_retention_seconds\""
        fi

        # Close the JSON object
        attributes+="}"

        # Create the source queue with specified attributes
        echo "Creating queue: $queue_name with configured attributes."
        aws --endpoint-url=$endpoint_url --profile=$profile sqs create-queue --queue-name $queue_name \
            --attributes "$attributes" || exit 1
        echo "Queue $queue_name created with DLQ settings."
    fi
}

check_secret_exists() {
    secret_name=$1

    aws --endpoint-url=$endpoint_url --profile $profile secretsmanager describe-secret \
        --secret-id "$secret_name" &>/dev/null

    if [ $? -eq 0 ]; then
        return 0 # Secret exists
    else
        return 1 # Secret does not exist
    fi
}

create_secret() {
    secret_name=$1
    secret_value=$2
    description="${3:-"No description provided"}"

    if check_secret_exists "$secret_name"; then
        echo "Secret '$secret_name' already exists."
    else
        echo "Creating secret '$secret_name'."
        aws --endpoint-url=$endpoint_url --profile $profile secretsmanager create-secret \
            --name "$secret_name" \
            --description "$description" \
            --secret-string "$secret_value"
    fi
}

check_dynamodb_table_exists() {
    table_name=$1

    if aws --endpoint-url=$endpoint_url --profile=$profile dynamodb describe-table --table-name "$table_name" &>/dev/null; then
        return 0 # Table exists
    else
        return 1 # Table does not exist
    fi
}

create_dynamodb_table() {
    table_name=$1
    ttl_attribute_name="${2:-X}"

    if check_dynamodb_table_exists "$table_name"; then
        echo "DynamoDB table '$table_name' already exists."
    else
        echo "Creating DynamoDB table '$table_name'."

        # Create table with binary primary key
        aws --endpoint-url=$endpoint_url --profile=$profile dynamodb create-table \
            --table-name "$table_name" \
            --attribute-definitions AttributeName=Id,AttributeType=B \
            --key-schema AttributeName=Id,KeyType=HASH \
            --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 || exit 1

        echo "Waiting for table '$table_name' to be active..."
        aws --endpoint-url=$endpoint_url --profile=$profile dynamodb wait table-exists --table-name "$table_name"

        echo "Enabling TTL for table '$table_name' on attribute '$ttl_attribute_name'."
        aws --endpoint-url=$endpoint_url --profile=$profile dynamodb update-time-to-live \
            --table-name "$table_name" \
            --time-to-live-specification "Enabled=true,AttributeName=$ttl_attribute_name"

        echo "DynamoDB table '$table_name' created with TTL enabled."
    fi
}

bucket_name="${DATA_ITEM_BUCKET:-raw-data-items}"

# DynamoDB table names (matching the patterns used in the application)
cache_table_name="${DDB_DATA_ITEM_TABLE:-upload-service-cache-${NODE_ENV:-local}}"
offsets_table_name="${DDB_OFFSETS_TABLE:-upload-service-offsets-${NODE_ENV:-local}}"

# Create resources
create_s3_bucket $bucket_name

create_sqs_queue "finalize-multipart-queue" 3 30 0 "" # Max Receives=3, Visibility Timeout=30s, Delay Seconds=0s, no custom retention period
create_sqs_queue "batch-insert-new-data-items-queue" 3 60 0 3600
create_sqs_queue "bdi-unbundle-queue" 2 315 0 ""
create_sqs_queue "prepare-bundle-queue" 4 315 3 ""
create_sqs_queue "post-bundle-queue" 4 315 3 ""
create_sqs_queue "seed-bundle-queue" 4 315 3 ""
create_sqs_queue "optical-post-queue" 1 45 0 600
create_sqs_queue "put-offsets-queue" 4 300 0 ""

# Create DynamoDB tables if service is available
if check_dynamodb_available; then
    echo "Creating DynamoDB tables for upload service..."
    create_dynamodb_table "$cache_table_name" "X"
    create_dynamodb_table "$offsets_table_name" "X"
    echo "DynamoDB tables created:"
    echo "  - Cache table: $cache_table_name"
    echo "  - Offsets table: $offsets_table_name"
else
    echo "Skipping DynamoDB table creation - service not available."
fi

create_secret "arweave-wallet" "${ARWEAVE_WALLET}" "Arweave wallet for Turbo uploads, receipts, and optical bridging"
create_secret "turbo-evm-data-item-signing-key" "${TURBO_EVM_DATA_ITEM_SIGNING_KEY}" "EVM Data Item Signing Key for unsigned uploads in Turbo"
create_secret "turbo-optical-key-${NODE_ENV}" "${TURBO_OPTICAL_KEY}" "Turbo Optical Key for ${NODE_ENV} environment"
