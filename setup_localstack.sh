#!/bin/bash
# Setup script for LocalStack S3 buckets

set -e

echo "=========================================="
echo "Setting up LocalStack S3 buckets..."
echo "=========================================="

# Wait for LocalStack to be ready
echo "Waiting for LocalStack to start..."
sleep 10

# Configure AWS CLI for LocalStack
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-2

# Check if awslocal is available
if command -v awslocal &> /dev/null; then
    AWS_CMD="awslocal"
    echo "Using awslocal command"
else
    AWS_CMD="aws --endpoint-url=http://localhost:4566"
    echo "Using aws command with endpoint override"
fi

# Create S3 bucket
BUCKET_NAME="${S3_BUCKET:-dagster-parquet-data}"

echo ""
echo "Creating S3 bucket: $BUCKET_NAME"
$AWS_CMD s3 mb s3://$BUCKET_NAME 2>/dev/null || echo "Bucket already exists"

# Verify bucket was created
echo ""
echo "Verifying bucket..."
$AWS_CMD s3 ls

echo ""
echo "=========================================="
echo "LocalStack S3 setup complete!"
echo "=========================================="
echo ""
echo "Bucket: s3://$BUCKET_NAME"
echo "Endpoint: http://localhost:4566"
echo "Region: us-east-2"
echo ""
echo "You can now materialize Dagster assets."
echo "Files will be stored in LocalStack S3."
echo ""
echo "To view files:"
echo "  $AWS_CMD s3 ls s3://$BUCKET_NAME/ --recursive"
echo ""
