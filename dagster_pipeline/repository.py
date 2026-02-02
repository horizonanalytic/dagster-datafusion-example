"""
Dagster Repository Definitions

Wires together:
- Assets (healthcare + ecommerce domains)
- IO Managers (standard + partitioned Parquet writers)
- Environment-based configuration (local, UAT, prod)
"""

import os
from dagster import Definitions, EnvVar

from dagster_pipeline.assets import all_assets
from dagster_pipeline.io_managers import S3ParquetIOManager, PartitionedS3ParquetIOManager


# ========== ENVIRONMENT DETECTION ==========

def get_environment() -> str:
    """
    Detect environment: local, uat, or prod

    Returns:
    - "local": Use LocalStack S3 emulation
    - "uat": Use real S3 with UAT bucket
    - "prod": Use real S3 with production bucket
    """
    return os.getenv("ENVIRONMENT", "local").lower()


def get_io_manager_config():
    """
    Configure IO managers based on environment

    Local:
    - Uses LocalStack (S3 emulator at http://localstack:4566)
    - Credentials: test/test
    - Bucket: dagster-parquet-data

    UAT/Prod:
    - Uses real S3
    - Credentials: From AWS env vars or IAM role
    - Bucket: From environment variable
    """
    environment = get_environment()

    if environment == "local":
        # LocalStack configuration
        bucket = os.getenv("S3_BUCKET", "dagster-parquet-data")
        prefix = os.getenv("S3_PREFIX", "datafusion_test")

        # Set LocalStack endpoint
        os.environ["AWS_ENDPOINT_URL"] = os.getenv(
            "AWS_ENDPOINT_URL",
            "http://localstack:4566"
        )
        os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "test")
        os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
        os.environ["AWS_REGION"] = os.getenv("AWS_REGION", "us-east-2")

        print(f"[LocalStack] Using bucket: {bucket}, prefix: {prefix}")

        return {
            "bucket": bucket,
            "prefix": prefix,
        }

    elif environment == "uat":
        # UAT S3 configuration
        bucket = os.getenv("UAT_S3_BUCKET")
        prefix = os.getenv("UAT_S3_PREFIX", "dagster_uat")

        if not bucket:
            raise ValueError("UAT_S3_BUCKET environment variable required for UAT environment")

        print(f"[UAT] Using S3 bucket: {bucket}, prefix: {prefix}")

        return {
            "bucket": bucket,
            "prefix": prefix,
        }

    elif environment == "prod":
        # Production S3 configuration
        bucket = os.getenv("PROD_S3_BUCKET")
        prefix = os.getenv("PROD_S3_PREFIX", "dagster_prod")

        if not bucket:
            raise ValueError("PROD_S3_BUCKET environment variable required for production environment")

        print(f"[Production] Using S3 bucket: {bucket}, prefix: {prefix}")

        return {
            "bucket": bucket,
            "prefix": prefix,
        }

    else:
        raise ValueError(f"Unknown environment: {environment}. Expected: local, uat, or prod")


# ========== RESOURCE CONFIGURATION ==========

io_config = get_io_manager_config()

# Standard Parquet IO Manager (for simple assets)
parquet_io_manager = S3ParquetIOManager(
    bucket=io_config["bucket"],
    prefix=io_config["prefix"],
)

# Partitioned Parquet IO Manager (for HIVE-style partitioning)
partitioned_parquet_io_manager = PartitionedS3ParquetIOManager(
    bucket=io_config["bucket"],
    prefix=io_config["prefix"],
)


# ========== DAGSTER DEFINITIONS ==========

defs = Definitions(
    assets=all_assets,
    resources={
        "parquet_io_manager": parquet_io_manager,
        "partitioned_parquet_io_manager": partitioned_parquet_io_manager,
    },
)
