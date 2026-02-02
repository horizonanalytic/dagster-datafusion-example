"""
Custom Parquet IO Managers for S3

Provides two IO managers:
1. S3ParquetIOManager - Standard Parquet writer
2. PartitionedS3ParquetIOManager - HIVE-style partitioned Parquet writer

Both support:
- Local filesystem (for testing)
- LocalStack (local S3 emulation)
- Real S3 (UAT/production)
"""

import os
from pathlib import Path
from io import BytesIO
from typing import Union, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    MetadataValue,
)


def get_s3_client():
    """
    Get S3 client with environment-based configuration

    Supports:
    - LocalStack (ENVIRONMENT=local, AWS_ENDPOINT_URL set)
    - Real S3 (ENVIRONMENT=uat or prod)
    """
    import boto3

    endpoint_url = os.getenv("AWS_ENDPOINT_URL")  # For LocalStack
    region = os.getenv("AWS_REGION", "us-east-2")

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )

    return s3_client


class S3ParquetIOManager(ConfigurableIOManager):
    """
    Standard Parquet IO Manager for S3

    Writes DataFrames to Parquet files in S3 or local filesystem.

    Path structure:
    - Local: {base_path}/{asset_key_path}.parquet
    - S3: s3://{bucket}/{prefix}/{asset_key_path}.parquet

    Example:
    - Asset key: ["Silver", "TaxID", "raw_organizations"]
    - Output: s3://bucket/prefix/Silver/TaxID/raw_organizations.parquet
    """

    bucket: Optional[str] = None  # S3 bucket (None = use local filesystem)
    prefix: str = ""  # S3 prefix or local base path

    def _get_path(self, context: Union[OutputContext, InputContext]) -> str:
        """Build path from asset key"""
        # Asset key path: ["Silver", "TaxID", "raw_organizations"] -> "Silver/TaxID/raw_organizations"
        asset_path = "/".join(context.asset_key.path)

        if self.bucket:
            # S3 path
            if self.prefix:
                return f"{self.prefix}/{asset_path}.parquet"
            return f"{asset_path}.parquet"
        else:
            # Local path
            if self.prefix:
                return f"{self.prefix}/{asset_path}.parquet"
            return f"data/{asset_path}.parquet"

    def _get_full_uri(self, path: str) -> str:
        """Get full URI (S3 or local)"""
        if self.bucket:
            return f"s3://{self.bucket}/{path}"
        return path

    def handle_output(self, context: OutputContext, obj: Union[pd.DataFrame, object]):
        """
        Save DataFrame to Parquet file

        Handles:
        - pandas DataFrame -> Parquet
        - MaterializeResult -> Extract DataFrame
        """
        # Extract DataFrame from MaterializeResult if needed
        if hasattr(obj, '__class__') and obj.__class__.__name__ == 'MaterializeResult':
            # For MaterializeResult, we need to get the actual data
            # In this case, the asset function should return the DataFrame directly
            # when using this IO manager
            context.log.warning(
                "MaterializeResult detected. Ensure asset returns DataFrame when using this IO manager."
            )
            return

        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"Expected pandas DataFrame, got {type(obj)}")

        path = self._get_path(context)
        full_uri = self._get_full_uri(path)

        context.log.info(f"Writing Parquet file to {full_uri}")

        # Write to S3 or local
        if self.bucket:
            # S3 write
            s3_client = get_s3_client()

            # Convert DataFrame to Parquet in memory
            buffer = BytesIO()
            obj.to_parquet(
                buffer,
                engine='pyarrow',
                compression='snappy',
                index=False,
            )
            buffer.seek(0)

            # Upload to S3
            s3_client.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )

            # Get file size
            response = s3_client.head_object(Bucket=self.bucket, Key=path)
            file_size_bytes = response['ContentLength']
        else:
            # Local filesystem write
            local_path = Path(path)
            local_path.parent.mkdir(parents=True, exist_ok=True)

            obj.to_parquet(
                str(local_path),
                engine='pyarrow',
                compression='snappy',
                index=False,
            )

            file_size_bytes = local_path.stat().st_size

        # Calculate metrics
        file_size_mb = file_size_bytes / (1024 * 1024)
        row_count = len(obj)
        column_count = len(obj.columns)

        # Add output metadata
        context.add_output_metadata({
            "path": MetadataValue.path(full_uri),
            "row_count": row_count,
            "column_count": column_count,
            "file_size_mb": MetadataValue.float(round(file_size_mb, 2)),
            "file_size_bytes": file_size_bytes,
            "compression": "snappy",
        })

        context.log.info(
            f"Wrote {row_count:,} rows ({column_count} columns) "
            f"to {full_uri} ({file_size_mb:.2f} MB)"
        )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load Parquet file as DataFrame"""
        path = self._get_path(context)
        full_uri = self._get_full_uri(path)

        context.log.info(f"Reading Parquet file from {full_uri}")

        if self.bucket:
            # S3 read
            s3_client = get_s3_client()

            response = s3_client.get_object(Bucket=self.bucket, Key=path)
            buffer = BytesIO(response['Body'].read())

            df = pd.read_parquet(buffer, engine='pyarrow')
        else:
            # Local read
            df = pd.read_parquet(path, engine='pyarrow')

        context.log.info(f"Loaded {len(df):,} rows from {full_uri}")

        return df


class PartitionedS3ParquetIOManager(ConfigurableIOManager):
    """
    Partitioned Parquet IO Manager with HIVE-style partitioning

    Creates HIVE-style partition structure:
    s3://bucket/prefix/table_name/partition_key=partition_value/data.parquet

    Examples:
    - Single partition (date):
      s3://bucket/orgs/date=2024-01-15/data.parquet

    - Multi-partition (state + date):
      s3://bucket/orgs/state=CA/date=2024-01-15/data.parquet

    This enables DataFusion partition pruning:
      SELECT * FROM orgs WHERE state = 'CA'
      -> Only reads state=CA/* (skips other states)
    """

    bucket: Optional[str] = None
    prefix: str = ""

    def _get_partition_path(self, context: Union[OutputContext, InputContext]) -> str:
        """
        Build HIVE-style partition path

        For multi-partitioned assets, partition_key is like:
        MultiPartitionKey({
            "date": "2024-01-15",
            "state": "CA"
        })

        Output: state=CA/date=2024-01-15/
        """
        asset_path = "/".join(context.asset_key.path)

        # Handle partitioning
        if hasattr(context, 'partition_key') and context.partition_key:
            partition_key = context.partition_key

            # Check if multi-partitioned
            if hasattr(partition_key, 'keys_by_dimension'):
                # Multi-partition: {"date": "2024-01-15", "state": "CA"}
                dimensions = partition_key.keys_by_dimension

                # Build HIVE-style path: state=CA/date=2024-01-15/
                partition_parts = [
                    f"{key}={value}"
                    for key, value in sorted(dimensions.items())  # Sort for consistency
                ]
                partition_path = "/".join(partition_parts)
            else:
                # Single partition: date=2024-01-15
                partition_path = f"partition={partition_key}"

            if self.bucket:
                if self.prefix:
                    return f"{self.prefix}/{asset_path}/{partition_path}/data.parquet"
                return f"{asset_path}/{partition_path}/data.parquet"
            else:
                if self.prefix:
                    return f"{self.prefix}/{asset_path}/{partition_path}/data.parquet"
                return f"data/{asset_path}/{partition_path}/data.parquet"
        else:
            # No partition
            return self._get_path_no_partition(context)

    def _get_path_no_partition(self, context):
        """Fallback for non-partitioned assets"""
        asset_path = "/".join(context.asset_key.path)

        if self.bucket:
            if self.prefix:
                return f"{self.prefix}/{asset_path}.parquet"
            return f"{asset_path}.parquet"
        else:
            if self.prefix:
                return f"{self.prefix}/{asset_path}.parquet"
            return f"data/{asset_path}.parquet"

    def _get_full_uri(self, path: str) -> str:
        """Get full URI"""
        if self.bucket:
            return f"s3://{self.bucket}/{path}"
        return path

    def handle_output(self, context: OutputContext, obj: Union[pd.DataFrame, object]):
        """Save DataFrame with HIVE-style partitioning"""
        # Extract DataFrame from MaterializeResult if needed
        if hasattr(obj, '__class__') and obj.__class__.__name__ == 'MaterializeResult':
            context.log.warning(
                "MaterializeResult detected. Asset should return DataFrame when using this IO manager."
            )
            return

        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f"Expected pandas DataFrame, got {type(obj)}")

        path = self._get_partition_path(context)
        full_uri = self._get_full_uri(path)

        context.log.info(f"Writing partitioned Parquet to {full_uri}")

        # Write to S3 or local
        if self.bucket:
            # S3 write
            s3_client = get_s3_client()

            buffer = BytesIO()
            obj.to_parquet(buffer, engine='pyarrow', compression='snappy', index=False)
            buffer.seek(0)

            s3_client.put_object(
                Bucket=self.bucket,
                Key=path,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )

            response = s3_client.head_object(Bucket=self.bucket, Key=path)
            file_size_bytes = response['ContentLength']
        else:
            # Local write
            local_path = Path(path)
            local_path.parent.mkdir(parents=True, exist_ok=True)

            obj.to_parquet(str(local_path), engine='pyarrow', compression='snappy', index=False)

            file_size_bytes = local_path.stat().st_size

        file_size_mb = file_size_bytes / (1024 * 1024)

        # Add metadata
        context.add_output_metadata({
            "path": MetadataValue.path(full_uri),
            "row_count": len(obj),
            "file_size_mb": MetadataValue.float(round(file_size_mb, 2)),
            "partition_key": str(context.partition_key) if hasattr(context, 'partition_key') else "none",
        })

        context.log.info(
            f"Wrote {len(obj):,} rows to partitioned path: {full_uri} ({file_size_mb:.2f} MB)"
        )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load partitioned Parquet file"""
        path = self._get_partition_path(context)
        full_uri = self._get_full_uri(path)

        context.log.info(f"Reading partitioned Parquet from {full_uri}")

        if self.bucket:
            s3_client = get_s3_client()

            response = s3_client.get_object(Bucket=self.bucket, Key=path)
            buffer = BytesIO(response['Body'].read())

            df = pd.read_parquet(buffer, engine='pyarrow')
        else:
            df = pd.read_parquet(path, engine='pyarrow')

        context.log.info(f"Loaded {len(df):,} rows from {full_uri}")

        return df
