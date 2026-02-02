"""
Basic tests for Dagster assets and IO managers

Run with: pytest tests/ -v
"""

import pytest
import pandas as pd
from unittest.mock import Mock, MagicMock
from dagster import build_asset_context, build_output_context, AssetKey


def test_import_assets():
    """Test that assets can be imported"""
    from dagster_pipeline.assets import healthcare, ecommerce

    assert healthcare is not None
    assert ecommerce is not None


def test_import_io_managers():
    """Test that IO managers can be imported"""
    from dagster_pipeline.io_managers import S3ParquetIOManager, PartitionedS3ParquetIOManager

    assert S3ParquetIOManager is not None
    assert PartitionedS3ParquetIOManager is not None


def test_io_manager_config():
    """Test IO manager configuration"""
    from dagster_pipeline.io_managers import S3ParquetIOManager

    io_manager = S3ParquetIOManager(bucket="test-bucket", prefix="test-prefix")

    assert io_manager.bucket == "test-bucket"
    assert io_manager.prefix == "test-prefix"


def test_asset_execution_mock():
    """Test asset execution with mock context"""
    from dagster_pipeline.assets.healthcare import generate_organizations_data

    # Generate test data
    df = generate_organizations_data(num_rows=100, states=["CA", "NY"], date="2024-01-15")

    # Validate
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 100
    assert "grouping_id" in df.columns
    assert "state" in df.columns
    assert set(df['state'].unique()).issubset({"CA", "NY"})


def test_ecommerce_data_generation():
    """Test e-commerce data generation"""
    from dagster_pipeline.assets.ecommerce import generate_orders_data

    df = generate_orders_data(num_rows=1000, regions=["US-WEST", "US-EAST"], date="2024-01-15")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1000
    assert "order_id" in df.columns
    assert "region" in df.columns
    assert set(df['region'].unique()).issubset({"US-WEST", "US-EAST"})


def test_io_manager_path_generation():
    """Test IO manager path generation"""
    from dagster_pipeline.io_managers import S3ParquetIOManager

    io_manager = S3ParquetIOManager(bucket="test-bucket", prefix="test-prefix")

    # Create mock context
    context = Mock()
    context.asset_key = AssetKey(["Silver", "TaxID", "organizations"])

    path = io_manager._get_path(context)

    assert "Silver/TaxID/organizations.parquet" in path


def test_partitioned_io_manager_hive_path():
    """Test HIVE-style partition path generation"""
    from dagster_pipeline.io_managers import PartitionedS3ParquetIOManager
    from dagster import MultiPartitionKey

    io_manager = PartitionedS3ParquetIOManager(bucket="test-bucket", prefix="test-prefix")

    # Create mock context with multi-partition
    context = Mock()
    context.asset_key = AssetKey(["Silver", "TaxID", "partitioned_organizations"])

    # Mock multi-partition key
    mock_partition_key = Mock()
    mock_partition_key.keys_by_dimension = {
        "date": "2024-01-15",
        "state": "CA"
    }
    context.partition_key = mock_partition_key

    path = io_manager._get_partition_path(context)

    # Should contain HIVE-style partitioning
    assert "date=2024-01-15" in path
    assert "state=CA" in path


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
