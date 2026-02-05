#!/usr/bin/env python3
"""
DataFusion Query Service - Python Client Examples

Demonstrates how to query Parquet data using the DataFusion service.
Supports both local development (LocalStack) and UAT environments.

Usage:
    # Local development (default)
    python query_examples.py

    # UAT environment
    DATAFUSION_URL=http://datafusion.uat.local:8080 python query_examples.py

    # Custom environment
    DATAFUSION_URL=http://custom-host:8080 DATAFUSION_ENV=prod python query_examples.py
"""

import os
import sys
import json
import time
from typing import Dict, List, Any, Optional
import requests
from datetime import datetime


class DataFusionClient:
    """Simple client for DataFusion Query Service"""

    def __init__(
        self,
        base_url: Optional[str] = None,
        environment: Optional[str] = None,
        timeout: int = 30
    ):
        """
        Initialize DataFusion client.

        Args:
            base_url: Service URL (defaults to localhost:8080)
            environment: Dagster environment (defaults to 'uat')
            timeout: Default query timeout in seconds
        """
        self.base_url = base_url or os.getenv("DATAFUSION_URL", "http://localhost:8080")
        self.environment = environment or os.getenv("DATAFUSION_ENV", "uat")
        self.timeout = timeout
        self.session = requests.Session()

    def health_check(self) -> Dict[str, Any]:
        """Check service health"""
        response = self.session.get(f"{self.base_url}/health", timeout=5)
        response.raise_for_status()
        return response.json()

    def query(
        self,
        sql: str,
        environment: Optional[str] = None,
        timeout_secs: Optional[int] = None,
        bypass_cache: bool = False
    ) -> Dict[str, Any]:
        """
        Execute SQL query.

        Args:
            sql: SQL query string
            environment: Override default environment
            timeout_secs: Query timeout (1-300 seconds)
            bypass_cache: Skip cache and force execution

        Returns:
            Query response with columns, data, and metadata
        """
        payload = {
            "sql": sql,
            "environment": environment or self.environment,
        }

        if timeout_secs:
            payload["timeout_secs"] = timeout_secs

        if bypass_cache:
            payload["bypass_cache"] = True

        response = self.session.post(
            f"{self.base_url}/api/v1/query",
            json=payload,
            timeout=self.timeout
        )

        if not response.ok:
            try:
                error_data = response.json()
                raise Exception(
                    f"Query failed: {error_data.get('error', 'Unknown error')} - "
                    f"{error_data.get('message', response.text)}"
                )
            except json.JSONDecodeError:
                raise Exception(f"Query failed: {response.status_code} - {response.text}")

        return response.json()

    def list_tables(self, environment: Optional[str] = None) -> List[Dict[str, Any]]:
        """List all available tables"""
        params = {}
        if environment:
            params["environment"] = environment

        response = self.session.get(
            f"{self.base_url}/api/v1/catalog/tables",
            params=params,
            timeout=10
        )
        response.raise_for_status()
        return response.json()["tables"]

    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema for specific table"""
        response = self.session.get(
            f"{self.base_url}/api/v1/catalog/tables/{table_name}/schema",
            timeout=10
        )
        response.raise_for_status()
        return response.json()

    def refresh_catalog(self) -> Dict[str, Any]:
        """Trigger catalog refresh from Dagster"""
        response = self.session.post(
            f"{self.base_url}/api/v1/catalog/refresh",
            timeout=30
        )
        response.raise_for_status()
        return response.json()


def print_query_result(result: Dict[str, Any], limit: int = 10):
    """Pretty print query results"""
    print(f"\n{'='*80}")
    print(f"Environment: {result['environment']}")
    print(f"Rows Returned: {result['rows_returned']}")
    print(f"Execution Time: {result['execution_time_ms']}ms")
    print(f"Cache Hit: {result.get('cache_hit', False)}")
    print(f"{'='*80}\n")

    if result['rows_returned'] == 0:
        print("No rows returned")
        return

    # Print column headers
    columns = result['columns']
    print(" | ".join(columns))
    print("-" * 80)

    # Print data rows (limited)
    data = result['data'][:limit]
    for row in data:
        values = [str(row.get(col, 'NULL')) for col in columns]
        print(" | ".join(values))

    if result['rows_returned'] > limit:
        print(f"\n... ({result['rows_returned'] - limit} more rows)")


def example_1_basic_select(client: DataFusionClient):
    """Example 1: Basic SELECT query"""
    print("\n" + "="*80)
    print("EXAMPLE 1: Basic SELECT Query")
    print("="*80)

    sql = """
    SELECT grouping_name, state, city, all_providers_primary
    FROM organizations
    LIMIT 10
    """

    print(f"\nSQL:\n{sql.strip()}")
    result = client.query(sql)
    print_query_result(result)


def example_2_filtered_query(client: DataFusionClient):
    """Example 2: Filtered query with WHERE clause"""
    print("\n" + "="*80)
    print("EXAMPLE 2: Filtered Query (Single State)")
    print("="*80)

    sql = """
    SELECT grouping_name, state, city, all_providers_primary
    FROM organizations
    WHERE state = 'CA'
    LIMIT 25
    """

    print(f"\nSQL:\n{sql.strip()}")
    print("\nNote: If data is partitioned by state, only CA partition is scanned!")
    result = client.query(sql)
    print_query_result(result)


def example_3_aggregation(client: DataFusionClient):
    """Example 3: Aggregation with GROUP BY"""
    print("\n" + "="*80)
    print("EXAMPLE 3: Aggregation (Count by State)")
    print("="*80)

    sql = """
    SELECT state, COUNT(*) as organization_count
    FROM organizations
    WHERE state IS NOT NULL
    GROUP BY state
    ORDER BY organization_count DESC
    LIMIT 15
    """

    print(f"\nSQL:\n{sql.strip()}")
    result = client.query(sql)
    print_query_result(result, limit=15)


def example_4_partition_pruning(client: DataFusionClient):
    """Example 4: Partition pruning demonstration"""
    print("\n" + "="*80)
    print("EXAMPLE 4: Partition Pruning (Multi-dimensional)")
    print("="*80)

    # Query with multiple partition keys
    sql = """
    SELECT grouping_name, state, city, all_providers_primary
    FROM partitioned_organizations
    WHERE state = 'NY' AND date = '2024-01-15'
    LIMIT 10
    """

    print(f"\nSQL:\n{sql.strip()}")
    print("\nNote: Only scans state=NY/date=2024-01-15 partition!")
    print("This can be 10-100x faster than full table scan.\n")

    try:
        result = client.query(sql)
        print_query_result(result)
    except Exception as e:
        print(f"Note: This requires partitioned data to be materialized in Dagster first.")
        print(f"Error: {e}")


def example_5_ecommerce_orders(client: DataFusionClient):
    """Example 5: E-commerce orders query"""
    print("\n" + "="*80)
    print("EXAMPLE 5: E-commerce Orders Query")
    print("="*80)

    sql = """
    SELECT order_id, customer_id, product_name, quantity, total_amount, region
    FROM orders
    WHERE region = 'US-WEST'
    ORDER BY total_amount DESC
    LIMIT 10
    """

    print(f"\nSQL:\n{sql.strip()}")

    try:
        result = client.query(sql)
        print_query_result(result)
    except Exception as e:
        print(f"Note: E-commerce assets need to be materialized in Dagster first.")
        print(f"Error: {e}")


def example_6_aggregation_sales(client: DataFusionClient):
    """Example 6: Sales aggregation"""
    print("\n" + "="*80)
    print("EXAMPLE 6: Sales Aggregation by Region")
    print("="*80)

    sql = """
    SELECT
        region,
        COUNT(*) as order_count,
        SUM(total_amount) as total_revenue,
        AVG(total_amount) as avg_order_value,
        MAX(total_amount) as largest_order
    FROM orders
    GROUP BY region
    ORDER BY total_revenue DESC
    """

    print(f"\nSQL:\n{sql.strip()}")

    try:
        result = client.query(sql)
        print_query_result(result)
    except Exception as e:
        print(f"Note: E-commerce assets need to be materialized in Dagster first.")
        print(f"Error: {e}")


def example_7_top_organizations(client: DataFusionClient):
    """Example 7: Top organizations by provider count"""
    print("\n" + "="*80)
    print("EXAMPLE 7: Top Organizations by Provider Count")
    print("="*80)

    sql = """
    SELECT
        grouping_name,
        state,
        all_providers_primary,
        all_providers_secondary,
        all_providers_primary + all_providers_secondary as total_providers
    FROM organizations
    WHERE grouping_name IS NOT NULL
    ORDER BY all_providers_primary DESC NULLS LAST
    LIMIT 25
    """

    print(f"\nSQL:\n{sql.strip()}")
    result = client.query(sql)
    print_query_result(result, limit=25)


def example_8_cache_demonstration(client: DataFusionClient):
    """Example 8: Demonstrate query caching"""
    print("\n" + "="*80)
    print("EXAMPLE 8: Query Cache Demonstration")
    print("="*80)

    sql = "SELECT COUNT(*) as total FROM organizations"

    print(f"\nSQL: {sql}")
    print("\nFirst execution (cold - no cache):")
    result1 = client.query(sql)
    time1 = result1['execution_time_ms']
    print(f"  Execution time: {time1}ms (cache_hit: {result1.get('cache_hit', False)})")

    print("\nSecond execution (should hit cache):")
    result2 = client.query(sql)
    time2 = result2['execution_time_ms']
    print(f"  Execution time: {time2}ms (cache_hit: {result2.get('cache_hit', False)})")

    if result2.get('cache_hit'):
        speedup = time1 / time2 if time2 > 0 else float('inf')
        print(f"\n  Cache speedup: {speedup:.1f}x faster!")


def list_available_tables(client: DataFusionClient):
    """List all available tables in the catalog"""
    print("\n" + "="*80)
    print("AVAILABLE TABLES")
    print("="*80)

    try:
        tables = client.list_tables()
        print(f"\nFound {len(tables)} tables:\n")

        for table in tables:
            print(f"  • {table['name']}")
            print(f"    Base Name: {table.get('base_name', 'N/A')}")
            print(f"    Environment: {table.get('environment', 'N/A')}")
            print(f"    Rows: {table.get('row_count', 'Unknown'):,}")
            print(f"    Columns: {table.get('column_count', 'Unknown')}")
            print(f"    Last Updated: {table.get('last_updated', 'Unknown')}")
            print()

    except Exception as e:
        print(f"Failed to list tables: {e}")


def main():
    """Run all examples"""
    print("="*80)
    print("DataFusion Query Service - Python Client Examples")
    print("="*80)

    # Initialize client
    base_url = os.getenv("DATAFUSION_URL", "http://localhost:8080")
    environment = os.getenv("DATAFUSION_ENV", "uat")

    print(f"\nConfiguration:")
    print(f"  Service URL: {base_url}")
    print(f"  Environment: {environment}")

    client = DataFusionClient(base_url=base_url, environment=environment)

    # Check health
    try:
        health = client.health_check()
        print(f"  Service Status: {health.get('status', 'unknown')}")
        print(f"  Version: {health.get('version', 'unknown')}")
    except Exception as e:
        print(f"\nERROR: Cannot connect to DataFusion service at {base_url}")
        print(f"   Make sure the service is running: docker-compose up -d")
        print(f"   Error: {e}")
        sys.exit(1)

    # List available tables
    list_available_tables(client)

    # Run examples
    try:
        example_1_basic_select(client)
        example_2_filtered_query(client)
        example_3_aggregation(client)
        example_4_partition_pruning(client)
        example_5_ecommerce_orders(client)
        example_6_aggregation_sales(client)
        example_7_top_organizations(client)
        example_8_cache_demonstration(client)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\nError running examples: {e}")
        sys.exit(1)

    print("\n" + "="*80)
    print("All examples completed!")
    print("="*80)


if __name__ == "__main__":
    main()
