"""
E-commerce Domain Assets

Demonstrates generating e-commerce order data in Parquet format.
Provides a generic, universally-understood example domain.
"""

from dagster import (
    asset,
    DailyPartitionsDefinition,
    StaticPartitionsDefinition,
    MultiPartitionsDefinition,
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    AssetKey,
)
import pandas as pd
import random
from datetime import datetime, timedelta
from typing import List


# ========== PARTITION DEFINITIONS ==========

# Daily partitions for order data
daily_partitions = DailyPartitionsDefinition(start_date="2024-01-01")

# Regional partitions (geographic regions)
region_partitions = StaticPartitionsDefinition([
    "US-WEST", "US-EAST", "US-CENTRAL", "US-SOUTH",
    "EU-WEST", "EU-EAST", "APAC", "LATAM"
])

# Multi-dimensional: region + date (HIVE partitioning)
multi_partitions = MultiPartitionsDefinition({
    "date": daily_partitions,
    "region": region_partitions,
})


# ========== HELPER FUNCTIONS ==========

def generate_product_name() -> str:
    """Generate realistic product names"""
    categories = ["Laptop", "Phone", "Tablet", "Watch", "Headphones", "Speaker", "Camera", "Monitor"]
    brands = ["TechCo", "DataMax", "CloudByte", "SmartDev", "FusionTech"]
    models = ["Pro", "Max", "Ultra", "Plus", "Elite", "Premium"]

    return f"{random.choice(brands)} {random.choice(categories)} {random.choice(models)}"


def generate_orders_data(
    num_rows: int = 10000,
    regions: List[str] = None,
    date: str = None
) -> pd.DataFrame:
    """
    Generate synthetic e-commerce order data

    Schema:
    - order_id: Unique order identifier
    - customer_id: Customer identifier
    - product_name: Product description
    - region: Geographic region
    - order_amount: Order total in USD
    - quantity: Number of items
    - order_date: Date of order
    - order_status: Order status
    """
    if regions is None:
        regions = ["US-WEST", "US-EAST", "US-CENTRAL", "EU-WEST"]

    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")

    # Generate order data
    data = {
        "order_id": [f"ORD-{i:08d}" for i in range(1, num_rows + 1)],
        "customer_id": [f"CUST-{random.randint(1, 50000):06d}" for _ in range(num_rows)],
        "product_name": [generate_product_name() for _ in range(num_rows)],
        "region": [random.choice(regions) for _ in range(num_rows)],
        "order_amount": [round(random.uniform(10.0, 2000.0), 2) for _ in range(num_rows)],
        "quantity": [random.randint(1, 10) for _ in range(num_rows)],
        "order_date": [date] * num_rows,
        "order_status": [
            random.choice(["completed", "pending", "shipped", "cancelled"])
            for _ in range(num_rows)
        ],
        "created_at": [datetime.now().isoformat()] * num_rows,
    }

    return pd.DataFrame(data)


# ========== ASSETS ==========

@asset(
    key=AssetKey(["Bronze", "Orders", "raw_orders"]),
    io_manager_key="parquet_io_manager",
    group_name="ecommerce",
    compute_kind="python",
)
def raw_orders(context: AssetExecutionContext) -> MaterializeResult:
    """
    Extract raw e-commerce order data

    Generates 50,000 synthetic order records with:
    - Order and customer IDs
    - Product names and quantities
    - Regional distribution
    - Order amounts and statuses

    Output: s3://bucket/Bronze/Orders/raw_orders.parquet

    Asset key structure: ["Bronze", "Orders", "raw_orders"]
    - Maps to schema: uat_bronze_orders (in datafusion_service)
    - Table: raw_orders
    """
    context.log.info("Generating raw orders data...")

    # Generate larger dataset for e-commerce (50K orders)
    df = generate_orders_data(
        num_rows=50000,
        regions=["US-WEST", "US-EAST", "US-CENTRAL", "US-SOUTH", "EU-WEST", "EU-EAST", "APAC", "LATAM"]
    )

    context.log.info(f"Generated {len(df):,} order records")

    # Calculate statistics
    region_counts = df['region'].value_counts()
    total_revenue = df['order_amount'].sum()
    avg_order_value = df['order_amount'].mean()
    status_counts = df['order_status'].value_counts()

    return MaterializeResult(
        asset_key=AssetKey(["Bronze", "Orders", "raw_orders"]),
        metadata={
            # Standard Dagster metadata
            "dagster/row_count": len(df),

            # Business metrics
            "total_orders": MetadataValue.int(len(df)),
            "total_revenue_usd": MetadataValue.float(round(total_revenue, 2)),
            "avg_order_value_usd": MetadataValue.float(round(avg_order_value, 2)),
            "unique_customers": MetadataValue.int(df['customer_id'].nunique()),
            "unique_regions": MetadataValue.int(len(region_counts)),

            # Regional distribution
            "region_distribution": MetadataValue.md(
                region_counts.to_markdown()
            ),

            # Order status breakdown
            "order_status_breakdown": MetadataValue.md(
                status_counts.to_markdown()
            ),

            # Data preview
            "preview": MetadataValue.md(
                df.head(10).to_markdown()
            ),
        }
    )


@asset(
    key=AssetKey(["Silver", "Orders", "partitioned_orders"]),
    partitions_def=multi_partitions,
    io_manager_key="partitioned_parquet_io_manager",
    group_name="ecommerce",
    compute_kind="python",
)
def partitioned_orders(
    context: AssetExecutionContext,
    raw_orders: pd.DataFrame,
) -> MaterializeResult:
    """
    Partition orders by region and date (HIVE-style)

    Multi-dimensional partitioning:
    - Partition by REGION (US-WEST, US-EAST, etc.)
    - Partition by DATE (2024-01-01, 2024-01-02, etc.)

    Output structure:
    s3://bucket/Silver/Orders/partitioned_orders/
      region=US-WEST/
        date=2024-01-15/
          data.parquet
      region=US-EAST/
        date=2024-01-15/
          data.parquet

    DataFusion Query Example:

    Query:  SELECT SUM(order_amount) FROM partitioned_orders
            WHERE region = 'US-WEST' AND order_date = '2024-01-15'
    Result: Only reads 1 partition (100x faster than full scan!)

    This is HIVE-style partitioning for partition pruning.
    """
    # Get partition keys
    partition_key = context.partition_key.keys_by_dimension
    partition_date = partition_key["date"]
    partition_region = partition_key["region"]

    context.log.info(f"Processing partition: region={partition_region}, date={partition_date}")

    # Filter raw data for this partition
    filtered_df = raw_orders[
        (raw_orders['region'] == partition_region) &
        (raw_orders['order_date'] == partition_date)
    ].copy()

    # If no data, generate synthetic data for this partition
    if len(filtered_df) == 0:
        context.log.warning(f"No data for region={partition_region}, date={partition_date}. Generating synthetic data.")
        filtered_df = generate_orders_data(
            num_rows=random.randint(100, 1000),
            regions=[partition_region],
            date=partition_date
        )

    # Add partition columns
    filtered_df['partition_date'] = partition_date
    filtered_df['partition_region'] = partition_region

    # Calculate partition metrics
    total_revenue = filtered_df['order_amount'].sum()
    avg_order_value = filtered_df['order_amount'].mean()

    context.log.info(
        f"Partitioned data: {len(filtered_df):,} orders, "
        f"${total_revenue:,.2f} revenue for {partition_region} on {partition_date}"
    )

    return MaterializeResult(
        metadata={
            "partition_date": partition_date,
            "partition_region": partition_region,
            "row_count": len(filtered_df),
            "total_revenue_usd": MetadataValue.float(round(total_revenue, 2)),
            "avg_order_value_usd": MetadataValue.float(round(avg_order_value, 2)),
            "processing_timestamp": datetime.now().isoformat(),
        }
    )


@asset(
    key=AssetKey(["Gold", "Analytics", "daily_sales_aggregates"]),
    partitions_def=daily_partitions,
    io_manager_key="parquet_io_manager",
    group_name="ecommerce",
    compute_kind="python",
)
def daily_sales_aggregates(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """
    Daily sales aggregations across all regions

    Time-series metrics for analytics dashboards:
    - Total orders by region
    - Revenue by region
    - Average order values
    - Order status breakdown

    Output: s3://bucket/Gold/Analytics/daily_sales_aggregates/{date}.parquet

    Use case: Track sales trends, identify top-performing regions
    """
    partition_date = context.partition_key

    context.log.info(f"Generating daily sales aggregates for {partition_date}")

    # Generate aggregate data
    regions = ["US-WEST", "US-EAST", "US-CENTRAL", "US-SOUTH", "EU-WEST", "EU-EAST", "APAC", "LATAM"]

    aggregates = {
        "date": [partition_date] * len(regions),
        "region": regions,
        "total_orders": [random.randint(500, 5000) for _ in regions],
        "total_revenue_usd": [round(random.uniform(10000, 500000), 2) for _ in regions],
        "avg_order_value_usd": [round(random.uniform(50, 200), 2) for _ in regions],
        "completed_orders": [random.randint(400, 4500) for _ in regions],
        "cancelled_orders": [random.randint(10, 200) for _ in regions],
    }

    df = pd.DataFrame(aggregates)

    # Calculate summary statistics
    total_orders = df['total_orders'].sum()
    total_revenue = df['total_revenue_usd'].sum()
    top_region = df.nlargest(1, 'total_revenue_usd').iloc[0]['region']
    avg_cancellation_rate = (df['cancelled_orders'].sum() / df['total_orders'].sum()) * 100

    context.log.info(
        f"Aggregated {total_orders:,} orders, "
        f"${total_revenue:,.2f} revenue across {len(regions)} regions"
    )

    return MaterializeResult(
        metadata={
            "date": partition_date,
            "total_orders": MetadataValue.int(int(total_orders)),
            "total_revenue_usd": MetadataValue.float(round(total_revenue, 2)),
            "top_revenue_region": top_region,
            "avg_cancellation_rate_pct": MetadataValue.float(round(avg_cancellation_rate, 2)),
            "regions_processed": MetadataValue.int(len(regions)),
            "aggregates_preview": MetadataValue.md(df.to_markdown()),
        }
    )


# ========== ASSET GROUP METADATA ==========

# These assets form the "ecommerce" domain in the Dagster UI
