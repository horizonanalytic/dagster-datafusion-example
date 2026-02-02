-- ============================================================================
-- DataFusion Query Service - Sample SQL Queries
-- ============================================================================
--
-- This file contains example SQL queries demonstrating various patterns
-- for querying Parquet data through the DataFusion service.
--
-- Usage:
--   Copy queries into Python client or cURL examples to execute them.
--   All queries assume the 'uat' environment by default.
--
-- Table References:
--   - organizations: Healthcare organization master data
--   - partitioned_organizations: Organizations partitioned by state + date
--   - orders: E-commerce order data
--   - partitioned_orders: Orders partitioned by region + date
-- ============================================================================


-- ============================================================================
-- SECTION 1: BASIC QUERIES
-- ============================================================================

-- 1.1: Simple SELECT with LIMIT
-- Use Case: Quick data preview
SELECT *
FROM organizations
LIMIT 10;


-- 1.2: SELECT specific columns
-- Best Practice: Always specify columns instead of SELECT *
SELECT grouping_name, state, city, all_providers_primary
FROM organizations
LIMIT 25;


-- 1.3: Count all rows
-- Use Case: Get total record count
SELECT COUNT(*) as total_organizations
FROM organizations;


-- 1.4: Get distinct values
-- Use Case: Find unique states in dataset
SELECT DISTINCT state
FROM organizations
WHERE state IS NOT NULL
ORDER BY state;


-- ============================================================================
-- SECTION 2: FILTERED QUERIES (WHERE clauses)
-- ============================================================================

-- 2.1: Filter by single state
-- PERFORMANCE TIP: If partitioned by state, only state=CA is scanned!
SELECT grouping_name, state, city, all_providers_primary
FROM organizations
WHERE state = 'CA'
LIMIT 100;


-- 2.2: Filter by multiple states (IN clause)
SELECT grouping_name, state, city
FROM organizations
WHERE state IN ('CA', 'NY', 'TX', 'FL')
LIMIT 50;


-- 2.3: Filter with multiple conditions (AND)
SELECT grouping_name, state, city, all_providers_primary
FROM organizations
WHERE state = 'CA'
  AND all_providers_primary > 100
LIMIT 25;


-- 2.4: Filter with OR conditions
SELECT grouping_name, state, all_providers_primary
FROM organizations
WHERE state = 'CA'
   OR all_providers_primary > 500
LIMIT 50;


-- 2.5: NULL handling
-- Use Case: Find organizations without city information
SELECT grouping_name, state, city
FROM organizations
WHERE city IS NULL
LIMIT 25;


-- 2.6: String pattern matching (LIKE)
SELECT grouping_name, state, city
FROM organizations
WHERE grouping_name LIKE '%Hospital%'
LIMIT 50;


-- ============================================================================
-- SECTION 3: AGGREGATIONS
-- ============================================================================

-- 3.1: Count by state
-- Use Case: Organization distribution by state
SELECT state, COUNT(*) as organization_count
FROM organizations
WHERE state IS NOT NULL
GROUP BY state
ORDER BY organization_count DESC;


-- 3.2: Multiple aggregations
-- Use Case: Provider statistics by state
SELECT
    state,
    COUNT(*) as org_count,
    SUM(all_providers_primary) as total_primary_providers,
    AVG(all_providers_primary) as avg_primary_providers,
    MAX(all_providers_primary) as max_primary_providers
FROM organizations
WHERE state IS NOT NULL
  AND all_providers_primary IS NOT NULL
GROUP BY state
ORDER BY total_primary_providers DESC
LIMIT 20;


-- 3.3: HAVING clause (filter after aggregation)
-- Use Case: Find states with more than 10,000 organizations
SELECT state, COUNT(*) as org_count
FROM organizations
WHERE state IS NOT NULL
GROUP BY state
HAVING COUNT(*) > 10000
ORDER BY org_count DESC;


-- 3.4: Count distinct values
-- Use Case: How many unique cities per state?
SELECT state, COUNT(DISTINCT city) as unique_cities
FROM organizations
WHERE state IS NOT NULL AND city IS NOT NULL
GROUP BY state
ORDER BY unique_cities DESC
LIMIT 15;


-- ============================================================================
-- SECTION 4: SORTING AND RANKING
-- ============================================================================

-- 4.1: Top organizations by provider count
-- PERFORMANCE NOTE: ORDER BY can be expensive on large datasets
SELECT
    grouping_name,
    state,
    all_providers_primary,
    all_providers_secondary
FROM organizations
WHERE grouping_name IS NOT NULL
ORDER BY all_providers_primary DESC NULLS LAST
LIMIT 25;


-- 4.2: Sorting by multiple columns
SELECT grouping_name, state, city, all_providers_primary
FROM organizations
WHERE state IS NOT NULL
ORDER BY state ASC, all_providers_primary DESC NULLS LAST
LIMIT 50;


-- 4.3: Bottom N (smallest values)
SELECT grouping_name, state, all_providers_primary
FROM organizations
WHERE all_providers_primary IS NOT NULL
  AND all_providers_primary > 0
ORDER BY all_providers_primary ASC
LIMIT 25;


-- ============================================================================
-- SECTION 5: CALCULATED FIELDS
-- ============================================================================

-- 5.1: Simple arithmetic
-- Use Case: Calculate total providers
SELECT
    grouping_name,
    state,
    all_providers_primary,
    all_providers_secondary,
    all_providers_primary + all_providers_secondary as total_providers
FROM organizations
WHERE grouping_name IS NOT NULL
ORDER BY total_providers DESC NULLS LAST
LIMIT 25;


-- 5.2: CASE expressions (conditional logic)
-- Use Case: Categorize organizations by size
SELECT
    grouping_name,
    state,
    all_providers_primary,
    CASE
        WHEN all_providers_primary >= 500 THEN 'Large'
        WHEN all_providers_primary >= 100 THEN 'Medium'
        WHEN all_providers_primary >= 10 THEN 'Small'
        ELSE 'Micro'
    END as organization_size
FROM organizations
WHERE all_providers_primary IS NOT NULL
ORDER BY all_providers_primary DESC
LIMIT 50;


-- 5.3: String manipulation
-- Use Case: Standardize state names to uppercase
SELECT
    UPPER(state) as state_code,
    LOWER(grouping_name) as org_name_lower,
    grouping_name as org_name_original
FROM organizations
WHERE state IS NOT NULL
LIMIT 25;


-- ============================================================================
-- SECTION 6: PARTITION PRUNING EXAMPLES
-- ============================================================================
-- These queries work with partitioned tables (HIVE-style partitioning)
-- DataFusion automatically prunes partitions, scanning only relevant files

-- 6.1: Single partition query (state only)
-- PERFORMANCE: Only reads state=NY partition (10-100x faster!)
SELECT grouping_name, state, city, all_providers_primary
FROM partitioned_organizations
WHERE state = 'NY'
LIMIT 100;


-- 6.2: Multi-dimensional partition query (state + date)
-- PERFORMANCE: Only reads state=CA/date=2024-01-15 partition
SELECT grouping_name, state, city, all_providers_primary
FROM partitioned_organizations
WHERE state = 'CA'
  AND date = '2024-01-15'
LIMIT 100;


-- 6.3: Date range query (multiple date partitions)
-- PERFORMANCE: Reads only partitions between 2024-01-01 and 2024-01-31
SELECT state, date, COUNT(*) as daily_count
FROM partitioned_organizations
WHERE date >= '2024-01-01'
  AND date <= '2024-01-31'
GROUP BY state, date
ORDER BY state, date;


-- 6.4: Multiple states + date range
-- PERFORMANCE: Reads only specified state/date combinations
SELECT state, date, COUNT(*) as org_count
FROM partitioned_organizations
WHERE state IN ('CA', 'NY', 'TX')
  AND date >= '2024-01-01'
  AND date <= '2024-01-07'
GROUP BY state, date
ORDER BY state, date;


-- ============================================================================
-- SECTION 7: E-COMMERCE QUERIES
-- ============================================================================
-- NOTE: These require e-commerce assets to be materialized in Dagster

-- 7.1: Basic order query
SELECT order_id, customer_id, product_name, quantity, total_amount, region
FROM orders
LIMIT 25;


-- 7.2: Orders by region
SELECT order_id, customer_id, product_name, quantity, total_amount, region
FROM orders
WHERE region = 'US-WEST'
ORDER BY total_amount DESC
LIMIT 50;


-- 7.3: Sales aggregation by region
SELECT
    region,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_order_value,
    MAX(total_amount) as largest_order,
    MIN(total_amount) as smallest_order
FROM orders
GROUP BY region
ORDER BY total_revenue DESC;


-- 7.4: Top products by quantity sold
SELECT
    product_name,
    SUM(quantity) as total_quantity_sold,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue
FROM orders
GROUP BY product_name
ORDER BY total_quantity_sold DESC
LIMIT 25;


-- 7.5: High-value orders (filter after calculation)
SELECT
    order_id,
    customer_id,
    product_name,
    quantity,
    total_amount,
    region
FROM orders
WHERE total_amount > 1000
ORDER BY total_amount DESC
LIMIT 50;


-- 7.6: Partitioned orders query (region-specific)
-- PERFORMANCE: Only reads region=US-WEST partition
SELECT
    order_id,
    customer_id,
    product_name,
    quantity,
    total_amount,
    region,
    order_date
FROM partitioned_orders
WHERE region = 'US-WEST'
  AND order_date = '2024-01-15'
LIMIT 100;


-- ============================================================================
-- SECTION 8: ADVANCED QUERIES
-- ============================================================================

-- 8.1: Subquery example
-- Use Case: Find organizations in states with >50K total organizations
SELECT grouping_name, state, all_providers_primary
FROM organizations
WHERE state IN (
    SELECT state
    FROM organizations
    WHERE state IS NOT NULL
    GROUP BY state
    HAVING COUNT(*) > 50000
)
LIMIT 100;


-- 8.2: Common Table Expression (CTE)
-- Use Case: Calculate percentiles
WITH state_stats AS (
    SELECT
        state,
        COUNT(*) as org_count,
        SUM(all_providers_primary) as total_providers
    FROM organizations
    WHERE state IS NOT NULL
    GROUP BY state
)
SELECT
    state,
    org_count,
    total_providers,
    ROUND(org_count * 100.0 / SUM(org_count) OVER (), 2) as percent_of_total
FROM state_stats
ORDER BY org_count DESC
LIMIT 15;


-- 8.3: Multiple CTEs
-- Use Case: Join aggregated data from multiple sources
WITH healthcare_summary AS (
    SELECT
        state,
        COUNT(*) as org_count,
        SUM(all_providers_primary) as total_providers
    FROM organizations
    WHERE state IS NOT NULL
    GROUP BY state
),
order_summary AS (
    SELECT
        region,
        COUNT(*) as order_count,
        SUM(total_amount) as total_sales
    FROM orders
    GROUP BY region
)
SELECT
    h.state,
    h.org_count,
    h.total_providers,
    o.order_count,
    o.total_sales
FROM healthcare_summary h
LEFT JOIN order_summary o
ON h.state = o.region
LIMIT 25;


-- ============================================================================
-- SECTION 9: PERFORMANCE OPTIMIZATION TIPS
-- ============================================================================

-- 9.1: AVOID: SELECT * on large tables
-- BAD (slow, reads all columns):
-- SELECT * FROM organizations;

-- GOOD (fast, reads only needed columns):
SELECT grouping_name, state, city
FROM organizations
LIMIT 100;


-- 9.2: AVOID: ORDER BY without LIMIT on large tables
-- BAD (sorts entire dataset):
-- SELECT grouping_name, state FROM organizations ORDER BY grouping_name;

-- GOOD (only sorts top N):
SELECT grouping_name, state
FROM organizations
ORDER BY grouping_name
LIMIT 100;


-- 9.3: USE: Partition pruning whenever possible
-- BAD (full table scan):
-- SELECT * FROM partitioned_organizations;

-- GOOD (partition pruning):
SELECT *
FROM partitioned_organizations
WHERE state = 'CA' AND date = '2024-01-15';


-- 9.4: USE: Aggregations to reduce data before sorting
-- GOOD (aggregate first, then sort smaller result):
SELECT state, COUNT(*) as count
FROM organizations
GROUP BY state
ORDER BY count DESC;


-- ============================================================================
-- SECTION 10: TROUBLESHOOTING QUERIES
-- ============================================================================

-- 10.1: Check table size
SELECT COUNT(*) as total_rows
FROM organizations;


-- 10.2: Sample data for debugging
SELECT *
FROM organizations
WHERE state = 'CA'
LIMIT 5;


-- 10.3: Check for NULL values
SELECT
    COUNT(*) as total_rows,
    COUNT(state) as non_null_state,
    COUNT(city) as non_null_city,
    COUNT(grouping_name) as non_null_name
FROM organizations;


-- 10.4: Data quality check
SELECT
    state,
    COUNT(*) as total,
    COUNT(city) as with_city,
    COUNT(grouping_name) as with_name,
    ROUND(COUNT(city) * 100.0 / COUNT(*), 2) as city_coverage_pct
FROM organizations
WHERE state IS NOT NULL
GROUP BY state
ORDER BY state;


-- ============================================================================
-- END OF SAMPLE QUERIES
-- ============================================================================
--
-- For more examples, see:
--   - query_examples.py (Python client)
--   - curl_examples.sh (cURL commands)
--   - benchmark.py (Performance testing)
--
-- Documentation:
--   - https://arrow.apache.org/datafusion/user-guide/sql/
--   - https://arrow.apache.org/datafusion/user-guide/introduction.html
-- ============================================================================
