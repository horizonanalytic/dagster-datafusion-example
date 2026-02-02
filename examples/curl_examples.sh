#!/usr/bin/env bash
################################################################################
# DataFusion Query Service - cURL Examples
#
# Demonstrates how to query Parquet data using raw HTTP requests.
# Supports both local development and UAT environments.
#
# Usage:
#   # Local development (default)
#   ./curl_examples.sh
#
#   # UAT environment
#   DATAFUSION_URL=http://datafusion.uat.local:8080 ./curl_examples.sh
#
#   # Run specific example
#   ./curl_examples.sh health_check
#   ./curl_examples.sh basic_query
################################################################################

set -euo pipefail

# Configuration
DATAFUSION_URL="${DATAFUSION_URL:-http://localhost:8080}"
DATAFUSION_ENV="${DATAFUSION_ENV:-uat}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
print_header() {
    echo -e "\n${BLUE}========================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}========================================${NC}\n"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${YELLOW}ℹ${NC} $1"
}

# Example 0: Health Check
health_check() {
    print_header "EXAMPLE 0: Health Check"

    echo "Request:"
    echo "  GET ${DATAFUSION_URL}/health"
    echo ""

    curl -s "${DATAFUSION_URL}/health" | jq '.'

    print_success "Service is healthy"
}

# Example 1: Basic SELECT Query
basic_query() {
    print_header "EXAMPLE 1: Basic SELECT Query"

    local sql="SELECT grouping_name, state, city, all_providers_primary FROM organizations LIMIT 10"

    echo "Request:"
    echo "  POST ${DATAFUSION_URL}/api/v1/query"
    echo ""
    echo "SQL:"
    echo "  ${sql}"
    echo ""

    curl -s -X POST "${DATAFUSION_URL}/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"sql\": \"${sql}\",
            \"environment\": \"${DATAFUSION_ENV}\"
        }" | jq '.'
}

# Example 2: Filtered Query (Partition Pruning)
filtered_query() {
    print_header "EXAMPLE 2: Filtered Query (Single State)"

    local sql="SELECT grouping_name, state, city, all_providers_primary FROM organizations WHERE state = 'CA' LIMIT 25"

    echo "Request:"
    echo "  POST ${DATAFUSION_URL}/api/v1/query"
    echo ""
    echo "SQL:"
    echo "  ${sql}"
    echo ""
    print_info "If partitioned by state, only CA partition is scanned!"
    echo ""

    curl -s -X POST "${DATAFUSION_URL}/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"sql\": \"${sql}\",
            \"environment\": \"${DATAFUSION_ENV}\"
        }" | jq '.'
}

# Example 3: Aggregation Query
aggregation_query() {
    print_header "EXAMPLE 3: Aggregation (Count by State)"

    local sql="SELECT state, COUNT(*) as organization_count FROM organizations WHERE state IS NOT NULL GROUP BY state ORDER BY organization_count DESC LIMIT 15"

    echo "Request:"
    echo "  POST ${DATAFUSION_URL}/api/v1/query"
    echo ""
    echo "SQL:"
    echo "  ${sql}"
    echo ""

    curl -s -X POST "${DATAFUSION_URL}/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"sql\": \"${sql}\",
            \"environment\": \"${DATAFUSION_ENV}\"
        }" | jq '.'
}

# Example 4: Top Organizations by Provider Count
top_organizations() {
    print_header "EXAMPLE 4: Top Organizations by Provider Count"

    local sql="SELECT grouping_name, state, all_providers_primary, all_providers_secondary, all_providers_primary + all_providers_secondary as total_providers FROM organizations WHERE grouping_name IS NOT NULL ORDER BY all_providers_primary DESC NULLS LAST LIMIT 25"

    echo "Request:"
    echo "  POST ${DATAFUSION_URL}/api/v1/query"
    echo ""
    echo "SQL (formatted):"
    echo "  SELECT grouping_name, state, all_providers_primary,"
    echo "         all_providers_secondary,"
    echo "         all_providers_primary + all_providers_secondary as total_providers"
    echo "  FROM organizations"
    echo "  WHERE grouping_name IS NOT NULL"
    echo "  ORDER BY all_providers_primary DESC NULLS LAST"
    echo "  LIMIT 25"
    echo ""

    curl -s -X POST "${DATAFUSION_URL}/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"sql\": \"${sql}\",
            \"environment\": \"${DATAFUSION_ENV}\"
        }" | jq '.'
}

# Example 5: E-commerce Orders Query
ecommerce_orders() {
    print_header "EXAMPLE 5: E-commerce Orders Query"

    local sql="SELECT order_id, customer_id, product_name, quantity, total_amount, region FROM orders WHERE region = 'US-WEST' ORDER BY total_amount DESC LIMIT 10"

    echo "Request:"
    echo "  POST ${DATAFUSION_URL}/api/v1/query"
    echo ""
    echo "SQL:"
    echo "  ${sql}"
    echo ""
    print_info "Note: E-commerce assets must be materialized in Dagster first"
    echo ""

    curl -s -X POST "${DATAFUSION_URL}/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"sql\": \"${sql}\",
            \"environment\": \"${DATAFUSION_ENV}\"
        }" | jq '.' || print_error "E-commerce table not found (materialize assets in Dagster)"
}

# Example 6: Sales Aggregation
sales_aggregation() {
    print_header "EXAMPLE 6: Sales Aggregation by Region"

    local sql="SELECT region, COUNT(*) as order_count, SUM(total_amount) as total_revenue, AVG(total_amount) as avg_order_value, MAX(total_amount) as largest_order FROM orders GROUP BY region ORDER BY total_revenue DESC"

    echo "Request:"
    echo "  POST ${DATAFUSION_URL}/api/v1/query"
    echo ""
    echo "SQL (formatted):"
    echo "  SELECT region,"
    echo "         COUNT(*) as order_count,"
    echo "         SUM(total_amount) as total_revenue,"
    echo "         AVG(total_amount) as avg_order_value,"
    echo "         MAX(total_amount) as largest_order"
    echo "  FROM orders"
    echo "  GROUP BY region"
    echo "  ORDER BY total_revenue DESC"
    echo ""
    print_info "Note: E-commerce assets must be materialized in Dagster first"
    echo ""

    curl -s -X POST "${DATAFUSION_URL}/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"sql\": \"${sql}\",
            \"environment\": \"${DATAFUSION_ENV}\"
        }" | jq '.' || print_error "E-commerce table not found (materialize assets in Dagster)"
}

# Example 7: Query with Custom Timeout
custom_timeout() {
    print_header "EXAMPLE 7: Query with Custom Timeout"

    local sql="SELECT COUNT(*) as total FROM organizations"

    echo "Request:"
    echo "  POST ${DATAFUSION_URL}/api/v1/query"
    echo ""
    echo "SQL:"
    echo "  ${sql}"
    echo ""
    print_info "Setting custom timeout to 5 seconds"
    echo ""

    curl -s -X POST "${DATAFUSION_URL}/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"sql\": \"${sql}\",
            \"environment\": \"${DATAFUSION_ENV}\",
            \"timeout_secs\": 5
        }" | jq '.'
}

# Example 8: Bypass Cache
bypass_cache() {
    print_header "EXAMPLE 8: Bypass Query Cache"

    local sql="SELECT COUNT(*) as total FROM organizations"

    echo "Request:"
    echo "  POST ${DATAFUSION_URL}/api/v1/query"
    echo ""
    echo "SQL:"
    echo "  ${sql}"
    echo ""
    print_info "Using bypass_cache=true to skip cache"
    echo ""

    curl -s -X POST "${DATAFUSION_URL}/api/v1/query" \
        -H "Content-Type: application/json" \
        -d "{
            \"sql\": \"${sql}\",
            \"environment\": \"${DATAFUSION_ENV}\",
            \"bypass_cache\": true
        }" | jq '.'
}

# Example 9: List All Tables
list_tables() {
    print_header "EXAMPLE 9: List All Tables"

    echo "Request:"
    echo "  GET ${DATAFUSION_URL}/api/v1/catalog/tables"
    echo ""

    curl -s "${DATAFUSION_URL}/api/v1/catalog/tables" | jq '.'
}

# Example 10: List Tables by Environment
list_tables_by_env() {
    print_header "EXAMPLE 10: List Tables by Environment"

    echo "Request:"
    echo "  GET ${DATAFUSION_URL}/api/v1/catalog/tables?environment=${DATAFUSION_ENV}"
    echo ""

    curl -s "${DATAFUSION_URL}/api/v1/catalog/tables?environment=${DATAFUSION_ENV}" | jq '.'
}

# Example 11: Get Table Schema
get_table_schema() {
    print_header "EXAMPLE 11: Get Table Schema"

    local table_name="${DATAFUSION_ENV}.organizations"

    echo "Request:"
    echo "  GET ${DATAFUSION_URL}/api/v1/catalog/tables/${table_name}/schema"
    echo ""

    curl -s "${DATAFUSION_URL}/api/v1/catalog/tables/${table_name}/schema" | jq '.'
}

# Example 12: Refresh Catalog
refresh_catalog() {
    print_header "EXAMPLE 12: Refresh Catalog from Dagster"

    echo "Request:"
    echo "  POST ${DATAFUSION_URL}/api/v1/catalog/refresh"
    echo ""
    print_info "This syncs metadata from Dagster to discover new tables"
    echo ""

    curl -s -X POST "${DATAFUSION_URL}/api/v1/catalog/refresh" | jq '.'
}

# Example 13: Get Cache Stats
cache_stats() {
    print_header "EXAMPLE 13: Cache Statistics"

    echo "Request:"
    echo "  GET ${DATAFUSION_URL}/api/v1/cache/stats"
    echo ""

    curl -s "${DATAFUSION_URL}/api/v1/cache/stats" | jq '.'
}

# Example 14: Clear Cache
clear_cache() {
    print_header "EXAMPLE 14: Clear Query Cache"

    echo "Request:"
    echo "  POST ${DATAFUSION_URL}/api/v1/cache/clear"
    echo ""
    print_info "Clearing entire cache"
    echo ""

    curl -s -X POST "${DATAFUSION_URL}/api/v1/cache/clear" \
        -H "Content-Type: application/json" \
        -d '{"all": true}' | jq '.'
}

# Example 15: Get Metrics
get_metrics() {
    print_header "EXAMPLE 15: Prometheus Metrics"

    echo "Request:"
    echo "  GET ${DATAFUSION_URL}/metrics"
    echo ""

    curl -s "${DATAFUSION_URL}/metrics"
}

# Example 16: List Environments
list_environments() {
    print_header "EXAMPLE 16: List All Environments"

    echo "Request:"
    echo "  GET ${DATAFUSION_URL}/api/v1/environments"
    echo ""

    curl -s "${DATAFUSION_URL}/api/v1/environments" | jq '.'
}

# Main execution
main() {
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}DataFusion Query Service - cURL Examples${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo "Configuration:"
    echo "  Service URL: ${DATAFUSION_URL}"
    echo "  Environment: ${DATAFUSION_ENV}"
    echo ""
    print_info "Make sure jq is installed for pretty JSON output"
    echo ""

    # Check if jq is installed
    if ! command -v jq &> /dev/null; then
        print_error "jq is not installed. Install it for better output formatting."
        print_info "macOS: brew install jq"
        print_info "Ubuntu: apt-get install jq"
        echo ""
    fi

    # If specific example is requested, run it
    if [ $# -gt 0 ]; then
        case "$1" in
            health_check|health)
                health_check
                ;;
            basic_query|basic)
                basic_query
                ;;
            filtered_query|filtered)
                filtered_query
                ;;
            aggregation_query|aggregation)
                aggregation_query
                ;;
            top_organizations|top)
                top_organizations
                ;;
            ecommerce_orders|ecommerce)
                ecommerce_orders
                ;;
            sales_aggregation|sales)
                sales_aggregation
                ;;
            custom_timeout|timeout)
                custom_timeout
                ;;
            bypass_cache|bypass)
                bypass_cache
                ;;
            list_tables|tables)
                list_tables
                ;;
            list_tables_by_env|env_tables)
                list_tables_by_env
                ;;
            get_table_schema|schema)
                get_table_schema
                ;;
            refresh_catalog|refresh)
                refresh_catalog
                ;;
            cache_stats|stats)
                cache_stats
                ;;
            clear_cache|clear)
                clear_cache
                ;;
            get_metrics|metrics)
                get_metrics
                ;;
            list_environments|environments)
                list_environments
                ;;
            *)
                echo "Unknown example: $1"
                echo ""
                echo "Available examples:"
                echo "  health_check, basic_query, filtered_query, aggregation_query,"
                echo "  top_organizations, ecommerce_orders, sales_aggregation,"
                echo "  custom_timeout, bypass_cache, list_tables, list_tables_by_env,"
                echo "  get_table_schema, refresh_catalog, cache_stats, clear_cache,"
                echo "  get_metrics, list_environments"
                exit 1
                ;;
        esac
        exit 0
    fi

    # Run all examples
    health_check
    list_environments
    list_tables
    list_tables_by_env
    get_table_schema
    basic_query
    filtered_query
    aggregation_query
    top_organizations
    custom_timeout
    cache_stats
    ecommerce_orders      # May fail if not materialized
    sales_aggregation     # May fail if not materialized

    echo ""
    print_success "All examples completed!"
    echo ""
    print_info "Run individual examples with: ./curl_examples.sh <example_name>"
}

# Run main if script is executed (not sourced)
if [ "${BASH_SOURCE[0]}" -eq "${0}" ]; then
    main "$@"
fi
