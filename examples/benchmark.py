#!/usr/bin/env python3
"""
DataFusion Query Service - Performance Benchmark

Compares query performance with and without partition pruning to demonstrate
the benefits of HIVE-style partitioning.

Usage:
    # Local development
    python benchmark.py

    # UAT environment
    DATAFUSION_URL=http://datafusion.uat.local:8080 python benchmark.py

    # Specific benchmark
    python benchmark.py --benchmark partition_pruning
    python benchmark.py --benchmark aggregations
    python benchmark.py --benchmark all
"""

import os
import sys
import time
import json
import argparse
from typing import Dict, List, Any, Optional, Tuple
import requests
from dataclasses import dataclass
from datetime import datetime


@dataclass
class BenchmarkResult:
    """Results from a single benchmark query"""
    name: str
    sql: str
    execution_time_ms: int
    rows_returned: int
    cache_hit: bool
    error: Optional[str] = None


class DataFusionBenchmark:
    """Benchmark runner for DataFusion Query Service"""

    def __init__(
        self,
        base_url: Optional[str] = None,
        environment: Optional[str] = None,
        warmup_runs: int = 1,
        benchmark_runs: int = 3
    ):
        """
        Initialize benchmark runner.

        Args:
            base_url: Service URL (defaults to localhost:8080)
            environment: Dagster environment (defaults to 'uat')
            warmup_runs: Number of warmup runs before benchmarking
            benchmark_runs: Number of benchmark runs to average
        """
        self.base_url = base_url or os.getenv("DATAFUSION_URL", "http://localhost:8080")
        self.environment = environment or os.getenv("DATAFUSION_ENV", "uat")
        self.warmup_runs = warmup_runs
        self.benchmark_runs = benchmark_runs
        self.session = requests.Session()

    def query(
        self,
        sql: str,
        bypass_cache: bool = True,
        timeout_secs: int = 60
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Execute SQL query and return results.

        Args:
            sql: SQL query string
            bypass_cache: Force execution (skip cache)
            timeout_secs: Query timeout

        Returns:
            Tuple of (result_dict, error_message)
        """
        payload = {
            "sql": sql,
            "environment": self.environment,
            "bypass_cache": bypass_cache,
            "timeout_secs": timeout_secs
        }

        try:
            response = self.session.post(
                f"{self.base_url}/api/v1/query",
                json=payload,
                timeout=timeout_secs + 5
            )

            if not response.ok:
                try:
                    error_data = response.json()
                    error_msg = f"{error_data.get('error', 'Unknown')}: {error_data.get('message', response.text)}"
                except:
                    error_msg = f"HTTP {response.status_code}: {response.text}"
                return None, error_msg

            return response.json(), None

        except requests.exceptions.Timeout:
            return None, "Query timeout"
        except Exception as e:
            return None, str(e)

    def run_query_benchmark(
        self,
        name: str,
        sql: str,
        warmup: bool = True
    ) -> BenchmarkResult:
        """
        Run benchmark for a single query.

        Args:
            name: Benchmark name
            sql: SQL query
            warmup: Whether to run warmup queries

        Returns:
            BenchmarkResult with timing and metadata
        """
        # Warmup runs
        if warmup:
            for _ in range(self.warmup_runs):
                self.query(sql, bypass_cache=True)

        # Benchmark runs
        times = []
        rows = 0
        error = None

        for _ in range(self.benchmark_runs):
            result, err = self.query(sql, bypass_cache=True)

            if err:
                error = err
                break

            times.append(result['execution_time_ms'])
            rows = result['rows_returned']

        if error:
            return BenchmarkResult(
                name=name,
                sql=sql,
                execution_time_ms=0,
                rows_returned=0,
                cache_hit=False,
                error=error
            )

        # Average timing
        avg_time = sum(times) // len(times)

        return BenchmarkResult(
            name=name,
            sql=sql,
            execution_time_ms=avg_time,
            rows_returned=rows,
            cache_hit=False
        )

    def benchmark_partition_pruning(self) -> List[BenchmarkResult]:
        """
        Benchmark partition pruning performance.

        Compares:
        - Full table scan (no partition filter)
        - Single partition filter (state only)
        - Multi-dimensional filter (state + date)
        """
        print("\n" + "="*80)
        print("BENCHMARK: Partition Pruning")
        print("="*80)
        print("\nComparing query performance with and without partition filters...")

        results = []

        # 1. Full table scan (baseline)
        print("\n[1/3] Full table scan (no partition filter)...")
        results.append(self.run_query_benchmark(
            name="Full Table Scan",
            sql="SELECT COUNT(*) as count FROM organizations"
        ))

        # 2. Single partition (state only)
        print("[2/3] Single state partition filter...")
        results.append(self.run_query_benchmark(
            name="Single State Filter (CA)",
            sql="SELECT COUNT(*) as count FROM organizations WHERE state = 'CA'"
        ))

        # 3. Multi-dimensional partition (state + date)
        print("[3/3] Multi-dimensional partition filter...")
        results.append(self.run_query_benchmark(
            name="State + Date Filter",
            sql="""
            SELECT COUNT(*) as count
            FROM partitioned_organizations
            WHERE state = 'CA' AND date = '2024-01-15'
            """
        ))

        return results

    def benchmark_aggregations(self) -> List[BenchmarkResult]:
        """
        Benchmark aggregation queries.

        Tests:
        - Simple COUNT
        - GROUP BY with single column
        - GROUP BY with multiple aggregations
        """
        print("\n" + "="*80)
        print("BENCHMARK: Aggregations")
        print("="*80)
        print("\nTesting various aggregation patterns...")

        results = []

        # 1. Simple COUNT
        print("\n[1/4] Simple COUNT(*)...")
        results.append(self.run_query_benchmark(
            name="Simple COUNT",
            sql="SELECT COUNT(*) as total FROM organizations"
        ))

        # 2. GROUP BY state (single aggregation)
        print("[2/4] GROUP BY state...")
        results.append(self.run_query_benchmark(
            name="GROUP BY State",
            sql="""
            SELECT state, COUNT(*) as count
            FROM organizations
            WHERE state IS NOT NULL
            GROUP BY state
            """
        ))

        # 3. GROUP BY with multiple aggregations
        print("[3/4] GROUP BY with multiple aggregations...")
        results.append(self.run_query_benchmark(
            name="Multi-Aggregation by State",
            sql="""
            SELECT
                state,
                COUNT(*) as org_count,
                SUM(all_providers_primary) as total_providers,
                AVG(all_providers_primary) as avg_providers
            FROM organizations
            WHERE state IS NOT NULL
            GROUP BY state
            """
        ))

        # 4. GROUP BY with HAVING
        print("[4/4] GROUP BY with HAVING clause...")
        results.append(self.run_query_benchmark(
            name="GROUP BY with HAVING",
            sql="""
            SELECT state, COUNT(*) as count
            FROM organizations
            WHERE state IS NOT NULL
            GROUP BY state
            HAVING COUNT(*) > 10000
            """
        ))

        return results

    def benchmark_filters(self) -> List[BenchmarkResult]:
        """
        Benchmark filter operations.

        Tests:
        - No filter
        - Single column filter
        - Multiple column filters (AND)
        - OR filters
        """
        print("\n" + "="*80)
        print("BENCHMARK: Filters")
        print("="*80)
        print("\nTesting various filter patterns...")

        results = []

        # 1. No filter
        print("\n[1/5] No filter (full scan)...")
        results.append(self.run_query_benchmark(
            name="No Filter",
            sql="SELECT grouping_name, state FROM organizations LIMIT 1000"
        ))

        # 2. Single column filter
        print("[2/5] Single column filter...")
        results.append(self.run_query_benchmark(
            name="Single Column Filter",
            sql="""
            SELECT grouping_name, state, city
            FROM organizations
            WHERE state = 'CA'
            LIMIT 1000
            """
        ))

        # 3. Multiple columns (AND)
        print("[3/5] Multiple column filters (AND)...")
        results.append(self.run_query_benchmark(
            name="Multiple AND Filters",
            sql="""
            SELECT grouping_name, state, city, all_providers_primary
            FROM organizations
            WHERE state = 'CA'
              AND all_providers_primary > 100
            LIMIT 1000
            """
        ))

        # 4. OR filters
        print("[4/5] OR filters...")
        results.append(self.run_query_benchmark(
            name="OR Filters",
            sql="""
            SELECT grouping_name, state, all_providers_primary
            FROM organizations
            WHERE state = 'CA' OR state = 'NY' OR state = 'TX'
            LIMIT 1000
            """
        ))

        # 5. IN clause
        print("[5/5] IN clause...")
        results.append(self.run_query_benchmark(
            name="IN Clause Filter",
            sql="""
            SELECT grouping_name, state
            FROM organizations
            WHERE state IN ('CA', 'NY', 'TX', 'FL')
            LIMIT 1000
            """
        ))

        return results

    def benchmark_sorting(self) -> List[BenchmarkResult]:
        """
        Benchmark sorting operations.

        Tests:
        - LIMIT without ORDER BY
        - ORDER BY single column
        - ORDER BY multiple columns
        """
        print("\n" + "="*80)
        print("BENCHMARK: Sorting")
        print("="*80)
        print("\nTesting sorting performance...")

        results = []

        # 1. LIMIT without ORDER BY
        print("\n[1/3] LIMIT without ORDER BY...")
        results.append(self.run_query_benchmark(
            name="LIMIT Only (No Sort)",
            sql="SELECT grouping_name, state FROM organizations LIMIT 100"
        ))

        # 2. ORDER BY single column
        print("[2/3] ORDER BY single column...")
        results.append(self.run_query_benchmark(
            name="ORDER BY Single Column",
            sql="""
            SELECT grouping_name, state, all_providers_primary
            FROM organizations
            WHERE all_providers_primary IS NOT NULL
            ORDER BY all_providers_primary DESC
            LIMIT 100
            """
        ))

        # 3. ORDER BY multiple columns
        print("[3/3] ORDER BY multiple columns...")
        results.append(self.run_query_benchmark(
            name="ORDER BY Multiple Columns",
            sql="""
            SELECT grouping_name, state, city, all_providers_primary
            FROM organizations
            WHERE state IS NOT NULL
            ORDER BY state ASC, all_providers_primary DESC
            LIMIT 100
            """
        ))

        return results

    def benchmark_cache_impact(self) -> List[BenchmarkResult]:
        """
        Benchmark cache impact.

        Compares:
        - Cold query (bypass cache)
        - Warm query (use cache)
        """
        print("\n" + "="*80)
        print("BENCHMARK: Cache Impact")
        print("="*80)
        print("\nMeasuring cache performance...")

        results = []

        sql = "SELECT COUNT(*) as total FROM organizations"

        # 1. Cold query (bypass cache)
        print("\n[1/2] Cold query (bypass cache)...")
        result, error = self.query(sql, bypass_cache=True)
        if error:
            results.append(BenchmarkResult(
                name="Cold Query",
                sql=sql,
                execution_time_ms=0,
                rows_returned=0,
                cache_hit=False,
                error=error
            ))
        else:
            results.append(BenchmarkResult(
                name="Cold Query (No Cache)",
                sql=sql,
                execution_time_ms=result['execution_time_ms'],
                rows_returned=result['rows_returned'],
                cache_hit=False
            ))

        # 2. Warm query (use cache)
        print("[2/2] Warm query (use cache)...")
        result, error = self.query(sql, bypass_cache=False)
        if error:
            results.append(BenchmarkResult(
                name="Warm Query",
                sql=sql,
                execution_time_ms=0,
                rows_returned=0,
                cache_hit=False,
                error=error
            ))
        else:
            results.append(BenchmarkResult(
                name="Warm Query (Cache Hit)",
                sql=sql,
                execution_time_ms=result['execution_time_ms'],
                rows_returned=result['rows_returned'],
                cache_hit=result.get('cache_hit', False)
            ))

        return results


def print_results(results: List[BenchmarkResult]):
    """Pretty print benchmark results"""
    print("\n" + "="*80)
    print("RESULTS")
    print("="*80)

    # Calculate max width for alignment
    max_name_len = max(len(r.name) for r in results)

    for result in results:
        if result.error:
            print(f"\n❌ {result.name:<{max_name_len}} - ERROR: {result.error}")
        else:
            print(f"\n✓ {result.name:<{max_name_len}}")
            print(f"  Execution Time: {result.execution_time_ms:>6}ms")
            print(f"  Rows Returned:  {result.rows_returned:>6}")
            if result.cache_hit:
                print(f"  Cache Hit:      YES")


def print_comparison(results: List[BenchmarkResult]):
    """Print performance comparison"""
    if len(results) < 2:
        return

    baseline = results[0]
    if baseline.error or baseline.execution_time_ms == 0:
        return

    print("\n" + "="*80)
    print("PERFORMANCE COMPARISON")
    print("="*80)
    print(f"\nBaseline: {baseline.name} ({baseline.execution_time_ms}ms)")

    for result in results[1:]:
        if result.error or result.execution_time_ms == 0:
            continue

        speedup = baseline.execution_time_ms / result.execution_time_ms
        time_saved = baseline.execution_time_ms - result.execution_time_ms

        if speedup > 1:
            print(f"\n✓ {result.name}:")
            print(f"  {speedup:.2f}x FASTER ({result.execution_time_ms}ms, saved {time_saved}ms)")
        elif speedup < 1:
            print(f"\n⚠ {result.name}:")
            print(f"  {1/speedup:.2f}x SLOWER ({result.execution_time_ms}ms, +{-time_saved}ms)")
        else:
            print(f"\n≈ {result.name}:")
            print(f"  Similar performance ({result.execution_time_ms}ms)")


def main():
    """Run benchmarks"""
    parser = argparse.ArgumentParser(
        description="DataFusion Query Service - Performance Benchmark"
    )
    parser.add_argument(
        "--benchmark",
        choices=["partition_pruning", "aggregations", "filters", "sorting", "cache", "all"],
        default="all",
        help="Which benchmark to run (default: all)"
    )
    parser.add_argument(
        "--warmup-runs",
        type=int,
        default=1,
        help="Number of warmup runs (default: 1)"
    )
    parser.add_argument(
        "--benchmark-runs",
        type=int,
        default=3,
        help="Number of benchmark runs to average (default: 3)"
    )

    args = parser.parse_args()

    print("="*80)
    print("DataFusion Query Service - Performance Benchmark")
    print("="*80)

    base_url = os.getenv("DATAFUSION_URL", "http://localhost:8080")
    environment = os.getenv("DATAFUSION_ENV", "uat")

    print(f"\nConfiguration:")
    print(f"  Service URL: {base_url}")
    print(f"  Environment: {environment}")
    print(f"  Warmup Runs: {args.warmup_runs}")
    print(f"  Benchmark Runs: {args.benchmark_runs}")

    # Check service health
    try:
        response = requests.get(f"{base_url}/health", timeout=5)
        response.raise_for_status()
        health = response.json()
        print(f"  Service Status: {health.get('status', 'unknown')}")
    except Exception as e:
        print(f"\n❌ ERROR: Cannot connect to DataFusion service at {base_url}")
        print(f"   Make sure the service is running: docker-compose up -d")
        print(f"   Error: {e}")
        sys.exit(1)

    # Initialize benchmark runner
    benchmark = DataFusionBenchmark(
        base_url=base_url,
        environment=environment,
        warmup_runs=args.warmup_runs,
        benchmark_runs=args.benchmark_runs
    )

    # Run benchmarks
    all_results = []

    if args.benchmark in ("partition_pruning", "all"):
        results = benchmark.benchmark_partition_pruning()
        all_results.extend(results)
        print_results(results)
        print_comparison(results)

    if args.benchmark in ("aggregations", "all"):
        results = benchmark.benchmark_aggregations()
        all_results.extend(results)
        print_results(results)

    if args.benchmark in ("filters", "all"):
        results = benchmark.benchmark_filters()
        all_results.extend(results)
        print_results(results)

    if args.benchmark in ("sorting", "all"):
        results = benchmark.benchmark_sorting()
        all_results.extend(results)
        print_results(results)

    if args.benchmark in ("cache", "all"):
        results = benchmark.benchmark_cache_impact()
        all_results.extend(results)
        print_results(results)
        print_comparison(results)

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)

    successful = [r for r in all_results if not r.error]
    failed = [r for r in all_results if r.error]

    print(f"\nTotal Benchmarks: {len(all_results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")

    if successful:
        avg_time = sum(r.execution_time_ms for r in successful) // len(successful)
        min_time = min(r.execution_time_ms for r in successful)
        max_time = max(r.execution_time_ms for r in successful)

        print(f"\nExecution Times:")
        print(f"  Average: {avg_time}ms")
        print(f"  Min: {min_time}ms ({[r.name for r in successful if r.execution_time_ms == min_time][0]})")
        print(f"  Max: {max_time}ms ({[r.name for r in successful if r.execution_time_ms == max_time][0]})")

    if failed:
        print(f"\n⚠ Failed Benchmarks:")
        for result in failed:
            print(f"  • {result.name}: {result.error}")

    print("\n" + "="*80)
    print("✅ Benchmark completed!")
    print("="*80)


if __name__ == "__main__":
    main()
