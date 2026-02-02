# Troubleshooting Guide

Common issues and solutions for DataFusion + Dagster example.

## Table of Contents

- [Docker Compose Issues](#docker-compose-issues)
- [LocalStack S3 Issues](#localstack-s3-issues)
- [Dagster Issues](#dagster-issues)
- [Asset Materialization Issues](#asset-materialization-issues)
- [IO Manager Issues](#io-manager-issues)
- [DataFusion Integration Issues](#datafusion-integration-issues)

---

## Docker Compose Issues

### Issue: Containers won't start

**Symptoms**:
```
ERROR: Failed to start container
```

**Solutions**:

1. **Check Docker is running**:
   ```bash
   docker info
   ```

2. **Check port conflicts**:
   ```bash
   # Ports used: 3000 (Dagster), 4566 (LocalStack), 5432 (Postgres)
   lsof -i :3000
   lsof -i :4566
   lsof -i :5432
   ```

3. **Clean and rebuild**:
   ```bash
   make clean
   make build
   make up
   ```

4. **Check logs**:
   ```bash
   docker-compose logs
   ```

### Issue: "no space left on device"

**Solution**:
```bash
# Clean Docker system
docker system prune -af --volumes

# Or clean specific components
docker volume prune -f
docker image prune -af
```

### Issue: Health checks failing

**Check service health**:
```bash
docker-compose ps

# Should show "healthy" for all services
```

**If unhealthy**:
```bash
# Check logs for specific service
docker-compose logs postgres
docker-compose logs localstack
docker-compose logs dagster_webserver
```

---

## LocalStack S3 Issues

### Issue: "NoSuchBucket" error

**Symptoms**:
```
botocore.exceptions.ClientError: An error occurred (NoSuchBucket)
```

**Solutions**:

1. **Run setup script**:
   ```bash
   ./setup_localstack.sh
   ```

2. **Manually create bucket**:
   ```bash
   awslocal s3 mb s3://dagster-parquet-data
   awslocal s3 ls  # Verify
   ```

3. **Check LocalStack is running**:
   ```bash
   docker-compose ps localstack
   curl http://localhost:4566/_localstack/health
   ```

### Issue: "Unable to locate credentials"

**Solution**:

LocalStack uses test credentials. Ensure these env vars are set:
```bash
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-2
```

### Issue: Can't see files in LocalStack

**Check files**:
```bash
# List all files
awslocal s3 ls s3://dagster-parquet-data/ --recursive

# Download a file to inspect
awslocal s3 cp s3://dagster-parquet-data/path/to/file.parquet ./test.parquet
```

**If empty**:
- Ensure assets have been materialized in Dagster UI
- Check asset logs for errors

### Issue: LocalStack data persists between restarts

**Clear data**:
```bash
# Remove LocalStack volumes
docker-compose down -v
rm -rf localstack_data/

# Restart and reinitialize
make up
```

---

## Dagster Issues

### Issue: Dagster UI not accessible

**Check webserver is running**:
```bash
docker-compose ps dagster_webserver

# Should show "Up" status
```

**If not running**:
```bash
docker-compose logs dagster_webserver

# Look for errors like:
# - Python import errors
# - Port binding issues
# - Database connection errors
```

**Restart webserver**:
```bash
docker-compose restart dagster_webserver
```

### Issue: "ModuleNotFoundError"

**Symptoms**:
```
ModuleNotFoundError: No module named 'dagster_pipeline'
```

**Solutions**:

1. **Check volume mounts** in `docker-compose.yml`:
   ```yaml
   volumes:
     - ./dagster_pipeline:/opt/dagster/app/dagster_pipeline
   ```

2. **Rebuild image**:
   ```bash
   docker-compose build dagster_webserver
   docker-compose up -d
   ```

3. **Check Python path** inside container:
   ```bash
   docker-compose exec dagster_webserver python -c "import dagster_pipeline; print('OK')"
   ```

### Issue: Dagster daemon not running assets

**Check daemon status**:
```bash
docker-compose logs dagster_daemon

# Look for:
# "Daemon started"
# "Checking for runs to launch"
```

**If not launching runs**:
```bash
# Restart daemon
docker-compose restart dagster_daemon

# Check Dagster UI -> Status -> Daemon Status
```

### Issue: Database connection errors

**Symptoms**:
```
psycopg2.OperationalError: could not connect to server
```

**Solutions**:

1. **Check Postgres is running**:
   ```bash
   docker-compose ps postgres
   ```

2. **Verify connection settings**:
   ```bash
   # In .env or docker-compose.yml
   DAGSTER_POSTGRES_HOST=postgres  # Container name
   DAGSTER_POSTGRES_PORT=5432
   DAGSTER_POSTGRES_USER=dagster
   DAGSTER_POSTGRES_PASSWORD=dagster
   DAGSTER_POSTGRES_DB=dagster
   ```

3. **Test connection manually**:
   ```bash
   docker-compose exec postgres psql -U dagster -d dagster -c "SELECT 1;"
   ```

---

## Asset Materialization Issues

### Issue: "Asset materialization failed"

**Steps to debug**:

1. **Check logs** in Dagster UI:
   - Click on failed run
   - View "Logs" tab
   - Look for Python stack traces

2. **Common errors**:

   **Import errors**:
   ```
   ModuleNotFoundError: No module named 'pandas'
   ```
   Solution: Rebuild Docker image with updated requirements.txt

   **Memory errors**:
   ```
   MemoryError: Unable to allocate array
   ```
   Solution: Reduce dataset size or increase Docker memory limit

   **S3 errors**:
   ```
   ClientError: An error occurred (NoSuchBucket)
   ```
   Solution: Run `./setup_localstack.sh`

### Issue: Assets taking too long to materialize

**Expected times**:
- `raw_organizations`: 1-2 seconds
- `partitioned_organizations`: 0.5-1 second per partition
- `daily_aggregates`: 0.5-1 second

**If slower**:
1. Check Docker resource allocation (CPU, memory)
2. Check I/O performance (disk speed)
3. Reduce dataset size for testing

### Issue: Partitions not generating correctly

**Check partition keys** in Dagster UI:
- Navigate to asset
- Click "Partitions" tab
- Verify expected partitions exist

**If missing**:
```python
# Verify partition definition in asset code
partitions_def=MultiPartitionsDefinition({
    "date": daily_partitions,
    "state": state_partitions,
})
```

### Issue: Metadata not appearing

**Check asset returns MaterializeResult**:
```python
@asset
def my_asset(context) -> MaterializeResult:
    df = generate_data()

    return MaterializeResult(
        metadata={
            "row_count": len(df),
            # ... other metadata
        }
    )
```

**View metadata** in Dagster UI:
- Click asset
- Click latest materialization
- View "Metadata" tab

---

## IO Manager Issues

### Issue: "TypeError: Expected pandas DataFrame"

**Symptoms**:
```
TypeError: Expected pandas DataFrame, got <class 'MaterializeResult'>
```

**Solutions**:

1. **For S3ParquetIOManager**:
   Asset should return DataFrame directly:
   ```python
   @asset(io_manager_key="parquet_io_manager")
   def my_asset(context) -> pd.DataFrame:
       df = generate_data()

       context.add_output_metadata({...})  # Add metadata this way

       return df  # Return DataFrame, not MaterializeResult
   ```

2. **For manual metadata control**:
   Return MaterializeResult but don't use IO manager:
   ```python
   @asset  # No io_manager_key
   def my_asset(context) -> MaterializeResult:
       df = generate_data()

       # Manually save Parquet
       df.to_parquet("path/to/file.parquet")

       return MaterializeResult(metadata={...})
   ```

### Issue: Parquet files not appearing in S3

**Check IO manager configuration**:
```python
# In repository.py
parquet_io_manager = S3ParquetIOManager(
    bucket="dagster-parquet-data",  # Correct bucket?
    prefix="datafusion_test",       # Correct prefix?
)
```

**Check asset configuration**:
```python
@asset(
    io_manager_key="parquet_io_manager",  # Matches resource key?
)
def my_asset(...) -> pd.DataFrame:
    ...
```

**Verify in LocalStack**:
```bash
awslocal s3 ls s3://dagster-parquet-data/datafusion_test/ --recursive
```

### Issue: HIVE partitioning not working

**Check PartitionedS3ParquetIOManager is used**:
```python
@asset(
    partitions_def=multi_partitions,
    io_manager_key="partitioned_parquet_io_manager",  # Not "parquet_io_manager"
)
def my_asset(...) -> pd.DataFrame:
    ...
```

**Verify partition path structure**:
```bash
# Should see: state=CA/date=2024-01-15/data.parquet
awslocal s3 ls s3://dagster-parquet-data/.../partitioned_organizations/ --recursive
```

**If wrong structure**:
- Check partition key dimensions in asset code
- Verify IO manager `_get_partition_path()` logic

---

## DataFusion Integration Issues

### Issue: DataFusion service can't find tables

**Check Dagster metadata DB**:
```bash
# Connect to Dagster Postgres
docker-compose exec postgres psql -U dagster -d dagster

# Query asset events
SELECT * FROM asset_event_tags WHERE asset_key LIKE '%organizations%';
```

**Verify S3 paths in metadata**:
- Metadata should contain S3 URIs
- URIs should match LocalStack or real S3 paths

### Issue: Partition pruning not working

**Verify HIVE path structure**:
```bash
# Should have key=value/ format
s3://bucket/table/state=CA/date=2024-01-15/data.parquet

# NOT:
s3://bucket/table/2024-01-15/CA/data.parquet
```

**Check DataFusion query**:
```sql
-- Should use partition column names
SELECT * FROM table WHERE state = 'CA' AND date = '2024-01-15';

-- NOT:
SELECT * FROM table WHERE partition_state = 'CA';
```

### Issue: Query performance slower than expected

**Without partitioning**:
- Expected: 3-4 seconds for 1.2M rows
- If slower: Check DataFusion configuration

**With partitioning**:
- Expected: <100ms for single partition
- If slower:
  - Verify partition pruning is working (check query plan)
  - Check partition file sizes (should be 10-100 KB each)
  - Consider file consolidation if too many small files

---

## General Debugging Tips

### Enable verbose logging

**Dagster**:
```python
# In asset code
context.log.setLevel(logging.DEBUG)
context.log.debug("Debug message")
```

**Docker Compose**:
```yaml
# In docker-compose.yml
environment:
  - DEBUG=1
  - LOG_LEVEL=DEBUG
```

### Inspect Docker containers

```bash
# Shell into container
docker-compose exec dagster_webserver bash

# Check Python environment
python -c "import pandas; print(pandas.__version__)"
python -c "import dagster; print(dagster.__version__)"

# Check filesystem
ls -la /opt/dagster/app/dagster_pipeline/

# Test AWS connection
aws --endpoint-url=http://localstack:4566 s3 ls
```

### Reset everything

```bash
# Nuclear option: start fresh
make clean
docker system prune -af --volumes
make build
make up
```

### Check resource usage

```bash
# Docker stats
docker stats

# If containers are OOM (out of memory):
# - Increase Docker memory limit in Docker Desktop
# - Or reduce dataset sizes in asset code
```

---

## Getting Help

If none of these solutions work:

1. **Check logs** thoroughly:
   ```bash
   docker-compose logs > full-logs.txt
   ```

2. **Create GitHub issue** with:
   - Full error message
   - Steps to reproduce
   - Environment details (OS, Docker version)
   - Relevant logs

3. **Dagster Community**:
   - [Dagster Slack](https://dagster.io/slack)
   - [GitHub Discussions](https://github.com/dagster-io/dagster/discussions)

4. **DataFusion Community**:
   - [DataFusion GitHub](https://github.com/apache/datafusion)
   - [DataFusion Discord](https://discord.gg/datafusion)
