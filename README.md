# DataFusion + Dagster Example Integration

> **Complete working example** demonstrating how to use [Dagster](https://dagster.io) to generate Parquet files and query them with [DataFusion](https://arrow.apache.org/datafusion/) for blazing-fast analytics.

## What This Demonstrates

- **Parquet Generation**: Dagster assets that generate partitioned Parquet files
- **HIVE Partitioning**: Multi-dimensional partitioning (by date + dimension) for query pruning
- **S3 Integration**: LocalStack for local development, real S3 for production
- **Metadata Logging**: Row counts, file sizes, schemas tracked automatically
- **Two Domain Examples**: Healthcare (organizations) and E-commerce (orders)

## Quick Start (5 Minutes)

### Prerequisites

- Docker and Docker Compose
- Python 3.9+ (for local development)
- AWS credentials (for production S3, optional for local)

### 1. Clone and Configure

```bash
git clone https://github.com/yourusername/datafusion-dagster-example.git
cd datafusion-dagster-example

# Copy environment template
cp .env.example .env

# Edit .env if needed (defaults work for local development)
```

### 2. Start Services

```bash
# Start LocalStack (S3 emulation) + Dagster
docker-compose up -d

# Initialize LocalStack S3 buckets
./setup_localstack.sh

# Check services are running
docker-compose ps
```

### 3. Open Dagster UI

```bash
open http://localhost:3000
```

### 4. Materialize Assets

In the Dagster UI:
1. Navigate to **Assets** tab
2. Select `healthcare/raw_organizations` or `ecommerce/raw_orders`
3. Click **Materialize**
4. Watch metadata appear (row counts, file sizes, S3 paths)

### 5. Query with DataFusion

See the companion [datafusion_service](https://github.com/yourusername/datafusion_service) for querying these Parquet files at blazing speed.

## Architecture

```
┌─────────────────┐
│ Dagster Assets  │  Generate data (Python)
└────────┬────────┘
         │ Parquet files
         ▼
┌─────────────────┐
│   LocalStack    │  Local S3 emulation (or real S3)
│      (S3)       │
└────────┬────────┘
         │ s3://bucket/state=CA/date=2024-01-15/data.parquet
         ▼
┌─────────────────┐
│  DataFusion     │  Query engine (see datafusion_service)
│  Query Service  │
└─────────────────┘
```

## Project Structure

```
datafusion-dagster-example/
├── dagster_pipeline/           # Dagster assets and resources
│   ├── assets/
│   │   ├── healthcare.py       # Healthcare domain assets
│   │   └── ecommerce.py        # E-commerce domain assets
│   ├── io_managers.py          # Custom S3 Parquet IO Manager
│   └── repository.py           # Dagster definitions
├── examples/                   # Query examples
│   ├── query_healthcare.py     # Query healthcare data
│   └── query_ecommerce.py      # Query e-commerce data
├── docs/                       # Documentation
│   ├── ARCHITECTURE.md         # Detailed architecture
│   └── TROUBLESHOOTING.md      # Common issues
├── tests/                      # Tests
├── docker-compose.yml          # Local development stack
├── setup_localstack.sh         # S3 bucket initialization
├── .env.example                # Configuration template
└── README.md                   # This file
```

## Example Assets

### Healthcare Domain

**Organizations Data** (1.2M rows):
- `raw_organizations`: Extracts organization master data
- `partitioned_organizations`: Partitioned by `state` and `date` (HIVE style)
- `daily_aggregates`: Daily rollups of provider counts by state

**HIVE Partitioning Structure**:
```
s3://bucket/organizations/
  state=CA/
    date=2024-01-15/data.parquet
    date=2024-01-16/data.parquet
  state=NY/
    date=2024-01-15/data.parquet
```

**DataFusion Query Benefits**:
```sql
-- Only reads state=CA files (partition pruning!)
SELECT * FROM organizations WHERE state = 'CA'

-- Only reads 1 partition (10-100x faster!)
SELECT * FROM organizations
WHERE state = 'CA' AND date = '2024-01-15'
```

### E-commerce Domain

**Orders Data** (500K rows):
- `raw_orders`: Extracts order data
- `partitioned_orders`: Partitioned by `region` and `order_date`
- `daily_sales_aggregates`: Daily sales metrics by region

## Configuration

### Environment Variables

See `.env.example` for all options. Key variables:

```bash
# Environment mode (local, uat, prod)
ENVIRONMENT=local

# S3 Configuration
S3_BUCKET=dagster-parquet-data
S3_PREFIX=datafusion_test

# LocalStack (local development only)
AWS_ENDPOINT_URL=http://localstack:4566
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test

# Production S3 (for uat/prod environments)
# AWS_REGION=us-east-2
# AWS_ACCESS_KEY_ID=<your-key>
# AWS_SECRET_ACCESS_KEY=<your-secret>
```

## Development

### Install Python Dependencies

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### Run Dagster Locally (without Docker)

```bash
export DAGSTER_HOME=$(pwd)/dagster_home
dagster dev -f dagster_pipeline/repository.py
```

### Run Tests

```bash
pytest tests/ -v
```

## Production Deployment

See [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) for:
- Deploying Dagster to ECS/Kubernetes
- Using real S3 instead of LocalStack
- Multi-environment configuration (UAT, Prod)
- Integration with [datafusion_service](https://github.com/yourusername/datafusion_service)

## Performance

**Parquet File Sizes** (uncompressed Snappy):
- Healthcare: ~150MB (1.2M rows)
- E-commerce: ~50MB (500K rows)

**DataFusion Query Performance** (with HIVE partitioning):
- Single partition query: <100ms
- Full table scan: 3-4s (unoptimized), <500ms (with caching)

## Learn More

- [Dagster Documentation](https://docs.dagster.io)
- [DataFusion Documentation](https://arrow.apache.org/datafusion/)
- [Apache Parquet Format](https://parquet.apache.org/docs/)
- [DataFusion Service](https://github.com/yourusername/datafusion_service) - Query service using these Parquet files

## License

MIT License - see [LICENSE](LICENSE)

## Contributing

Contributions welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.
