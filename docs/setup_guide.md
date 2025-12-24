# Setup Guide

Complete setup instructions for the Modern Data Stack project.

## Quick Start

```bash
# 1. Clone repository
git clone https://github.com/YOUR_USERNAME/modern-data-stack.git
cd modern-data-stack

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure
cp config/aws_config.yaml.example config/aws_config.yaml
cp config/database_config.yaml.example config/database_config.yaml

# Edit config files with your credentials

# 5. Start services
docker-compose up -d

# 6. Run pipeline
# Access Airflow at http://localhost:8080
```

## Detailed Setup

### 1. System Requirements

- **OS**: Windows, macOS, or Linux
- **Python**: 3.11 or higher
- **Docker**: 20.10 or higher
- **RAM**: Minimum 8GB (16GB recommended)
- **Disk Space**: 10GB free space

### 2. API Keys

You'll need to sign up for free API keys:

#### OpenWeatherMap
1. Go to https://openweathermap.org/api
2. Sign up for free account
3. Generate API key
4. Add to `config/database_config.yaml`

#### Alpha Vantage
1. Go to https://www.alphavantage.co/support/#api-key
2. Get free API key
3. Add to `config/database_config.yaml`

### 3. AWS Setup

Follow the detailed guide in `docs/aws_setup.md` to:
- Configure AWS CLI
- Create S3 buckets
- Setup IAM roles
- Configure Athena

### 4. Docker Services

Start PostgreSQL and Airflow:

```bash
docker-compose up -d
```

Services started:
- PostgreSQL: `localhost:5432`
- Airflow Webserver: `localhost:8080`
- Airflow Scheduler: Background process

Access Airflow UI:
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 5. Initialize Database

```bash
# Connect to PostgreSQL
docker exec -it modern-data-stack-postgres psql -U airflow

# Database is auto-initialized with init_db.sql
# Verify tables:
\dt
```

### 6. Configuration Files

#### aws_config.yaml
```yaml
aws:
  region: us-east-1
  profile: default

s3:
  bronze_bucket: modern-data-stack-bronze
  silver_bucket: modern-data-stack-silver
  gold_bucket: modern-data-stack-gold
```

#### database_config.yaml
```yaml
apis:
  openweathermap:
    api_key: YOUR_OPENWEATHERMAP_KEY
    cities: ["New York", "London", "Tokyo"]
  
  alphavantage:
    api_key: YOUR_ALPHAVANTAGE_KEY
    symbols: ["AAPL", "GOOGL", "MSFT"]
```

### 7. Test Installation

Run individual components:

```bash
# Test S3 connection
python -c "from src.utils.s3_utils import S3Client; s3 = S3Client(); print('✓ S3 connected')"

# Test weather ingestion
python src/ingestion/weather_api_ingestion.py

# Test transformation
python src/transformation/bronze_to_silver.py
```

### 8. Run First Pipeline

1. Open Airflow: http://localhost:8080
2. Enable DAG: `daily_data_ingestion`
3. Click "Trigger DAG" button
4. Monitor progress in Graph View
5. Check S3 for uploaded data

## Verification

### Check S3 Data

```bash
# List Bronze layer data
aws s3 ls s3://modern-data-stack-bronze/weather/ --recursive

# List Silver layer data
aws s3 ls s3://modern-data-stack-silver/weather_cleaned/ --recursive
```

### QueryData with Athena

1. Open AWS Athena Console
2. Run table creation script from `sql/athena_table_creation.sql`
3. Run sample query:

```sql
SELECT * FROM aggregated_weather_metrics LIMIT 10;
```

## Troubleshooting

### Docker containers won't start

```bash
# Check logs
docker-compose logs

# Restart services
docker-compose down
docker-compose up -d
```

### Python import errors

```bash
# Ensure virtual environment is activated
source venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### AWS permissions issues

- Verify AWS credentials: `aws sts get-caller-identity`
- Check IAM policies
- Ensure S3 buckets exist

### Airflow DAGs not appearing

- Check DAG folder: `airflow/dags/`
- Verify no syntax errors in DAG files
- Refresh Airflow UI
- Check scheduler logs: `docker-compose logs airflow-scheduler`

## Development Workflow

1. **Make Changes**: Edit Python files in `src/`
2. **Test Locally**: Run scripts directly
3. **Update DAGs**: Modify Airflow DAGs
4. **Test in Airflow**: Trigger DAG manually
5. **Commit Changes**: `git add . && git commit -m "Description"`
6. **Push to GitHub**: `git push origin main`

## Next Steps

After successful setup:
1. ✅ Customize data sources
2. ✅ Add more transformations
3. ✅ Create dashboards
4. ✅ Implement CI/CD
5. ✅ Add monitoring

## Support

For issues:
- Check documentation in `docs/`
- Review error logs
- Open GitHub issue
- Contact via email

## Clean Up

To stop all services:

```bash
# Stop Docker containers
docker-compose down

# (Optional) Remove volumes
docker-compose down -v

# Deactivate virtual environment
deactivate
```
