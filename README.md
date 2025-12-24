# 🏗️ Modern Data Stack - Mini Data Warehouse

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange.svg)](https://spark.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7-green.svg)](https://airflow.apache.org/)
[![AWS](https://img.shields.io/badge/AWS-S3%20|%20Lambda%20|%20Athena-yellow.svg)](https://aws.amazon.com/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)](https://www.docker.com/)

A complete **Data Engineering portfolio project** demonstrating modern data warehouse architecture with **Python**, **AWS**, **PySpark**, **SQL**, **Airflow**, and **GitHub**.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Technologies](#-technologies)
- [Architecture](#-architecture)
- [Features](#-features)
- [Project Structure](#-project-structure)
- [Setup Instructions](#-setup-instructions)
- [Usage](#-usage)
- [Data Pipeline](#-data-pipeline)
- [SQL Analytics](#-sql-analytics)
- [Cost Optimization](#-cost-optimization)
- [Future Enhancements](#-future-enhancements)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🎯 Overview

This project implements a **modern data stack** with a **medallion architecture** (Bronze/Silver/Gold layers) to ingest, transform, and analyze data from multiple sources. The pipeline is orchestrated with **Apache Airflow** and uses **PySpark** for distributed data processing.

**Purpose**: Portfolio project to demonstrate data engineering skills for job applications.

### Key Objectives

✅ **Data Ingestion**: Extract data from APIs, CSV files, and databases  
✅ **Data Transformation**: Clean and aggregate data using PySpark  
✅ **Data Quality**: Implement validation and quality checks  
✅ **Orchestration**: Automate pipelines with Airflow  
✅ **Analytics**: SQL queries on data warehouse (Athena)  
✅ **Cloud**: Deploy using AWS services (free tier)  

---

## 🛠️ Technologies

| Category | Technology | Purpose |
|----------|-----------|---------|
| **Languages** | Python 3.11+ | Core programming language |
| **Big Data Processing** | Apache Spark (PySpark) | Distributed data transformations |
| **Workflow Orchestration** | Apache Airflow 2.7+ | Pipeline scheduling & monitoring |
| **Cloud Storage** | AWS S3 | Data lake (Bronze/Silver/Gold) |
| **Serverless Compute** | AWS Lambda | Event-driven triggers |
| **Analytics Engine** | AWS Athena | SQL queries on S3 data |
| **Database** | PostgreSQL 15 | Source database & Airflow metadata |
| **Containerization** | Docker & Docker Compose | Local development environment |
| **Version Control** | Git & GitHub | Code versioning |
| **Data Formats** | JSON, CSV, Parquet | Various data formats |

---

## 🏗️ Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────┐
│                  DATA SOURCES                       │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │  Weather │  │  Stocks  │  │   CSV    │         │
│  │   API    │  │   API    │  │  Files   │         │
│  └──────────┘  └──────────┘  └──────────┘         │
│       │              │              │               │
│       └──────────────┴──────────────┘               │
│                      │                               │
└──────────────────────┼───────────────────────────────┘
                       │
                 ┌─────▼─────┐
                 │   Python  │
                 │ Ingestion │
                 │  Scripts  │
                 └─────┬─────┘
                       │
         ┌─────────────┴─────────────┐
         │      AWS S3 Data Lake      │
         │                            │
         │  ┌──────────────────────┐ │
         │  │   Bronze Layer       │ │
         │  │   (Raw Data)         │ │
         │  └──────────┬───────────┘ │
         │             │              │
         │  ┌──────────▼───────────┐ │
         │  │   Silver Layer       │ │
         │  │ (Cleaned Data)       │ │◄─── PySpark
         │  └──────────┬───────────┘ │     Transformations
         │             │              │
         │  ┌──────────▼───────────┐ │
         │  │   Gold Layer         │ │
         │  │ (Aggregated Data)    │ │
         │  └──────────────────────┘ │
         └──────────────┬─────────────┘
                        │
                 ┌──────▼──────┐
                 │   Athena    │
                 │ SQL Queries │
                 └─────────────┘
                        ▲
                        │
                 ┌──────┴──────┐
                 │   Airflow   │
                 │ Orchestrator│
                 └─────────────┘
```

### Medallion Architecture

**Bronze Layer (Raw)**: 
- Unprocessed data as ingested from sources
- JSON format with full data lineage
- Partitioned by date

**Silver Layer (Cleaned)**:
- Validated and standardized data
- Parquet format for efficiency
- Schema enforcement
- Duplicate removal

**Gold Layer (Analytics-Ready)**:
- Business-level aggregations
- Fact and dimension tables
- Optimized for queries
- Parquet with Snappy compression

---

## ✨ Features

### 1. **Multi-Source Data Ingestion**
- **Weather API**: OpenWeatherMap integration
- **Stock Market API**: Alpha Vantage integration
- **CSV Files**: E-commerce transaction data
- **PostgreSQL**: User dimension data

### 2. **PySpark Data Transformations**
- Bronze → Silver: Data cleaning and standardization
- Silver → Gold: Business aggregations
- Schema validation
- Data quality checks

### 3. **Apache Airflow Orchestration**
- **Daily Ingestion DAG**: Parallel data extraction
- **Transformation DAG**: Sequential processing
- **Full Pipeline DAG**: End-to-end orchestration
- Error handling and retries

### 4. **AWS Integration**
- S3 buckets for data lake layers
- Lambda functions for triggers
- Athena for SQL analytics
- IAM roles for security

### 5. **SQL Analytics**
- Pre-built analytical queries
- Athena table definitions
- Reusable views
- Performance optimization

### 6. **Data Quality Framework**
- Null rate validation
- Duplicate detection
- Schema validation
- Value range checks

---

## 📁 Project Structure

```
modern-data-stack/
├── README.md                          # This file
├── requirements.txt                   # Python dependencies
├── .gitignore                        # Git ignore rules
├── docker-compose.yml                # Docker services configuration
│
├── config/                           # Configuration files
│   ├── aws_config.yaml.example       # AWS configuration template
│   └── database_config.yaml.example  # Database & API configs
│
├── src/                              # Source code
│   ├── __init__.py
│   ├── ingestion/                    # Data ingestion modules
│   │   ├── weather_api_ingestion.py
│   │   ├── stock_api_ingestion.py
│   │   ├── csv_ingestion.py
│   │   └── database_ingestion.py
│   ├── transformation/               # PySpark transformations
│   │   ├── bronze_to_silver.py
│   │   ├── silver_to_gold.py
│   │   └── data_quality.py
│   ├── utils/                        # Utility functions
│   │   ├── logger.py
│   │   ├── config_loader.py
│   │   └── s3_utils.py
│   └── lambda/                       # AWS Lambda functions
│       └── trigger_ingestion.py
│
├── airflow/                          # Airflow configuration
│   ├── dags/                         # DAG definitions
│   │   ├── daily_ingestion_dag.py
│   │   ├── transformation_dag.py
│   │   └── full_pipeline_dag.py
│   └── plugins/                      # Custom Airflow plugins
│
├── sql/                              # SQL scripts
│   ├── init_db.sql                   # PostgreSQL initialization
│   ├── athena_table_creation.sql     # Athena DDL
│   ├── analytical_queries.sql        # Sample queries
│   └── views.sql                     # Athena views
│
├── data/                             # Sample data files
│   └── sample_ecommerce_transactions.csv
│
├── tests/                            # Unit tests
│   ├── test_ingestion.py
│   └── test_transformation.py
│
└── docs/                             # Documentation
    ├── setup_guide.md
    ├── aws_setup.md
    └── data_dictionary.md
```

---

## 🚀 Setup Instructions

### Prerequisites

- **Python 3.11+** installed
- **Docker & Docker Compose** installed
- **AWS Account** (free tier)
- **API Keys**: OpenWeatherMap, Alpha Vantage
- **Git** installed

### 1. Clone Repository

```bash
git clone https://github.com/fpuyog/modern-data-stack.git
cd modern-data-stack
```

### 2. Install Python Dependencies

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure AWS

```bash
# Install AWS CLI
pip install awscli

# Configure credentials
aws configure
```

Create S3 buckets:

```bash
aws s3 mb s3://modern-data-stack-bronze
aws s3 mb s3://modern-data-stack-silver
aws s3 mb s3://modern-data-stack-gold
```

### 4. Setup Configuration Files

```bash
# Copy example configs
cp config/aws_config.yaml.example config/aws_config.yaml
cp config/database_config.yaml.example config/database_config.yaml

# Edit with your credentials
# - OpenWeatherMap API key
# - Alpha Vantage API key
# - AWS region and bucket names
```

### 5. Start Docker Services

```bash
# Start Airflow and PostgreSQL
docker-compose up -d

# Check status
docker-compose ps
```

Access Airflow UI: http://localhost:8080
- Username: `admin`
- Password: `admin`

### 6. Initialize Database

```bash
# Connect to PostgreSQL
docker exec-it modern-data-stack-postgres psql -U airflow

# Run init script
\i /docker-entrypoint-initdb.d/init_db.sql
```

---

## 💻 Usage

### Running Individual Scripts

#### Ingest Weather Data
```bash
python src/ingestion/weather_api_ingestion.py
```

#### Ingest Stock Data
```bash
python src/ingestion/stock_api_ingestion.py
```

#### Run Bronze → Silver Transformation
```bash
python src/transformation/bronze_to_silver.py
```

#### Run Silver → Gold Transformation
```bash
python src/transformation/silver_to_gold.py
```

### Running Airflow DAGs

1. Open Airflow UI: http://localhost:8080
2. Enable the desired DAG:
   - `daily_data_ingestion`
   - `data_transformation_pipeline`
   - `full_etl_pipeline`
3. Trigger manually or wait for scheduled run

### Querying Data with Athena

1. Open AWS Athena Console
2. Run table creation script:
```sql
-- Copy from sql/athena_table_creation.sql
```

3. Run analytical queries:
```sql
-- Example: Average temperature by country
SELECT 
    country_code,
    ROUND(AVG(avg_temperature), 2) as avg_temp
FROM aggregated_weather_metrics
GROUP BY country_code
ORDER BY avg_temp DESC;
```

---

## 🔄 Data Pipeline

### Ingestion Pipeline (Daily at 1 AM)

1. **Weather API** → Extract current weather for configured cities
2. **Stock API** → Extract daily stock prices (with rate limiting)
3. **CSV Files** → Upload transaction data
4. **PostgreSQL** → Extract user dimension data
5. **Validation** → Verify all data landed in S3 Bronze layer

### Transformation Pipeline (Daily at 3 AM)

1. **Bronze → Silver**:
   - Read JSON files from S3 Bronze
   - Flatten nested structures
   - Type casting and validation
   - Remove duplicates
   - Write Parquet to S3 Silver

2. **Data Quality Checks**:
   - Null rate validation
   - Duplicate detection
   - Schema validation

3. **Silver → Gold**:
   - Create aggregations
   - Build fact and dimension tables
   - Calculate KPIs
   - Write optimized Parquet to S3 Gold

4. **Update Analytics**:
   - Refresh Athena table metadata
   - Update views

---

## 📊 SQL Analytics

### Sample Queries

#### 1. Cities with Extreme Temperatures
```sql
SELECT city_name, country_code,
       max_temperature - min_temperature as temp_range
FROM aggregated_weather_metrics
WHERE (max_temperature - min_temperature) > 10
ORDER BY temp_range DESC;
```

#### 2. Climate Zones
```sql
SELECT 
    CASE 
        WHEN avg_temperature < 10 THEN 'Cold'
        WHEN avg_temperature BETWEEN 10 AND 20 THEN 'Temperate'
        ELSE 'Warm'
    END as climate_zone,
    COUNT(*) as city_count
FROM aggregated_weather_metrics
GROUP BY 1;
```

More queries available in `sql/analytical_queries.sql`

---

## 💰 Cost Optimization

### AWS Free Tier Usage

| Service | Free Tier | Monthly Cost |
|---------|-----------|--------------|
| S3 | 5 GB storage | $0 |
| Lambda | 1M requests/month | $0 |
| Athena | 10 GB scanned (new users) | ~$0.01 |
| **Total** | | **$0 - $2** |

### Optimization Strategies

1. **Parquet Format**: 70% storage reduction vs JSON
2. **Partitioning**: Reduce Athena scan volume
3. **Local Processing**: Run PySpark locally (not EMR)
4. **Compression**: Snappy compression for Parquet
5. **Lifecycle Policies**: Archive old Bronze data to Glacier

---

## 🔮 Future Enhancements

- [ ] Add more data sources (Twitter API, Reddit API)
- [ ] Implement CDC (Change Data Capture)
- [ ] Add dbt for SQL transformations
- [ ] Create dashboard with QuickSight/Tableau
- [ ] Add streaming with Kinesis
- [ ] Implement data catalog with AWS Glue
- [ ] Add CI/CD with GitHub Actions
- [ ] Containerize PySpark jobs
- [ ] Add monitoring with CloudWatch
- [ ] Implement SLA monitoring

---

## 📝 Key Learnings

This project demonstrates:

✅ **Python**: Object-oriented programming, error handling, logging  
✅ **AWS**: S3, Lambda, Athena, IAM, boto3 SDK  
✅ **PySpark**: DataFrame API, transformations, optimization  
✅ **SQL**: Complex queries, aggregations, window functions  
✅ **Airflow**: DAG creation, task dependencies, scheduling  
✅ **GitHub**: Version control, documentation, best practices  
✅ **Data Engineering**: ETL patterns, data quality, architecture  

---

## 👨‍💻 Author

**Francisco Puyó G.**  
Data Engineer | Python | AWS | PySpark | SQL  

- GitHub: [@fpuyog](https://github.com/fpuyog)
- LinkedIn: [Francisco Puyó Gallardo](https://linkedin.com/in/fr-puyog)
- Email: fr.puyog@gmail.com

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- OpenWeatherMap for free weather API
- Alpha Vantage for free stock market API
- Apache Foundation for Airflow and Spark
- AWS for free tier services

---

## 📞 Contact

For questions or collaboration opportunities, please reach out via:
- GitHub Issues
- LinkedIn
- Email

**⭐ If this project helped you, please star it on GitHub!**
