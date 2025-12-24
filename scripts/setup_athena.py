"""
Quick Athena Setup
Creates database and tables in AWS Athena to query S3 data
"""

import boto3
import time


# Athena configuration
ATHENA_OUTPUT_BUCKET = "s3://modern-data-stack-athena-results/"
DATABASE_NAME = "modern_data_warehouse"
REGION = "us-east-1"

# SQL statements
CREATE_DATABASE = f"""
CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}
COMMENT 'Modern Data Stack - Data Warehouse'
LOCATION 's3://modern-data-stack-gold/'
"""

CREATE_WEATHER_TABLE = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.bronze_weather (
    coord STRUCT<lat:DOUBLE, lon:DOUBLE>,
    weather ARRAY<STRUCT<main:STRING, description:STRING>>,
    main STRUCT<
        temp:DOUBLE,
        feels_like:DOUBLE,
        temp_min:DOUBLE,
        temp_max:DOUBLE,
        pressure:INT,
        humidity:INT
    >,
    wind STRUCT<speed:DOUBLE>,
    clouds STRUCT<all:INT>,
    dt BIGINT,
    sys STRUCT<country:STRING>,
    id BIGINT,
    name STRING,
    ingestion_timestamp STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://modern-data-stack-bronze/weather/'
"""

def run_query(query, database=None):
    """Execute Athena query and wait for results"""
    athena = boto3.client('athena', region_name=REGION)
    
    params = {
        'QueryString': query,
        'ResultConfiguration': {'OutputLocation': ATHENA_OUTPUT_BUCKET}
    }
    
    if database:
        params['QueryExecutionContext'] = {'Database': database}
    
    # Start query
    response = athena.start_query_execution(**params)
    query_id = response['QueryExecutionId']
    
    print(f"  Query ID: {query_id}")
    
    # Wait for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        
        if state == 'SUCCEEDED':
            print(f"  ✓ Query completed successfully")
            return True
        elif state in ['FAILED', 'CANCELLED']:
            reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            print(f"  ✗ Query {state.lower()}: {reason}")
            return False
        
        time.sleep(1)


def main():
    print("=" * 70)
    print("  ATHENA QUICK SETUP")
    print("=" * 70)
    print()
    
    try:
        # Step 1: Create database
        print("[1/2] Creating database...")
        if run_query(CREATE_DATABASE):
            print(f"✓ Database '{DATABASE_NAME}' ready")
        else:
            print("✗ Failed to create database")
            return False
        
        print()
        
        # Step 2: Create table
        print("[2/2] Creating weather table...")
        if run_query(CREATE_WEATHER_TABLE, DATABASE_NAME):
            print(f"✓ Table '{DATABASE_NAME}.bronze_weather' created")
        else:
            print("✗ Failed to create table")
            return False
        
        print()
        print("=" * 70)
        print("✓ ATHENA SETUP COMPLETE!")
        print("=" * 70)
        print()
        print("You can now query your data:")
        print()
        print("  Example query:")
        print(f"    SELECT name, main.temp, main.humidity")
        print(f"    FROM {DATABASE_NAME}.bronze_weather")
        print(f"    LIMIT 10;")
        print()
        print("Run queries at:")
        print(f"  https://console.aws.amazon.com/athena/home?region={REGION}")
        print()
        
        return True
    
    except Exception as e:
        print(f"\n✗ Error: {e}")
        print("\nTroubleshooting:")
        print("  - Verify S3 bucket exists: modern-data-stack-athena-results")
        print("  - Check AWS permissions for Athena")
        print("  - Ensure data exists in S3 Bronze layer")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
