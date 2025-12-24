"""
Query Athena - Create tables for Silver and Gold layers
"""

import boto3
import time


ATHENA_OUTPUT_BUCKET = "s3://modern-data-stack-athena-results/"
DATABASE_NAME = "modern_data_warehouse"
REGION = "us-east-1"


# Create Silver table
CREATE_SILVER_TABLE = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.silver_weather (
    city_id INT,
    city_name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    temperature_celsius DOUBLE,
    feels_like_celsius DOUBLE,
    temp_min_celsius DOUBLE,
    temp_max_celsius DOUBLE,
    pressure_hpa INT,
    humidity_percent INT,
    wind_speed_mps DOUBLE,
    cloudiness_percent INT,
    weather_condition STRING,
    weather_description STRING,
    measurement_timestamp INT,
    country_code STRING,
    ingestion_timestamp STRING,
    processing_timestamp STRING
)
PARTITIONED BY (country_code STRING)
STORED AS PARQUET
LOCATION 's3://modern-data-stack-silver/weather_cleaned/'
"""

# Create Gold table  
CREATE_GOLD_TABLE = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE_NAME}.gold_weather_aggregations (
    city_name STRING,
    country_code STRING,
    avg_temperature DOUBLE,
    min_temperature DOUBLE,
    max_temperature DOUBLE,
    avg_humidity DOUBLE,
    avg_pressure DOUBLE,
    avg_wind_speed DOUBLE,
    measurement_count BIGINT,
    last_updated STRING
)
STORED AS PARQUET
LOCATION 's3://modern-data-stack-gold/aggregated_weather/'
"""


def run_query(query, database=None):
    """Execute Athena query"""
    athena = boto3.client('athena', region_name=REGION)
    
    params = {
        'QueryString': query,
        'ResultConfiguration': {'OutputLocation': ATHENA_OUTPUT_BUCKET}
    }
    
    if database:
        params['QueryExecutionContext'] = {'Database': database}
    
    response = athena.start_query_execution(**params)
    query_id = response['QueryExecutionId']
    
    # Wait for completion
    while True:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        
        if state == 'SUCCEEDED':
            return True
        elif state in ['FAILED', 'CANCELLED']:
            return False
        
        time.sleep(1)


def main():
    print("=" * 70)
    print("  ATHENA - CREATING SILVER & GOLD TABLES")
    print("=" * 70)
    print()
    
    # Create Silver table
    print("[1/4] Creating Silver table...")
    if run_query(CREATE_SILVER_TABLE, DATABASE_NAME):
        print("✓ Silver table created")
    
    # Repair partitions
    print("\n[2/4] Repairing Silver partitions...")
    if run_query(f"MSCK REPAIR TABLE {DATABASE_NAME}.silver_weather", DATABASE_NAME):
        print("✓ Partitions loaded")
    
    # Create Gold table
    print("\n[3/4] Creating Gold table...")
    if run_query(CREATE_GOLD_TABLE, DATABASE_NAME):
        print("✓ Gold table created")
    
    # Test query
    print("\n[4/4] Running test query...")
    test_query = f"SELECT * FROM {DATABASE_NAME}.gold_weather_aggregations LIMIT 5"
    if run_query(test_query, DATABASE_NAME):
        print("✓ Query successful")
    
    print("\n" + "=" * 70)
    print("✓ ATHENA SETUP COMPLETE!")
    print("=" * 70)
    print()
    print("Available tables:")
    print(f"  - {DATABASE_NAME}.bronze_weather")
    print(f"  - {DATABASE_NAME}.silver_weather")
    print(f"  - {DATABASE_NAME}.gold_weather_aggregations")
    print()
    print("Example queries:")
    print()
    print(f"  -- View aggregated metrics")
    print(f"  SELECT * FROM {DATABASE_NAME}.gold_weather_aggregations;")
    print()
    print(f"  -- View cleaned data")
    print(f"  SELECT city_name, temperature_celsius, humidity_percent")
    print(f"  FROM {DATABASE_NAME}.silver_weather LIMIT 10;")
    print()
    print("Query at: https://console.aws.amazon.com/athena/")
    print()


if __name__ == "__main__":
    main()
