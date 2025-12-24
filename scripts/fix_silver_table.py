"""
Fix Athena Silver Table - Manual Creation
"""

import boto3
import time

ATHENA_OUTPUT = "s3://modern-data-stack-athena-results/"
DATABASE = "modern_data_warehouse"


def run_athena_query(query):
    """Run Athena query and wait for completion"""
    athena = boto3.client('athena', region_name='us-east-1')
    
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    
    query_id = response['QueryExecutionId']
    print(f"  Query ID: {query_id}")
    
    # Wait for completion
    for _ in range(30):
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status['QueryExecution']['Status']['State']
        
        if state == 'SUCCEEDED':
            print(f"  ✓ Success")
            return True
        elif state in ['FAILED', 'CANCELLED']:
            reason = status['QueryExecution']['Status'].get('StateChangeReason', 'Unknown')
            print(f"  ✗ Failed: {reason}")
            return False
        
        time.sleep(1)
    
    print("  ✗ Timeout")
    return False


def main():
    print("=" * 70)
    print("  FIXING ATHENA SILVER TABLE")
    print("=" * 70)
    print()
    
    # Drop table if exists
    print("[1/3] Dropping existing silver_weather table (if any)...")
    run_athena_query("DROP TABLE IF EXISTS silver_weather")
    
    # Create table
    print("\n[2/3] Creating silver_weather table...")
    create_table_sql = """
    CREATE EXTERNAL TABLE silver_weather (
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
        ingestion_timestamp STRING,
        processing_timestamp STRING
    )
    PARTITIONED BY (country_code STRING)
    STORED AS PARQUET
    LOCATION 's3://modern-data-stack-silver/weather_cleaned/'
    """
    
    if not run_athena_query(create_table_sql):
        print("\n✗ Failed to create table")
        return False
    
    # Repair partitions
    print("\n[3/3] Loading partitions...")
    if not run_athena_query("MSCK REPAIR TABLE silver_weather"):
        print("\n✗ Failed to repair partitions")
        return False
    
    # Test query
    print("\n[TEST] Running test query...")
    if run_athena_query("SELECT COUNT(*) FROM silver_weather"):
        print("\n✓ Table is working!")
    
    print("\n" + "=" * 70)
    print("✓ SILVER TABLE FIXED!")
    print("=" * 70)
    print()
    print("You can now query:")
    print("  SELECT * FROM modern_data_warehouse.silver_weather LIMIT 10;")
    print()
    print("Refresh the Athena UI to see the table.")
    print()


if __name__ == "__main__":
    main()
