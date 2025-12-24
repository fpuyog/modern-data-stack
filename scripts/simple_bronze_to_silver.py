"""
Simplified Bronze to Silver Transformation
Works with the synthetic weather data we uploaded
"""

import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import boto3


def create_spark_session():
    """Create Spark session configured for S3"""
    spark = SparkSession.builder \
        .appName("Bronze to Silver - Weather Data") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def transform_weather_data(spark, bronze_bucket, silver_bucket):
    """Transform weather data from Bronze to Silver"""
    
    print("\n[1/4] Reading data from Bronze layer...")
    bronze_path = f"s3a://{bronze_bucket}/weather/"
    
    try:
        # Read JSON data
        df_bronze = spark.read.json(bronze_path)
        print(f"✓ Read {df_bronze.count()} records from Bronze")
        
        # Show sample
        print("\nSample Bronze data:")
        df_bronze.select("name", "main.temp", "sys.country").show(5, truncate=False)
    
    except Exception as e:
        print(f"✗ Error reading Bronze data: {e}")
        return False
    
    print("\n[2/4] Transforming data...")
    
    # Transform and flatten structure
    df_silver = df_bronze.select(
        F.col("id").cast(IntegerType()).alias("city_id"),
        F.col("name").alias("city_name"),
        F.col("coord.lat").cast(DoubleType()).alias("latitude"),
        F.col("coord.lon").cast(DoubleType()).alias("longitude"),
        F.col("main.temp").cast(DoubleType()).alias("temperature_celsius"),
        F.col("main.feels_like").cast(DoubleType()).alias("feels_like_celsius"),
        F.col("main.temp_min").cast(DoubleType()).alias("temp_min_celsius"),
        F.col("main.temp_max").cast(DoubleType()).alias("temp_max_celsius"),
        F.col("main.pressure").cast(IntegerType()).alias("pressure_hpa"),
        F.col("main.humidity").cast(IntegerType()).alias("humidity_percent"),
        F.col("wind.speed").cast(DoubleType()).alias("wind_speed_mps"),
        F.col("clouds.all").cast(IntegerType()).alias("cloudiness_percent"),
        F.col("weather")[0]["main"].alias("weather_condition"),
        F.col("weather")[0]["description"].alias("weather_description"),
        F.col("dt").cast(IntegerType()).alias("measurement_timestamp"),
        F.col("sys.country").alias("country_code"),
        F.col("ingestion_timestamp").cast(TimestampType()).alias("ingestion_timestamp")
    )
    
    # Remove duplicates
    df_silver = df_silver.dropDuplicates(["city_id", "measurement_timestamp"])
    
    # Remove nulls in critical fields
    df_silver = df_silver.filter(
        F.col("city_id").isNotNull() & 
        F.col("temperature_celsius").isNotNull()
    )
    
    # Add processing timestamp
    df_silver = df_silver.withColumn(
        "processing_timestamp",
        F.current_timestamp()
    )
    
    print(f"✓ Transformed {df_silver.count()} records")
    print("\nSample Silver data:")
    df_silver.select("city_name", "temperature_celsius", "humidity_percent", "country_code").show(5)
    
    print("\n[3/4] Writing to Silver layer...")
    
    # Write to Silver as Parquet
    silver_path = f"s3a://{silver_bucket}/weather_cleaned/"
    
    try:
        df_silver.write \
            .mode("overwrite") \
            .partitionBy("country_code") \
            .parquet(silver_path)
        
        print(f"✓ Data written to: {silver_path}")
    
    except Exception as e:
        print(f"✗ Error writing to Silver: {e}")
        return False
    
    print("\n[4/4] Verifying Silver data...")
    
    # Verify by reading back
    df_verify = spark.read.parquet(silver_path)
    record_count = df_verify.count()
    
    print(f"✓ Verified {record_count} records in Silver layer")
    print("\nPartitions created:")
    df_verify.groupBy("country_code").count().show()
    
    return True


def main():
    """Main execution function"""
    
    print("=" * 70)
    print("  BRONZE → SILVER TRANSFORMATION (PySpark)")
    print("=" * 70)
    
    # Configuration
    bronze_bucket = "modern-data-stack-bronze"
    silver_bucket = "modern-data-stack-silver"
    
    # Create Spark session
    print("\nInitializing Spark...")
    spark = create_spark_session()
    print("✓ Spark session created")
    
    # Run transformation
    try:
        success = transform_weather_data(spark, bronze_bucket, silver_bucket)
        
        if success:
            print("\n" + "=" * 70)
            print("✓ TRANSFORMATION COMPLETE!")
            print("=" * 70)
            print()
            print("Silver layer data is now available at:")
            print(f"  s3://{silver_bucket}/weather_cleaned/")
            print()
            print("Next step:")
            print("  Run Silver → Gold transformation")
            print()
            return True
        else:
            print("\n✗ Transformation failed")
            return False
    
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        spark.stop()
        print("Spark session stopped")


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
