-- Athena Table Creation Script
-- Create external tables in Athena pointing to S3 Gold layer

-- Create database
CREATE DATABASE IF NOT EXISTS modern_data_warehouse;

-- Use the database
USE modern_data_warehouse;

-- ========================================
-- GOLD LAYER TABLES
-- ========================================

-- Aggregated Weather Metrics Table
CREATE EXTERNAL TABLE IF NOT EXISTS aggregated_weather_metrics (
    city_name STRING,
    country_code STRING,
    avg_temperature DOUBLE,
    min_temperature DOUBLE,
    max_temperature DOUBLE,
    avg_humidity DOUBLE,
    avg_pressure DOUBLE,
    avg_wind_speed DOUBLE,
    measurement_count BIGINT,
    last_updated TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://modern-data-stack-gold/aggregated_weather_metrics/';

-- Weather Trends Table
CREATE EXTERNAL TABLE IF NOT EXISTS weather_trends (
    city_name STRING,
    country_code STRING,
    current_temp DOUBLE,
    current_humidity INT,
    weather_condition STRING,
    measurement_timestamp INT
)
STORED AS PARQUET
LOCATION 's3://modern-data-stack-gold/weather_trends/';

-- City Dimension Table
CREATE EXTERNAL TABLE IF NOT EXISTS dim_city (
    dim_city_key BIGINT,
    city_id INT,
    city_name STRING,
    country_code STRING,
    latitude DOUBLE,
    longitude DOUBLE
)
STORED AS PARQUET
LOCATION 's3://modern-data-stack-gold/dim_city/';

-- ========================================
-- OPTIMIZE TABLES
-- ========================================

-- Repair partitions if using partitioned tables
MSCK REPAIR TABLE aggregated_weather_metrics;
MSCK REPAIR TABLE weather_trends;
MSCK REPAIR TABLE dim_city;
