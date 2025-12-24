"""
Sample Athena Queries - All 3 Layers
Copy and paste these queries into Athena console
"""

print("=" * 70)
print("  SAMPLE ATHENA QUERIES")
print("  Database: modern_data_warehouse")
print("=" * 70)
print()

print("QUERY 1: Bronze Layer - Raw Weather Data")
print("-" * 70)
query1 = """
SELECT 
    name as city,
    main.temp as temperature,
    main.humidity as humidity,
    sys.country as country
FROM modern_data_warehouse.bronze_weather
LIMIT 10;
"""
print(query1)

print("\nQUERY 2: Silver Layer - Cleaned Data by Country")
print("-" * 70)
query2 = """
SELECT 
    city_name,
    temperature_celsius,
    humidity_percent,
    country_code
FROM modern_data_warehouse.silver_weather
ORDER BY temperature_celsius DESC
LIMIT 10;
"""
print(query2)

print("\nQUERY 3: Gold Layer - Aggregated Metrics")
print("-" * 70)
query3 = """
SELECT 
    city_name,
    country_code,
    avg_temperature,
    min_temperature,
    max_temperature,
    avg_humidity,
    measurement_count
FROM modern_data_warehouse.gold_weather_aggregations
ORDER BY avg_temperature DESC;
"""
print(query3)

print("\nQUERY 4: Compare All Layers")
print("-" * 70)
query4 = """
-- Bronze: Raw count
SELECT 'Bronze' as layer, COUNT(*) as record_count 
FROM modern_data_warehouse.bronze_weather
UNION ALL
-- Silver: Cleaned count
SELECT 'Silver' as layer, COUNT(*) as record_count 
FROM modern_data_warehouse.silver_weather
UNION ALL
-- Gold: Aggregated count
SELECT 'Gold' as layer, COUNT(*) as record_count 
FROM modern_data_warehouse.gold_weather_aggregations;
"""
print(query4)

print("\nQUERY 5: Weather Analysis - Temperature Range")
print("-" * 70)
query5 = """
SELECT 
    city_name,
    country_code,
    max_temperature - min_temperature as temp_range,
    avg_temperature,
    measurement_count
FROM modern_data_warehouse.gold_weather_aggregations
WHERE (max_temperature - min_temperature) > 5
ORDER BY temp_range DESC;
"""
print(query5)

print("\nQUERY 6: Partition Info (Silver Layer)")
print("-" * 70)
query6 = """
SELECT 
    country_code,
    COUNT(*) as records,
    AVG(temperature_celsius) as avg_temp,
    AVG(humidity_percent) as avg_humidity
FROM modern_data_warehouse.silver_weather
GROUP BY country_code
ORDER BY records DESC;
"""
print(query6)

print("\n" + "=" * 70)
print("Copy any query above and run it in Athena:")
print("https://console.aws.amazon.com/athena/")
print("=" * 70)
print()
