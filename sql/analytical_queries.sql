-- Analytical Queries for Business Intelligence
-- Sample queries to demonstrate SQL skills

USE modern_data_warehouse;

-- ========================================
-- WEATHER ANALYTICS
-- ========================================

-- 1. Average temperature by country
SELECT 
    country_code,
    ROUND(AVG(avg_temperature), 2) as avg_temp,
    COUNT(DISTINCT city_name) as num_cities
FROM aggregated_weather_metrics
GROUP BY country_code
ORDER BY avg_temp DESC;

-- 2. Cities with extreme temperatures
SELECT 
    city_name,
    country_code,
    avg_temperature,
    min_temperature,
    max_temperature,
    (max_temperature - min_temperature) as temp_range
FROM aggregated_weather_metrics
WHERE (max_temperature - min_temperature) > 10
ORDER BY temp_range DESC
LIMIT 10;

-- 3. Current weather conditions by city
SELECT 
    wt.city_name,
    wt.country_code,
    wt.current_temp,
    wt.current_humidity,
    wt.weather_condition,
    dc.latitude,
    dc.longitude
FROM weather_trends wt
JOIN dim_city dc ON wt.city_name = dc.city_name
ORDER BY wt.current_temp DESC;

-- 4. High humidity cities
SELECT 
    city_name,
    country_code,
    avg_humidity,
    avg_temperature
FROM aggregated_weather_metrics
WHERE avg_humidity > 70
ORDER BY avg_humidity DESC;

-- 5. Weather statistics summary
SELECT 
    COUNT(DISTINCT city_name) as total_cities,
    ROUND(AVG(avg_temperature), 2) as global_avg_temp,
    ROUND(MIN(min_temperature), 2) as coldest_temp,
    ROUND(MAX(max_temperature), 2) as hottest_temp,
    ROUND(AVG(avg_humidity), 2) as global_avg_humidity
FROM aggregated_weather_metrics;

-- 6. Cities by climate zones (simplified)
SELECT 
    CASE 
        WHEN avg_temperature < 10 THEN 'Cold'
        WHEN avg_temperature BETWEEN 10 AND 20 THEN 'Temperate'
        WHEN avg_temperature > 20 THEN 'Warm'
    END as climate_zone,
    COUNT(*) as city_count,
    ROUND(AVG(avg_temperature), 2) as avg_zone_temp
FROM aggregated_weather_metrics
GROUP BY 
    CASE 
        WHEN avg_temperature < 10 THEN 'Cold'
        WHEN avg_temperature BETWEEN 10 AND 20 THEN 'Temperate'
        WHEN avg_temperature > 20 THEN 'Warm'
    END
ORDER BY avg_zone_temp;

-- ========================================
-- DATA QUALITY QUERIES
-- ========================================

-- 7. Data freshness check
SELECT 
    MAX(last_updated) as last_data_update,
    DATE_DIFF('hour', MAX(last_updated), CURRENT_TIMESTAMP) as hours_since_update
FROM aggregated_weather_metrics;

-- 8. Missing data identification
SELECT 
    city_name,
    country_code,
    measurement_count
FROM aggregated_weather_metrics
WHERE measurement_count < 5
ORDER BY measurement_count;
