-- SQL Views for Common Business Questions

USE modern_data_warehouse;

-- ========================================
-- ANALYTICAL VIEWS
-- ========================================

-- View 1: Current Weather Summary
CREATE OR REPLACE VIEW v_current_weather_summary AS
SELECT 
    wt.city_name,
    wt.country_code,
    wt.current_temp,
    wt.current_humidity,
    wt.weather_condition,
    awm.avg_temperature as historical_avg_temp,
    (wt.current_temp - awm.avg_temperature) as temp_deviation
FROM weather_trends wt
LEFT JOIN aggregated_weather_metrics awm 
    ON wt.city_name = awm.city_name;

-- View 2: City Climate Profile
CREATE OR REPLACE VIEW v_city_climate_profile AS
SELECT 
    dc.city_name,
    dc.country_code,
    dc.latitude,
    dc.longitude,
    awm.avg_temperature,
    awm.avg_humidity,
    awm.avg_pressure,
    CASE 
        WHEN awm.avg_temperature < 10 THEN 'Cold'
        WHEN awm.avg_temperature BETWEEN 10 AND 20 THEN 'Temperate'
        ELSE 'Warm'
    END as climate_classification
FROM dim_city dc
LEFT JOIN aggregated_weather_metrics awm 
    ON dc.city_name = awm.city_name;

-- View 3: Weather Extremes
CREATE OR REPLACE VIEW v_weather_extremes AS
SELECT 
    city_name,
    country_code,
    max_temperature as highest_temp,
    min_temperature as lowest_temp,
    (max_temperature - min_temperature) as temperature_range,
    avg_temperature
FROM aggregated_weather_metrics
WHERE (max_temperature - min_temperature) > 5;
