# Data Dictionary

Complete data dictionary for all datasets in the Modern Data Stack project.

## Bronze Layer

### Weather Data

**Source**: OpenWeatherMap API  
**Format**: JSON  
**Update Frequency**: Daily  
**Location**: `s3://modern-data-stack-bronze/weather/`

| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| id | Integer | City ID from OpenWeatherMap | 5128581 |
| name | String | City name | "New York" |
| coord.lat | Double | Latitude | 40.7128 |
| coord.lon | Double | Longitude | -74.0060 |
| main.temp | Double | Temperature in Celsius | 15.5 |
| main.feels_like | Double | Feels like temperature | 14.2 |
| main.temp_min | Double | Minimum temperature | 12.0 |
| main.temp_max | Double | Maximum temperature | 18.0 |
| main.pressure | Integer | Atmospheric pressure (hPa) | 1013 |
| main.humidity | Integer | Humidity percentage | 65 |
| wind.speed | Double | Wind speed (meters/sec) | 5.2 |
| clouds.all | Integer | Cloudiness percentage | 40 |
| weather[0].main | String | Weather condition | "Clouds" |
| weather[0].description | String | Weather description | "scattered clouds" |
| dt | Integer | Unix timestamp | 1703174400 |
| sys.country | String | Country code | "US" |
| ingestion_timestamp | Timestamp | Data ingestion time | "2024-12-23T10:00:00Z" |

### Stock Data

**Source**: Alpha Vantage API  
**Format**: JSON  
**Update Frequency**: Daily  
**Location**: `s3://modern-data-stack-bronze/stocks/`

| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| symbol | String | Stock ticker symbol | "AAPL" |
| Meta Data.1. Information | String | Data description | "Daily Prices" |
| Meta Data.2. Symbol | String | Stock symbol | "AAPL" |
| Meta Data.3. Last Refreshed | Date | Last update date | "2024-12-22" |
| Time Series (Daily) | Object | Daily price data | {...} |
| ingestion_timestamp | Timestamp | Data ingestion time | "2024-12-23T10:00:00Z" |

### E-commerce Transactions

**Source**: CSV Files  
**Format**: CSV  
**Update Frequency**: Daily  
**Location**: `s3://modern-data-stack-bronze/transactions/`

| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| transaction_id | String | Unique transaction ID | "TXN001" |
| customer_id | String | Customer identifier | "CUST101" |
| product | String | Product name | "Laptop" |
| amount | Decimal | Transaction amount (USD) | 1299.99 |
| timestamp | Timestamp | Transaction timestamp | "2024-12-01 10:30:00" |
| category | String | Product category | "Electronics" |

### User Data

**Source**: PostgreSQL Database  
**Format**: JSON (dumped)  
**Update Frequency**: Daily  
**Location**: `s3://modern-data-stack-bronze/users/`

| Field Name | Data Type | Description | Example |
|------------|-----------|-------------|---------|
| user_id | Integer | Unique user ID | 1 |
| username | String | Username | "jdoe" |
| email | String | Email address | "john.doe@example.com" |
| full_name | String | User's full name | "John Doe" |
| country | String | User's country | "USA" |
| city | String | User's city | "New York" |
| registration_date | Timestamp | Account creation date | "2024-01-15" |
| last_login | Timestamp | Last login timestamp | "2024-12-20" |
| is_active | Boolean | Account status | true |

---

## Silver Layer

### Weather Cleaned

**Format**: Parquet  
**Location**: `s3://modern-data-stack-silver/weather_cleaned/`  
**Partitioned By**: country_code

| Field Name | Data Type | Description | Constraints |
|------------|-----------|-------------|-------------|
| city_id | Integer | City identifier | NOT NULL |
| city_name | String | City name | NOT NULL |
| latitude | Double | Geographic latitude | -90 to 90 |
| longitude | Double | Geographic longitude | -180 to 180 |
| temperature_celsius | Double | Temperature | NOT NULL |
| feels_like_celsius | Double | Perceived temperature | |
| temp_min_celsius | Double | Minimum temperature | |
| temp_max_celsius | Double | Maximum temperature | |
| pressure_hpa | Integer | Atmospheric pressure | > 0 |
| humidity_percent | Integer | Humidity | 0 to 100 |
| wind_speed_mps | Double | Wind speed | >= 0 |
| cloudiness_percent | Integer | Cloudiness | 0 to 100 |
| weather_condition | String | Weather category | |
| weather_description | String | Weather details | |
| measurement_timestamp | Integer | Unix timestamp | |
| country_code | String | ISO country code | 2 chars |
| ingestion_timestamp | Timestamp | Original ingestion time | |
| processing_timestamp | Timestamp | Processing time | |

---

## Gold Layer

### Aggregated Weather Metrics

**Format**: Parquet  
**Location**: `s3://modern-data-stack-gold/aggregated_weather_metrics/`

| Field Name | Data Type | Description | Calculation |
|------------|-----------|-------------|-------------|
| city_name | String | City name | Grouped by |
| country_code | String | Country code | Grouped by |
| avg_temperature | Double | Average temperature | AVG(temperature_celsius) |
| min_temperature | Double | Minimum temperature | MIN(temp_min_celsius) |
| max_temperature | Double | Maximum temperature | MAX(temp_max_celsius) |
| avg_humidity | Double | Average humidity | AVG(humidity_percent) |
| avg_pressure | Double | Average pressure | AVG(pressure_hpa) |
| avg_wind_speed | Double | Average wind speed | AVG(wind_speed_mps) |
| measurement_count | Long | Number of measurements | COUNT(*) |
| last_updated | Timestamp | Last processing time | MAX(processing_timestamp) |

### Weather Trends

**Format**: Parquet  
**Location**: `s3://modern-data-stack-gold/weather_trends/`

| Field Name | Data Type | Description |
|------------|-----------|-------------|
| city_name | String | City name |
| country_code | String | Country code |
| current_temp | Double | Current temperature |
| current_humidity | Integer | Current humidity |
| weather_condition | String | Current weather |
| measurement_timestamp | Integer | Measurement time |

### Dimension: City

**Format**: Parquet  
**Location**: `s3://modern-data-stack-gold/dim_city/`

| Field Name | Data Type | Description | Type |
|------------|-----------|-------------|------|
| dim_city_key | Long | Surrogate key | Primary Key |
| city_id | Integer | Natural key | Unique |
| city_name | String | City name | |
| country_code | String | Country | |
| latitude | Double | Latitude | |
| longitude | Double | Longitude | |

---

## Data Quality Metrics

### Null Rate Thresholds

| Layer | Field | Maximum Null % |
|-------|-------|----------------|
| Silver | city_id | 0% |
| Silver | temperature_celsius | 0% |
| Silver | country_code | 5% |
| Gold | All fields | 0% |

### Duplicate Detection

- **Bronze**: Allowed (raw data)
- **Silver**: Removed based on (city_id, measurement_timestamp)
- **Gold**: No duplicates allowed

### Value Constraints

| Field | Constraint |
|-------|------------|
| temperature_celsius | -50 to 60 |
| humidity_percent | 0 to 100 |
| pressure_hpa | 800 to 1100 |
| wind_speed_mps | 0 to 100 |

---

## Schema Evolution

### Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2024-12-23 | Initial schema |

### Backward Compatibility

All schema changes maintain backward compatibility with previous versions.

---

## Notes

- All timestamps are in UTC
- Currency amounts are in USD
- Geographic coordinates use WGS84 datum
- Temperature values are in Celsius
- Pressure values are in hectopascals (hPa)
- Wind speed is in meters per second
