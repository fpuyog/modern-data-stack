"""
Generate Synthetic Weather Data
Alternative to OpenWeatherMap API for testing
"""

import json
import random
from datetime import datetime
from src.utils.logger import setup_logger
from src.utils.config_loader import config_loader
from src.utils.s3_utils import S3Client

logger = setup_logger(__name__)


def generate_weather_data(city_name, country_code):
    """Generate synthetic weather data for a city"""
    # City coordinates (approximate)
    coordinates = {
        "New York": {"lat": 40.7128, "lon": -74.0060},
        "London": {"lat": 51.5074, "lon": -0.1278},
        "Tokyo": {"lat": 35.6762, "lon": 139.6503},
        "Buenos Aires": {"lat": -34.6037, "lon": -58.3816},
        "São Paulo": {"lat": -23.5505, "lon": -46.6333}
    }
    
    coords = coordinates.get(city_name, {"lat": 0, "lon": 0})
    
    # Generate realistic weather data
    data = {
        "coord": {
            "lon": coords["lon"],
            "lat": coords["lat"]
        },
        "weather": [
            {
                "id": random.choice([800, 801, 802, 803]),
                "main": random.choice(["Clear", "Clouds", "Rain", "Snow"]),
                "description": random.choice(["clear sky", "few clouds", "scattered clouds", "light rain"]),
                "icon": "01d"
            }
        ],
        "base": "stations",
        "main": {
            "temp": round(random.uniform(10, 30), 2),
            "feels_like": round(random.uniform(10, 30), 2),
            "temp_min": round(random.uniform(5, 25), 2),
            "temp_max": round(random.uniform(15, 35), 2),
            "pressure": random.randint(1000, 1020),
            "humidity": random.randint(40, 80)
        },
        "visibility": 10000,
        "wind": {
            "speed": round(random.uniform(0, 10), 2),
            "deg": random.randint(0, 360)
        },
        "clouds": {
            "all": random.randint(0, 100)
        },
        "dt": int(datetime.now().timestamp()),
        "sys": {
            "type": 1,
            "id": random.randint(1000, 9999),
            "country": country_code,
            "sunrise": int(datetime.now().timestamp()) - 21600,
            "sunset": int(datetime.now().timestamp()) + 21600
        },
        "timezone": 0,
        "id": random.randint(1000000, 9999999),
        "name": city_name,
        "cod": 200,
        "ingestion_timestamp": datetime.now().isoformat(),
        "city_search": city_name
    }
    
    return data


def main():
    """Generate synthetic weather data and upload to S3"""
    logger.info("Starting synthetic weather data generation")
    
    # Cities to generate data for
    cities = [
        ("New York", "US"),
        ("London", "GB"),
        ("Tokyo", "JP"),
        ("Buenos Aires", "AR"),
        ("São Paulo", "BR")
    ]
    
    # Generate data
    all_data = []
    for city, country in cities:
        logger.info(f"Generating data for {city}, {country}")
        data = generate_weather_data(city, country)
        all_data.append(data)
    
    # Upload to S3
    try:
        aws_config = config_loader.get_aws_config()
        s3_client = S3Client()
        
        bronze_bucket = aws_config['s3']['bronze_bucket']
        weather_prefix = aws_config['s3']['prefixes']['weather']
        
        now = datetime.now()
        filename = f"weather_{now.strftime('%Y%m%d_%H%M%S')}.json"
        s3_key = s3_client.get_partitioned_key(
            base_prefix=weather_prefix,
            date=now,
            filename=filename
        )
        
        success = s3_client.upload_data(
            data=all_data,
            bucket=bronze_bucket,
            s3_key=s3_key,
            format="json"
        )
        
        if success:
            logger.info(f"✓ Synthetic weather data uploaded to s3://{bronze_bucket}/{s3_key}")
            print(f"\n✓ Successfully generated and uploaded weather data for {len(all_data)} cities")
            print(f"  Location: s3://{bronze_bucket}/{s3_key}")
            return True
        else:
            logger.error("Failed to upload data to S3")
            return False
    
    except Exception as e:
        logger.error(f"Error: {e}")
        print(f"\n✗ Error: {e}")
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
