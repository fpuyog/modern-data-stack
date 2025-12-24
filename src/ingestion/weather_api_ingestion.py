"""
Weather API Data Ingestion
Extract weather data from OpenWeatherMap API and store in S3 Bronze layer
"""

import requests
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from src.utils.logger import setup_logger
from src.utils.config_loader import config_loader
from src.utils.s3_utils import S3Client

logger = setup_logger(__name__)


class WeatherAPIIngestion:
    """Extract weather data from OpenWeatherMap API"""
    
    def __init__(self):
        """Initialize Weather API ingestion"""
        try:
            # Load configuration
            self.db_config = config_loader.get_db_config()
            self.aws_config = config_loader.get_aws_config()
            
            # API configuration
            api_config = self.db_config.get('apis', {}).get('openweathermap', {})
            self.api_key = api_config.get('api_key')
            self.base_url = api_config.get('base_url')
            self.cities = api_config.get('cities', [])
            
            # S3 configuration
            self.s3_client = S3Client()
            self.bronze_bucket = self.aws_config['s3']['bronze_bucket']
            self.weather_prefix = self.aws_config['s3']['prefixes']['weather']
            
            logger.info("Weather API Ingestion initialized")
        
        except Exception as e:
            logger.error(f"Error initializing Weather API Ingestion: {e}")
            raise
    
    def fetch_weather_data(self, city: str) -> Optional[Dict[str, Any]]:
        """
        Fetch weather data for a specific city
        
        Args:
            city: City name
        
        Returns:
            Weather data dictionary or None if error
        """
        try:
            url = f"{self.base_url}/weather"
            params = {
                'q': city,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            logger.info(f"Successfully fetched weather data for {city}")
            return data
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching weather data for {city}: {e}")
            return None
    
    def fetch_all_cities(self) -> List[Dict[str, Any]]:
        """
        Fetch weather data for all configured cities
        
        Returns:
            List of weather data dictionaries
        """
        all_data = []
        
        for city in self.cities:
            data = self.fetch_weather_data(city)
            if data:
                # Add metadata
                data['ingestion_timestamp'] = datetime.now().isoformat()
                data['city_search'] = city
                all_data.append(data)
        
        logger.info(f"Fetched weather data for {len(all_data)} cities")
        return all_data
    
    def save_to_s3(self, data: List[Dict[str, Any]], date: Optional[datetime] = None) -> bool:
        """
        Save weather data to S3 Bronze layer
        
        Args:
            data: List of weather data
            date: Date for partitioning (defaults to today)
        
        Returns:
            True if successful
        """
        if not data:
            logger.warning("No data to save")
            return False
        
        try:
            if date is None:
                date = datetime.now()
            
            # Create partitioned S3 key
            filename = f"weather_{date.strftime('%Y%m%d_%H%M%S')}.json"
            s3_key = self.s3_client.get_partitioned_key(
                base_prefix=self.weather_prefix,
                date=date,
                filename=filename
            )
            
            # Upload to S3
            success = self.s3_client.upload_data(
                data=data,
                bucket=self.bronze_bucket,
                s3_key=s3_key,
                format="json"
            )
            
            if success:
                logger.info(f"Saved weather data to s3://{self.bronze_bucket}/{s3_key}")
            
            return success
        
        except Exception as e:
            logger.error(f"Error saving weather data to S3: {e}")
            return False
    
    def run(self) -> bool:
        """
        Run the complete weather ingestion process
        
        Returns:
            True if successful
        """
        try:
            logger.info("Starting weather data ingestion")
            
            # Fetch data
            weather_data = self.fetch_all_cities()
            
            if not weather_data:
                logger.warning("No weather data fetched")
                return False
            
            # Save to S3
            success = self.save_to_s3(weather_data)
            
            if success:
                logger.info("Weather data ingestion completed successfully")
            
            return success
        
        except Exception as e:
            logger.error(f"Error in weather ingestion process: {e}")
            return False


def main():
    """Main function to run weather ingestion"""
    ingestion = WeatherAPIIngestion()
    success = ingestion.run()
    
    if success:
        print("Weather data ingestion completed successfully")
    else:
        print("Weather data ingestion failed")
        exit(1)


if __name__ == "__main__":
    main()
