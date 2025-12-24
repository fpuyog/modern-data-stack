"""
Stock Market API Data Ingestion
Extract stock data from Alpha Vantage API and store in S3 Bronze layer
"""

import requests
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
import time
from src.utils.logger import setup_logger
from src.utils.config_loader import config_loader
from src.utils.s3_utils import S3Client

logger = setup_logger(__name__)


class StockAPIIngestion:
    """Extract stock data from Alpha Vantage API"""
    
    def __init__(self):
        """Initialize Stock API ingestion"""
        try:
            # Load configuration
            self.db_config = config_loader.get_db_config()
            self.aws_config = config_loader.get_aws_config()
            
            # API configuration
            api_config = self.db_config.get('apis', {}).get('alphavantage', {})
            self.api_key = api_config.get('api_key')
            self.base_url = api_config.get('base_url')
            self.symbols = api_config.get('symbols', [])
            
            # S3 configuration
            self.s3_client = S3Client()
            self.bronze_bucket = self.aws_config['s3']['bronze_bucket']
            self.stocks_prefix = self.aws_config['s3']['prefixes']['stocks']
            
            logger.info("Stock API Ingestion initialized")
        
        except Exception as e:
            logger.error(f"Error initializing Stock API Ingestion: {e}")
            raise
    
    def fetch_daily_stock_data(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Fetch daily stock data for a specific symbol
        
        Args:
            symbol: Stock symbol (e.g., 'AAPL')
        
        Returns:
            Stock data dictionary or None if error
        """
        try:
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': symbol,
                'apikey': self.api_key,
                'outputsize': 'compact'  # Last 100 data points
            }
            
            response = requests.get(self.base_url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if 'Error Message' in data:
                logger.error(f"API error for {symbol}: {data['Error Message']}")
                return None
            
            if 'Note' in data:  # Rate limit message
                logger.warning(f"API rate limit for {symbol}: {data['Note']}")
                return None
            
            logger.info(f"Successfully fetched stock data for {symbol}")
            return data
        
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching stock data for {symbol}: {e}")
            return None
    
    def fetch_all_symbols(self) -> List[Dict[str, Any]]:
        """
        Fetch stock data for all configured symbols
        Note: Alpha Vantage free tier has rate limits (5 API requests per minute)
        
        Returns:
            List of stock data dictionaries
        """
        all_data = []
        
        for i, symbol in enumerate(self.symbols):
            data = self.fetch_daily_stock_data(symbol)
            
            if data:
                # Add metadata
                data['ingestion_timestamp'] = datetime.now().isoformat()
                data['symbol'] = symbol
                all_data.append(data)
            
            # Rate limiting: wait between requests (free tier limit is 5 calls/minute)
            if i < len(self.symbols) - 1:
                logger.info("Waiting to respect API rate limits...")
                time.sleep(12)  # 12 seconds between requests = 5 requests per minute
        
        logger.info(f"Fetched stock data for {len(all_data)} symbols")
        return all_data
    
    def save_to_s3(self, data: List[Dict[str, Any]], date: Optional[datetime] = None) -> bool:
        """
        Save stock data to S3 Bronze layer
        
        Args:
            data: List of stock data
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
            filename = f"stocks_{date.strftime('%Y%m%d_%H%M%S')}.json"
            s3_key = self.s3_client.get_partitioned_key(
                base_prefix=self.stocks_prefix,
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
                logger.info(f"Saved stock data to s3://{self.bronze_bucket}/{s3_key}")
            
            return success
        
        except Exception as e:
            logger.error(f"Error saving stock data to S3: {e}")
            return False
    
    def run(self) -> bool:
        """
        Run the complete stock ingestion process
        
        Returns:
            True if successful
        """
        try:
            logger.info("Starting stock data ingestion")
            
            # Fetch data
            stock_data = self.fetch_all_symbols()
            
            if not stock_data:
                logger.warning("No stock data fetched")
                return False
            
            # Save to S3
            success = self.save_to_s3(stock_data)
            
            if success:
                logger.info("Stock data ingestion completed successfully")
            
            return success
        
        except Exception as e:
            logger.error(f"Error in stock ingestion process: {e}")
            return False


def main():
    """Main function to run stock ingestion"""
    ingestion = StockAPIIngestion()
    success = ingestion.run()
    
    if success:
        print("Stock data ingestion completed successfully")
    else:
        print("Stock data ingestion failed")
        exit(1)


if __name__ == "__main__":
    main()
