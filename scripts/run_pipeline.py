"""
Complete Pipeline Runner
Executes the entire data pipeline from ingestion to transformation
"""

import sys
import os
from datetime import datetime

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.ingestion.weather_api_ingestion import WeatherAPIIngestion
from src.ingestion.stock_api_ingestion import StockAPIIngestion
from src.ingestion.csv_ingestion import CSVIngestion
from src.utils.logger import setup_logger

logger = setup_logger(__name__)


def print_header(text):
    """Print a formatted header"""
    print()
    print("=" * 70)
    print(f"  {text}")
    print("=" * 70)
    print()


def print_step(step_num, total_steps, text):
    """Print a step indicator"""
    print(f"\n[Step {step_num}/{total_steps}] {text}")
    print("-" * 70)


def run_pipeline():
    """Run the complete data pipeline"""
    start_time = datetime.now()
    
    print_header("MODERN DATA STACK - PIPELINE EXECUTION")
    print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    total_steps = 3
    current_step = 0
    
    # Step 1: Weather Data Ingestion
    current_step += 1
    print_step(current_step, total_steps, "Weather Data Ingestion")
    
    try:
        weather_ingestion = WeatherAPIIngestion()
        weather_success = weather_ingestion.run()
        
        if weather_success:
            print("✓ Weather data ingestion completed successfully")
        else:
            print("✗ Weather data ingestion failed")
            return False
    
    except Exception as e:
        logger.error(f"Weather ingestion error: {e}")
        print(f"✗ Error: {e}")
        print("\nTroubleshooting:")
        print("  - Check your OpenWeatherMap API key in config/database_config.yaml")
        print("  - Verify AWS credentials are configured (aws configure)")
        print("  - Check internet connection")
        return False
    
    # Step 2: Stock Data Ingestion
    current_step += 1
    print_step(current_step, total_steps, "Stock Data Ingestion")
    print("⚠ This step takes ~1 minute due to API rate limits...")
    
    try:
        stock_ingestion = StockAPIIngestion()
        stock_success = stock_ingestion.run()
        
        if stock_success:
            print("✓ Stock data ingestion completed successfully")
        else:
            print("✗ Stock data ingestion failed")
            return False
    
    except Exception as e:
        logger.error(f"Stock ingestion error: {e}")
        print(f"✗ Error: {e}")
        print("\nTroubleshooting:")
        print("  - Check your Alpha Vantage API key in config/database_config.yaml")
        print("  - Free tier has rate limits (5 calls/minute)")
        return False
    
    # Step 3: CSV Data Ingestion
    current_step += 1
    print_step(current_step, total_steps, "CSV Data Ingestion")
    
    try:
        csv_ingestion = CSVIngestion()
        csv_success = csv_ingestion.process_and_upload('sample_ecommerce_transactions.csv')
        
        if csv_success:
            print("✓ CSV data ingestion completed successfully")
        else:
            print("✗ CSV data ingestion failed")
            return False
    
    except Exception as e:
        logger.error(f"CSV ingestion error: {e}")
        print(f"✗ Error: {e}")
        return False
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    print_header("PIPELINE EXECUTION COMPLETED")
    print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration.total_seconds():.2f} seconds")
    print()
    print("✓ All data ingested successfully to S3 Bronze layer!")
    print()
    print("Next steps:")
    print("1. Verify data in S3:")
    print("   aws s3 ls s3://modern-data-stack-bronze/weather/ --recursive")
    print("   aws s3 ls s3://modern-data-stack-bronze/stocks/ --recursive")
    print("   aws s3 ls s3://modern-data-stack-bronze/transactions/ --recursive")
    print()
    print("2. (Optional) Run PySpark transformations:")
    print("   python src/transformation/bronze_to_silver.py")
    print()
    print("3. Query data in AWS Athena:")
    print("   - Go to AWS Athena Console")
    print("   - Create tables using sql/athena_table_creation.sql")
    print("   - Run queries from sql/analytical_queries.sql")
    print()
    
    return True


if __name__ == "__main__":
    try:
        success = run_pipeline()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nPipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"\n✗ Unexpected error: {e}")
        sys.exit(1)
