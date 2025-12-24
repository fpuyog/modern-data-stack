"""
CSV Data Ingestion
Read CSV files and upload to S3 Bronze layer
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
from src.utils.logger import setup_logger
from src.utils.config_loader import config_loader
from src.utils.s3_utils import S3Client

logger = setup_logger(__name__)


class CSVIngestion:
    """Ingest CSV files to S3 Bronze layer"""
    
    def __init__(self, data_dir: str = "data"):
        """
        Initialize CSV ingestion
        
        Args:
            data_dir: Directory containing CSV files
        """
        try:
            self.data_dir = Path(data_dir)
            
            # Load AWS configuration
            self.aws_config = config_loader.get_aws_config()
            self.s3_client = S3Client()
            self.bronze_bucket = self.aws_config['s3']['bronze_bucket']
            self.transactions_prefix = self.aws_config['s3']['prefixes']['transactions']
            
            logger.info(f"CSV Ingestion initialized with data dir: {self.data_dir}")
        
        except Exception as e:
            logger.error(f"Error initializing CSV Ingestion: {e}")
            raise
    
    def read_csv(self, filename: str) -> Optional[pd.DataFrame]:
        """
        Read CSV file into pandas DataFrame
        
        Args:
            filename: CSV filename
        
        Returns:
            DataFrame or None if error
        """
        try:
            filepath = self.data_dir / filename
            
            if not filepath.exists():
                logger.error(f"CSV file not found: {filepath}")
                return None
            
            df = pd.read_csv(filepath)
            logger.info(f"Read {len(df)} rows from {filename}")
            return df
        
        except Exception as e:
            logger.error(f"Error reading CSV file {filename}: {e}")
            return None
    
    def validate_schema(self, df: pd.DataFrame, expected_columns: List[str]) -> bool:
        """
        Validate DataFrame schema
        
        Args:
            df: DataFrame to validate
            expected_columns: List of expected column names
        
        Returns:
            True if schema is valid
        """
        actual_columns = set(df.columns)
        expected_columns_set = set(expected_columns)
        
        if not expected_columns_set.issubset(actual_columns):
            missing = expected_columns_set - actual_columns
            logger.error(f"Missing columns: {missing}")
            return False
        
        logger.info("Schema validation passed")
        return True
    
    def upload_csv_to_s3(
        self, 
        filename: str, 
        prefix: Optional[str] = None,
        date: Optional[datetime] = None
    ) -> bool:
        """
        Upload CSV file directly to S3
        
        Args:
            filename: CSV filename
            prefix: S3 prefix (defaults to transactions)
            date: Date for partitioning
        
        Returns:
            True if successful
        """
        try:
            filepath = self.data_dir / filename
            
            if not filepath.exists():
                logger.error(f"CSV file not found: {filepath}")
                return False
            
            if date is None:
                date = datetime.now()
            
            if prefix is None:
                prefix = self.transactions_prefix
            
            # Create partitioned S3 key
            s3_key = self.s3_client.get_partitioned_key(
                base_prefix=prefix,
                date=date,
                filename=filename
            )
            
            # Upload file
            success = self.s3_client.upload_file(
                local_path=str(filepath),
                bucket=self.bronze_bucket,
                s3_key=s3_key,
                metadata={
                    'source': 'csv_ingestion',
                    'original_filename': filename,
                    'ingestion_date': date.isoformat()
                }
            )
            
            return success
        
        except Exception as e:
            logger.error(f"Error uploading CSV to S3: {e}")
            return False
    
    def process_and_upload(
        self, 
        filename: str,
        expected_columns: Optional[List[str]] = None
    ) -> bool:
        """
        Read, validate, and upload CSV file
        
        Args:
            filename: CSV filename
            expected_columns: Optional list of expected columns for validation
        
        Returns:
            True if successful
        """
        try:
            logger.info(f"Processing CSV file: {filename}")
            
            # Read CSV
            df = self.read_csv(filename)
            if df is None:
                return False
            
            # Validate schema if expected columns provided
            if expected_columns:
                if not self.validate_schema(df, expected_columns):
                    return False
            
            # Upload to S3
            success = self.upload_csv_to_s3(filename)
            
            if success:
                logger.info(f"Successfully processed and uploaded {filename}")
            
            return success
        
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")
            return False


def main():
    """Main function to run CSV ingestion"""
    ingestion = CSVIngestion()
    
    # Example: Upload ecommerce transactions CSV
    expected_cols = ['transaction_id', 'customer_id', 'product', 'amount', 'timestamp']
    success = ingestion.process_and_upload(
        'ecommerce_transactions.csv',
        expected_columns=expected_cols
    )
    
    if success:
        print("CSV ingestion completed successfully")
    else:
        print("CSV ingestion failed")
        exit(1)


if __name__ == "__main__":
    main()
