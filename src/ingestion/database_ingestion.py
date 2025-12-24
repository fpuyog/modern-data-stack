"""
Database Data Ingestion
Extract data from PostgreSQL database and upload to S3 Bronze layer
"""

import psycopg2
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, Any, List
import json
from src.utils.logger import setup_logger
from src.utils.config_loader import config_loader
from src.utils.s3_utils import S3Client

logger = setup_logger(__name__)


class DatabaseIngestion:
    """Extract data from PostgreSQL and load to S3"""
    
    def __init__(self, use_local: bool = True):
        """
        Initialize Database ingestion
        
        Args:
            use_local: If True, use local PostgreSQL; else use RDS
        """
        try:
            # Load configuration
            self.db_config = config_loader.get_db_config()
            self.aws_config = config_loader.get_aws_config()
            
            # Database configuration
            db_section = 'local' if use_local else 'rds'
            self.db_params = self.db_config['postgres'][db_section]
            
            # S3 configuration
            self.s3_client = S3Client()
            self.bronze_bucket = self.aws_config['s3']['bronze_bucket']
            self.users_prefix = self.aws_config['s3']['prefixes']['users']
            
            self.connection = None
            
            logger.info("Database Ingestion initialized")
        
        except Exception as e:
            logger.error(f"Error initializing Database Ingestion: {e}")
            raise
    
    def connect(self) -> bool:
        """
        Establish database connection
        
        Returns:
            True if successful
        """
        try:
            self.connection = psycopg2.connect(
                host=self.db_params['host'],
                port=self.db_params['port'],
                database=self.db_params['database'],
                user=self.db_params['username'],
                password=self.db_params['password']
            )
            logger.info("Database connection established")
            return True
        
        except psycopg2.Error as e:
            logger.error(f"Error connecting to database: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def execute_query(self, query: str) -> Optional[pd.DataFrame]:
        """
        Execute SQL query and return results as DataFrame
        
        Args:
            query: SQL query string
        
        Returns:
            DataFrame with results or None if error
        """
        try:
            if not self.connection:
                if not self.connect():
                    return None
            
            df = pd.read_sql_query(query, self.connection)
            logger.info(f"Query executed successfully, {len(df)} rows returned")
            return df
        
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return None
    
    def extract_table(self, table_name: str, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Extract entire table or with limit
        
        Args:
            table_name: Name of the table
            limit: Optional row limit
        
        Returns:
            DataFrame with table data
        """
        query = f"SELECT * FROM {table_name}"
        if limit:
            query += f" LIMIT {limit}"
        
        return self.execute_query(query)
    
    def extract_incremental(
        self, 
        table_name: str, 
        timestamp_column: str,
        last_extracted: Optional[datetime] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract data incrementally based on timestamp
        
        Args:
            table_name: Table name
            timestamp_column: Column name for timestamp filtering
            last_extracted: Last extraction timestamp
        
        Returns:
            DataFrame with incremental data
        """
        query = f"SELECT * FROM {table_name}"
        
        if last_extracted:
            query += f" WHERE {timestamp_column} > '{last_extracted.isoformat()}'"
        
        query += f" ORDER BY {timestamp_column}"
        
        return self.execute_query(query)
    
    def save_to_s3(
        self, 
        df: pd.DataFrame, 
        prefix: str,
        filename: str,
        date: Optional[datetime] = None
    ) -> bool:
        """
        Save DataFrame to S3 as JSON
        
        Args:
            df: DataFrame to save
            prefix: S3 prefix
            filename: File name
            date: Date for partitioning
        
        Returns:
            True if successful
        """
        if df is None or df.empty:
            logger.warning("No data to save")
            return False
        
        try:
            if date is None:
                date = datetime.now()
            
            # Convert DataFrame to JSON
            data = df.to_dict('records')
            
            # Add metadata
            payload = {
                'data': data,
                'metadata': {
                    'row_count': len(df),
                    'columns': list(df.columns),
                    'extraction_timestamp': datetime.now().isoformat()
                }
            }
            
            # Create partitioned S3 key
            s3_key = self.s3_client.get_partitioned_key(
                base_prefix=prefix,
                date=date,
                filename=filename
            )
            
            # Upload to S3
            success = self.s3_client.upload_data(
                data=payload,
                bucket=self.bronze_bucket,
                s3_key=s3_key,
                format="json"
            )
            
            if success:
                logger.info(f"Saved {len(df)} rows to s3://{self.bronze_bucket}/{s3_key}")
            
            return success
        
        except Exception as e:
            logger.error(f"Error saving data to S3: {e}")
            return False
    
    def run(self, table_name: str = "users", limit: Optional[int] = None) -> bool:
        """
        Run the complete database ingestion process
        
        Args:
            table_name: Table to extract
            limit: Optional row limit
        
        Returns:
            True if successful
        """
        try:
            logger.info(f"Starting database ingestion for table: {table_name}")
            
            # Connect to database
            if not self.connect():
                return False
            
            # Extract data
            df = self.extract_table(table_name, limit=limit)
            
            if df is None or df.empty:
                logger.warning(f"No data extracted from {table_name}")
                return False
            
            # Save to S3
            filename = f"{table_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            success = self.save_to_s3(
                df=df,
                prefix=self.users_prefix,
                filename=filename
            )
            
            # Cleanup
            self.disconnect()
            
            if success:
                logger.info("Database ingestion completed successfully")
            
            return success
        
        except Exception as e:
            logger.error(f"Error in database ingestion process: {e}")
            self.disconnect()
            return False


def main():
    """Main function to run database ingestion"""
    ingestion = DatabaseIngestion(use_local=True)
    success = ingestion.run(table_name="users")
    
    if success:
        print("Database ingestion completed successfully")
    else:
        print("Database ingestion failed")
        exit(1)


if __name__ == "__main__":
    main()
