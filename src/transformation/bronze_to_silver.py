"""
Bronze to Silver Transformation
Clean and standardize data from Bronze layer using PySpark
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from datetime import datetime
from typing import Optional

from src.utils.logger import setup_logger
from src.utils.config_loader import config_loader

logger = setup_logger(__name__)


class BronzeToSilverTransformation:
    """Transform Bronze layer data to Silver layer"""
    
    def __init__(self):
        """Initialize Spark session and configuration"""
        try:
            # Create Spark session
            self.spark = SparkSession.builder \
                .appName("Bronze to Silver Transformation") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            # Set log level
            self.spark.sparkContext.setLogLevel("WARN")
            
            # Load configuration
            self.aws_config = config_loader.get_aws_config()
            self.bronze_bucket = self.aws_config['s3']['bronze_bucket']
            self.silver_bucket = self.aws_config['s3']['silver_bucket']
            
            logger.info("Bronze to Silver transformation initialized")
        
        except Exception as e:
            logger.error(f"Error initializing transformation: {e}")
            raise
    
    def read_bronze_data(self, s3_path: str, format: str = "json") -> Optional[DataFrame]:
        """
        Read data from Bronze layer
        
        Args:
            s3_path: S3 path to Bronze data
            format: Data format (json, csv, parquet)
        
        Returns:
            Spark DataFrame or None
        """
        try:
            full_path = f"s3a://{self.bronze_bucket}/{s3_path}"
            
            if format == "json":
                df = self.spark.read.json(full_path)
            elif format == "csv":
                df = self.spark.read.csv(full_path, header=True, inferSchema=True)
            elif format == "parquet":
                df = self.spark.read.parquet(full_path)
            else:
                logger.error(f"Unsupported format: {format}")
                return None
            
            logger.info(f"Read {df.count()} rows from {full_path}")
            return df
        
        except Exception as e:
            logger.error(f"Error reading Bronze data: {e}")
            return None
    
    def transform_weather_data(self, df: DataFrame) -> DataFrame:
        """
        Transform weather data from Bronze to Silver
        
        Args:
            df: Raw weather DataFrame
        
        Returns:
            Cleaned DataFrame
        """
        try:
            # Flatten nested JSON structure
            df_clean = df.select(
                F.col("id").cast(IntegerType()).alias("city_id"),
                F.col("name").alias("city_name"),
                F.col("coord.lat").cast(DoubleType()).alias("latitude"),
                F.col("coord.lon").cast(DoubleType()).alias("longitude"),
                F.col("main.temp").cast(DoubleType()).alias("temperature_celsius"),
                F.col("main.feels_like").cast(DoubleType()).alias("feels_like_celsius"),
                F.col("main.temp_min").cast(DoubleType()).alias("temp_min_celsius"),
                F.col("main.temp_max").cast(DoubleType()).alias("temp_max_celsius"),
                F.col("main.pressure").cast(IntegerType()).alias("pressure_hpa"),
                F.col("main.humidity").cast(IntegerType()).alias("humidity_percent"),
                F.col("wind.speed").cast(DoubleType()).alias("wind_speed_mps"),
                F.col("clouds.all").cast(IntegerType()).alias("cloudiness_percent"),
                F.col("weather")[0]["main"].alias("weather_condition"),
                F.col("weather")[0]["description"].alias("weather_description"),
                F.col("dt").cast(IntegerType()).alias("measurement_timestamp"),
                F.col("sys.country").alias("country_code"),
                F.col("ingestion_timestamp").cast(TimestampType()).alias("ingestion_timestamp")
            )
            
            # Remove duplicates
            df_clean = df_clean.dropDuplicates(["city_id", "measurement_timestamp"])
            
            # Remove nulls in critical fields
            df_clean = df_clean.filter(
                F.col("city_id").isNotNull() & 
                F.col("temperature_celsius").isNotNull()
            )
            
            # Add processing timestamp
            df_clean = df_clean.withColumn(
                "processing_timestamp",
                F.current_timestamp()
            )
            
            logger.info(f"Transformed weather data: {df_clean.count()} rows")
            return df_clean
        
        except Exception as e:
            logger.error(f"Error transforming weather data: {e}")
            raise
    
    def transform_stock_data(self, df: DataFrame) -> DataFrame:
        """
        Transform stock data from Bronze to Silver
        
        Args:
            df: Raw stock DataFrame
        
        Returns:
            Cleaned DataFrame
        """
        try:
            # Extract time series data
            # Note: Alpha Vantage returns nested structure
            # This is a simplified transformation
            
            df_clean = df.select(
                F.col("symbol").alias("stock_symbol"),
                F.col("Meta Data.2. Symbol").alias("metadata_symbol"),
                F.col("Meta Data.3. Last Refreshed").alias("last_refreshed"),
                F.col("ingestion_timestamp").cast(TimestampType()).alias("ingestion_timestamp")
            )
            
            # Remove duplicates
            df_clean = df_clean.dropDuplicates(["stock_symbol", "last_refreshed"])
            
            # Add processing timestamp
            df_clean = df_clean.withColumn(
                "processing_timestamp",
                F.current_timestamp()
            )
            
            logger.info(f"Transformed stock data: {df_clean.count()} rows")
            return df_clean
        
        except Exception as e:
            logger.error(f"Error transforming stock data: {e}")
            raise
    
    def write_silver_data(
        self, 
        df: DataFrame, 
        output_prefix: str,
        partition_columns: Optional[list] = None
    ) -> bool:
        """
        Write data to Silver layer in Parquet format
        
        Args:
            df: DataFrame to write
            output_prefix: S3 prefix for output
            partition_columns: Optional list of columns for partitioning
        
        Returns:
            True if successful
        """
        try:
            output_path = f"s3a://{self.silver_bucket}/{output_prefix}"
            
            writer = df.write.mode("overwrite").format("parquet")
            
            if partition_columns:
                writer = writer.partitionBy(*partition_columns)
            
            writer.save(output_path)
            
            logger.info(f"Wrote {df.count()} rows to {output_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error writing Silver data: {e}")
            return False
    
    def run_weather_transformation(self, date: Optional[datetime] = None) -> bool:
        """
        Run complete weather transformation pipeline
        
        Args:
            date: Date for data processing (defaults to today)
        
        Returns:
            True if successful
        """
        try:
            if date is None:
                date = datetime.now()
            
            date_str = f"year={date.year}/month={date.month:02d}/day={date.day:02d}"
            bronze_path = f"weather/{date_str}/*.json"
            
            logger.info("Starting weather Bronze → Silver transformation")
            
            # Read Bronze data
            df_bronze = self.read_bronze_data(bronze_path, format="json")
            if df_bronze is None:
                return False
            
            # Transform
            df_silver = self.transform_weather_data(df_bronze)
            
            # Write to Silver
            success = self.write_silver_data(
                df_silver,
                "weather_cleaned/",
                partition_columns=["country_code"]
            )
            
            return success
        
        except Exception as e:
            logger.error(f"Error in weather transformation pipeline: {e}")
            return False
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main function to run Bronze to Silver transformation"""
    transformation = BronzeToSilverTransformation()
    
    try:
        # Run weather transformation
        success = transformation.run_weather_transformation()
        
        if success:
            print("Bronze to Silver transformation completed successfully")
        else:
            print("Bronze to Silver transformation failed")
            exit(1)
    
    finally:
        transformation.stop()


if __name__ == "__main__":
    main()
