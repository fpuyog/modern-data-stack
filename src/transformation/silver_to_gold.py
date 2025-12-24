"""
Silver to Gold Transformation
Create business-level aggregations and analytics-ready datasets
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
from typing import Optional

from src.utils.logger import setup_logger
from src.utils.config_loader import config_loader

logger = setup_logger(__name__)


class SilverToGoldTransformation:
    """Transform Silver layer data to Gold layer (analytics-ready)"""
    
    def __init__(self):
        """Initialize Spark session and configuration"""
        try:
            # Create Spark session
            self.spark = SparkSession.builder \
                .appName("Silver to Gold Transformation") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            
            # Load configuration
            self.aws_config = config_loader.get_aws_config()
            self.silver_bucket = self.aws_config['s3']['silver_bucket']
            self.gold_bucket = self.aws_config['s3']['gold_bucket']
            
            logger.info("Silver to Gold transformation initialized")
        
        except Exception as e:
            logger.error(f"Error initializing transformation: {e}")
            raise
    
    def read_silver_data(self, s3_path: str) -> Optional[DataFrame]:
        """
        Read data from Silver layer (Parquet format)
        
        Args:
            s3_path: S3 path to Silver data
        
        Returns:
            Spark DataFrame
        """
        try:
            full_path = f"s3a://{self.silver_bucket}/{s3_path}"
            df = self.spark.read.parquet(full_path)
            logger.info(f"Read {df.count()} rows from {full_path}")
            return df
        
        except Exception as e:
            logger.error(f"Error reading Silver data: {e}")
            return None
    
    def create_weather_aggregations(self, df: DataFrame) -> DataFrame:
        """
        Create aggregated weather metrics
        
        Args:
            df: Silver weather DataFrame
        
        Returns:
            Aggregated DataFrame
        """
        try:
            # Daily aggregations by city
            df_agg = df.groupBy("city_name", "country_code") \
                .agg(
                    F.avg("temperature_celsius").alias("avg_temperature"),
                    F.min("temp_min_celsius").alias("min_temperature"),
                    F.max("temp_max_celsius").alias("max_temperature"),
                    F.avg("humidity_percent").alias("avg_humidity"),
                    F.avg("pressure_hpa").alias("avg_pressure"),
                    F.avg("wind_speed_mps").alias("avg_wind_speed"),
                    F.count("*").alias("measurement_count"),
                    F.max("processing_timestamp").alias("last_updated")
                )
            
            # Round decimal values
            for col in ["avg_temperature", "min_temperature", "max_temperature", 
                       "avg_humidity", "avg_pressure", "avg_wind_speed"]:
                df_agg = df_agg.withColumn(col, F.round(F.col(col), 2))
            
            logger.info(f"Created weather aggregations: {df_agg.count()} cities")
            return df_agg
        
        except Exception as e:
            logger.error(f"Error creating weather aggregations: {e}")
            raise
    
    def create_weather_trends(self, df: DataFrame) -> DataFrame:
        """
        Create weather trend analysis
        
        Args:
            df: Silver weather DataFrame
        
        Returns:
            Trend DataFrame
        """
        try:
            # Window specification for ranking
            window_spec = Window.partitionBy("city_name").orderBy(F.col("measurement_timestamp").desc())
            
            # Add row number to get latest vs previous
            df_ranked = df.withColumn("rank", F.row_number().over(window_spec))
            
            # Get current and previous readings
            df_current = df_ranked.filter(F.col("rank") == 1) \
                .select(
                    "city_name",
                    "country_code",
                    F.col("temperature_celsius").alias("current_temp"),
                    F.col("humidity_percent").alias("current_humidity"),
                    "weather_condition",
                    "measurement_timestamp"
                )
            
            logger.info(f"Created weather trends for {df_current.count()} cities")
            return df_current
        
        except Exception as e:
            logger.error(f"Error creating weather trends: {e}")
            raise
    
    def create_dimension_table(self, df: DataFrame, dim_name: str) -> DataFrame:
        """
        Create dimension table with deduplication
        
        Args:
            df: Source DataFrame
            dim_name: Dimension name
        
        Returns:
            Dimension DataFrame
        """
        try:
            if dim_name == "city":
                dim_df = df.select(
                    "city_id",
                    "city_name",
                    "country_code",
                    "latitude",
                    "longitude"
                ).dropDuplicates(["city_id"])
                
                # Add surrogate key if needed
                dim_df = dim_df.withColumn(
                    "dim_city_key",
                    F.monotonically_increasing_id()
                )
            
            logger.info(f"Created {dim_name} dimension: {dim_df.count()} rows")
            return dim_df
        
        except Exception as e:
            logger.error(f"Error creating dimension table: {e}")
            raise
    
    def write_gold_data(self, df: DataFrame, output_prefix: str) -> bool:
        """
        Write data to Gold layer in Parquet format
        
        Args:
            df: DataFrame to write
            output_prefix: S3 prefix for output
        
        Returns:
            True if successful
        """
        try:
            output_path = f"s3a://{self.gold_bucket}/{output_prefix}"
            
            df.write.mode("overwrite") \
                .format("parquet") \
                .option("compression", "snappy") \
                .save(output_path)
            
            logger.info(f"Wrote {df.count()} rows to {output_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error writing Gold data: {e}")
            return False
    
    def run_weather_gold_transformation(self) -> bool:
        """
        Run complete weather Silver → Gold transformation
        
        Returns:
            True if successful
        """
        try:
            logger.info("Starting weather Silver → Gold transformation")
            
            # Read Silver data
            df_silver = self.read_silver_data("weather_cleaned/")
            if df_silver is None:
                return False
            
            # Create aggregations
            df_aggregated = self.create_weather_aggregations(df_silver)
            success_agg = self.write_gold_data(df_aggregated, "aggregated_weather_metrics/")
            
            # Create trends
            df_trends = self.create_weather_trends(df_silver)
            success_trends = self.write_gold_data(df_trends, "weather_trends/")
            
            # Create dimension table
            df_dim_city = self.create_dimension_table(df_silver, "city")
            success_dim = self.write_gold_data(df_dim_city, "dim_city/")
            
            return success_agg and success_trends and success_dim
        
        except Exception as e:
            logger.error(f"Error in Gold transformation pipeline: {e}")
            return False
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main function to run Silver to Gold transformation"""
    transformation = SilverToGoldTransformation()
    
    try:
        success = transformation.run_weather_gold_transformation()
        
        if success:
            print("Silver to Gold transformation completed successfully")
        else:
            print("Silver to Gold transformation failed")
            exit(1)
    
    finally:
        transformation.stop()


if __name__ == "__main__":
    main()
