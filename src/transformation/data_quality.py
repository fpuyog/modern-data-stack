"""
Data Quality Checks
Validate data quality and generate reports
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, List
from datetime import datetime
import json

from src.utils.logger import setup_logger
from src.utils.s3_utils import S3Client

logger = setup_logger(__name__)


class DataQualityChecker:
    """Perform data quality checks on datasets"""
    
    def __init__(self):
        """Initialize Data Quality Checker"""
        try:
            self.spark = SparkSession.builder \
                .appName("Data Quality Checker") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            self.s3_client = S3Client()
            
            logger.info("Data Quality Checker initialized")
        
        except Exception as e:
            logger.error(f"Error initializing Data Quality Checker: {e}")
            raise
    
    def check_null_rates(self, df: DataFrame) -> Dict[str, float]:
        """
        Calculate null rate for each column
        
        Args:
            df: DataFrame to check
        
        Returns:
            Dictionary with column names and null rates
        """
        total_rows = df.count()
        null_rates = {}
        
        for column in df.columns:
            null_count = df.filter(F.col(column).isNull()).count()
            null_rate = (null_count / total_rows) * 100 if total_rows > 0 else 0
            null_rates[column] = round(null_rate, 2)
        
        logger.info(f"Calculated null rates for {len(df.columns)} columns")
        return null_rates
    
    def check_duplicates(self, df: DataFrame, key_columns: List[str]) -> Dict[str, Any]:
        """
        Check for duplicate records based on key columns
        
        Args:
            df: DataFrame to check
            key_columns: List of columns that should be unique
        
        Returns:
            Dictionary with duplicate statistics
        """
        total_rows = df.count()
        distinct_rows = df.select(key_columns).distinct().count()
        duplicate_count = total_rows - distinct_rows
        duplicate_rate = (duplicate_count / total_rows) * 100 if total_rows > 0 else 0
        
        result = {
            "total_rows": total_rows,
            "distinct_rows": distinct_rows,
            "duplicate_count": duplicate_count,
            "duplicate_rate": round(duplicate_rate, 2)
        }
        
        logger.info(f"Found {duplicate_count} duplicates ({duplicate_rate:.2f}%)")
        return result
    
    def check_value_ranges(self, df: DataFrame, column: str) -> Dict[str, Any]:
        """
        Check value ranges for numeric columns
        
        Args:
            df: DataFrame to check
            column: Column name
        
        Returns:
            Dictionary with min, max, avg, stddev
        """
        try:
            stats = df.select(
                F.min(column).alias("min"),
                F.max(column).alias("max"),
                F.avg(column).alias("avg"),
                F.stddev(column).alias("stddev"),
                F.count(column).alias("count")
            ).collect()[0]
            
            result = {
                "column": column,
                "min": float(stats["min"]) if stats["min"] is not None else None,
                "max": float(stats["max"]) if stats["max"] is not None else None,
                "avg": float(stats["avg"]) if stats["avg"] is not None else None,
                "stddev": float(stats["stddev"]) if stats["stddev"] is not None else None,
                "count": stats["count"]
            }
            
            logger.info(f"Calculated value ranges for column: {column}")
            return result
        
        except Exception as e:
            logger.error(f"Error checking value ranges for {column}: {e}")
            return {}
    
    def check_schema(self, df: DataFrame, expected_schema: Dict[str, str]) -> Dict[str, Any]:
        """
        Validate DataFrame schema against expected schema
        
        Args:
            df: DataFrame to validate
            expected_schema: Dictionary with column names and expected types
        
        Returns:
            Dictionary with schema validation results
        """
        actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}
        
        missing_columns = set(expected_schema.keys()) - set(actual_schema.keys())
        extra_columns = set(actual_schema.keys()) - set(expected_schema.keys())
        type_mismatches = []
        
        for col, expected_type in expected_schema.items():
            if col in actual_schema and actual_schema[col] != expected_type:
                type_mismatches.append({
                    "column": col,
                    "expected": expected_type,
                    "actual": actual_schema[col]
                })
        
        result = {
            "schema_valid": len(missing_columns) == 0 and len(type_mismatches) == 0,
            "missing_columns": list(missing_columns),
            "extra_columns": list(extra_columns),
            "type_mismatches": type_mismatches
        }
        
        logger.info(f"Schema validation: {result['schema_valid']}")
        return result
    
    def generate_quality_report(
        self, 
        df: DataFrame, 
        dataset_name: str,
        key_columns: List[str] = None
    ) -> Dict[str, Any]:
        """
        Generate comprehensive data quality report
        
        Args:
            df: DataFrame to analyze
            dataset_name: Name of the dataset
            key_columns: Optional list of key columns for duplicate checking
        
        Returns:
            Complete quality report dictionary
        """
        try:
            logger.info(f"Generating quality report for: {dataset_name}")
            
            report = {
                "dataset_name": dataset_name,
                "timestamp": datetime.now().isoformat(),
                "row_count": df.count(),
                "column_count": len(df.columns),
                "columns": df.columns,
                "null_rates": self.check_null_rates(df)
            }
            
            # Check for duplicates if key columns provided
            if key_columns:
                report["duplicate_check"] = self.check_duplicates(df, key_columns)
            
            # Check numeric columns
            numeric_checks = {}
            for column in df.columns:
                col_type = dict(df.dtypes)[column]
                if col_type in ['int', 'bigint', 'double', 'float']:
                    numeric_checks[column] = self.check_value_ranges(df, column)
            
            if numeric_checks:
                report["numeric_statistics"] = numeric_checks
            
            logger.info(f"Quality report generated for {dataset_name}")
            return report
        
        except Exception as e:
            logger.error(f"Error generating quality report: {e}")
            return {}
    
    def save_report(self, report: Dict[str, Any], output_path: str) -> bool:
        """
        Save quality report to file
        
        Args:
            report: Quality report dictionary
            output_path: Path to save report
        
        Returns:
            True if successful
        """
        try:
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.info(f"Saved quality report to: {output_path}")
            return True
        
        except Exception as e:
            logger.error(f"Error saving report: {e}")
            return False
    
    def stop(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()


def main():
    """Example usage of Data Quality Checker"""
    checker = DataQualityChecker()
    
    # This is just an example - would need actual data
    print("Data Quality Checker initialized")
    
    checker.stop()


if __name__ == "__main__":
    main()
