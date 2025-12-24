"""
S3 Utility Functions
Helper functions for interacting with AWS S3
"""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import List, Optional, Dict, Any
from pathlib import Path
import json
from datetime import datetime
from src.utils.logger import setup_logger
from src.utils.config_loader import config_loader

logger = setup_logger(__name__)


class S3Client:
    """AWS S3 Client wrapper with utility methods"""
    
    def __init__(self, region: Optional[str] = None, profile: Optional[str] = None):
        """
        Initialize S3 client
        
        Args:
            region: AWS region (defaults to config or us-east-1)
            profile: AWS profile name (defaults to config or default)
        """
        try:
            # Load AWS config
            aws_config = config_loader.get_aws_config()
            
            self.region = region or aws_config.get('aws', {}).get('region', 'us-east-1')
            self.profile = profile or aws_config.get('aws', {}).get('profile', 'default')
            
            # Create boto3 session
            session = boto3.Session(profile_name=self.profile, region_name=self.region)
            self.s3_client = session.client('s3')
            self.s3_resource = session.resource('s3')
            
            logger.info(f"S3 Client initialized for region: {self.region}")
        
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except Exception as e:
            logger.error(f"Error initializing S3 client: {e}")
            raise
    
    def upload_file(
        self, 
        local_path: str, 
        bucket: str, 
        s3_key: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Upload file to S3
        
        Args:
            local_path: Local file path
            bucket: S3 bucket name
            s3_key: S3 object key (path)
            metadata: Optional metadata dictionary
        
        Returns:
            True if successful, False otherwise
        """
        try:
            extra_args = {}
            if metadata:
                extra_args['Metadata'] = metadata
            
            self.s3_client.upload_file(local_path, bucket, s3_key, ExtraArgs=extra_args)
            logger.info(f"Uploaded {local_path} to s3://{bucket}/{s3_key}")
            return True
        
        except FileNotFoundError:
            logger.error(f"Local file not found: {local_path}")
            return False
        except ClientError as e:
            logger.error(f"Error uploading to S3: {e}")
            return False
    
    def upload_data(
        self, 
        data: Any, 
        bucket: str, 
        s3_key: str,
        format: str = "json"
    ) -> bool:
        """
        Upload data directly to S3 (without local file)
        
        Args:
            data: Data to upload (dict, list, or string)
            bucket: S3 bucket name
            s3_key: S3 object key
            format: Data format ('json' or 'text')
        
        Returns:
            True if successful
        """
        try:
            if format == "json":
                body = json.dumps(data, default=str)
            else:
                body = str(data)
            
            self.s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=body
            )
            logger.info(f"Uploaded data to s3://{bucket}/{s3_key}")
            return True
        
        except ClientError as e:
            logger.error(f"Error uploading data to S3: {e}")
            return False
    
    def download_file(self, bucket: str, s3_key: str, local_path: str) -> bool:
        """
        Download file from S3
        
        Args:
            bucket: S3 bucket name
            s3_key: S3 object key
            local_path: Local destination path
        
        Returns:
            True if successful
        """
        try:
            # Create parent directories if needed
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.s3_client.download_file(bucket, s3_key, local_path)
            logger.info(f"Downloaded s3://{bucket}/{s3_key} to {local_path}")
            return True
        
        except ClientError as e:
            logger.error(f"Error downloading from S3: {e}")
            return False
    
    def list_objects(
        self, 
        bucket: str, 
        prefix: str = "", 
        max_keys: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        List objects in S3 bucket with prefix
        
        Args:
            bucket: S3 bucket name
            prefix: Object key prefix
            max_keys: Maximum number of keys to return
        
        Returns:
            List of object metadata dictionaries
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=max_keys
            )
            
            objects = response.get('Contents', [])
            logger.info(f"Found {len(objects)} objects in s3://{bucket}/{prefix}")
            return objects
        
        except ClientError as e:
            logger.error(f"Error listing S3 objects: {e}")
            return []
    
    def delete_object(self, bucket: str, s3_key: str) -> bool:
        """
        Delete object from S3
        
        Args:
            bucket: S3 bucket name
            s3_key: S3 object key
        
        Returns:
            True if successful
        """
        try:
            self.s3_client.delete_object(Bucket=bucket, Key=s3_key)
            logger.info(f"Deleted s3://{bucket}/{s3_key}")
            return True
        
        except ClientError as e:
            logger.error(f"Error deleting S3 object: {e}")
            return False
    
    def object_exists(self, bucket: str, s3_key: str) -> bool:
        """
        Check if object exists in S3
        
        Args:
            bucket: S3 bucket name
            s3_key: S3 object key
        
        Returns:
            True if object exists
        """
        try:
            self.s3_client.head_object(Bucket=bucket, Key=s3_key)
            return True
        except ClientError:
            return False
    
    def get_partitioned_key(
        self, 
        base_prefix: str, 
        date: Optional[datetime] = None,
        filename: str = "data.json"
    ) -> str:
        """
        Generate partitioned S3 key with date
        
        Args:
            base_prefix: Base prefix (e.g., 'weather/')
            date: Date for partitioning (defaults to today)
            filename: File name
        
        Returns:
            Partitioned S3 key
        """
        if date is None:
            date = datetime.now()
        
        key = f"{base_prefix}year={date.year}/month={date.month:02d}/day={date.day:02d}/{filename}"
        return key


# Example usage
if __name__ == "__main__":
    # This will fail without proper AWS configuration
    try:
        s3 = S3Client()
        print("S3 Client initialized successfully")
    except Exception as e:
        print(f"Error: {e}")
