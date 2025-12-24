"""
AWS Bucket Setup Script
Creates all necessary S3 buckets for the project
"""

import boto3
from botocore.exceptions import ClientError
import sys

# Bucket names - CHANGE THESE if they are already taken
BUCKETS = [
    "modern-data-stack-bronze",
    "modern-data-stack-silver",
    "modern-data-stack-gold",
    "modern-data-stack-athena-results"
]

def create_bucket(bucket_name, region='us-east-1'):
    """Create an S3 bucket in a specified region"""
    try:
        s3_client = boto3.client('s3', region_name=region)
        
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location
            )
        
        print(f"✓ Bucket created: {bucket_name}")
        return True
    
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'BucketAlreadyExists':
            print(f"✗ Bucket '{bucket_name}' already exists globally. Choose a different name.")
        elif error_code == 'BucketAlreadyOwnedByYou':
            print(f"✓ Bucket '{bucket_name}' already exists and you own it.")
            return True
        else:
            print(f"✗ Error creating bucket '{bucket_name}': {e}")
        
        return False


def verify_bucket(bucket_name):
    """Verify that a bucket exists and is accessible"""
    try:
        s3_client = boto3.client('s3')
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✓ Bucket verified: {bucket_name}")
        return True
    except ClientError:
        print(f"✗ Bucket not accessible: {bucket_name}")
        return False


def main():
    print("=" * 60)
    print("AWS S3 Bucket Setup Script")
    print("=" * 60)
    print()
    
    # Test AWS credentials
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        print(f"AWS Account ID: {identity['Account']}")
        print(f"User ARN: {identity['Arn']}")
        print()
    except Exception as e:
        print(f"✗ Error: Cannot connect to AWS. Please configure credentials:")
        print(f"  Run: aws configure")
        print(f"  Error details: {e}")
        sys.exit(1)
    
    # Create buckets
    print("Creating S3 buckets...")
    print("-" * 60)
    
    success_count = 0
    for bucket_name in BUCKETS:
        if create_bucket(bucket_name):
            success_count += 1
        print()
    
    # Verify all buckets
    print("-" * 60)
    print("Verifying buckets...")
    print("-" * 60)
    
    for bucket_name in BUCKETS:
        verify_bucket(bucket_name)
    
    print()
    print("=" * 60)
    print(f"Setup complete: {success_count}/{len(BUCKETS)} buckets ready")
    print("=" * 60)
    
    if success_count < len(BUCKETS):
        print()
        print("⚠ Some buckets couldn't be created.")
        print("  Option 1: Change bucket names in this script and in config/aws_config.yaml")
        print("  Option 2: Create them manually in AWS Console")
        sys.exit(1)
    else:
        print()
        print("✓ All buckets created successfully!")
        print()
        print("Next steps:")
        print("1. Update config/aws_config.yaml with your bucket names")
        print("2. Get API keys (OpenWeatherMap, Alpha Vantage)")
        print("3. Update config/database_config.yaml with API keys")
        print("4. Run: python src/ingestion/weather_api_ingestion.py")


if __name__ == "__main__":
    main()
