"""
Simple Data Uploader - Generates and uploads test data to S3
No external APIs required - works standalone
"""

import boto3
import json
import random
from datetime import datetime
from pathlib import Path


def generate_weather_data():
    """Generate realistic synthetic weather data"""
    cities = [
        {"name": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
        {"name": "London", "country": "GB", "lat": 51.5074, "lon": -0.1278},
        {"name": "Tokyo", "country": "JP", "lat": 35.6762, "lon": 139.6503},
        {"name": "Buenos Aires", "country": "AR", "lat": -34.6037, "lon": -58.3816},
    ]
    
    all_data = []
    for city in cities:
        data = {
            "coord": {"lat": city["lat"], "lon": city["lon"]},
            "weather": [{
                "main": random.choice(["Clear", "Clouds", "Rain"]),
                "description": random.choice(["clear sky", "few clouds", "light rain"])
            }],
            "main": {
                "temp": round(random.uniform(15, 28), 2),
                "feels_like": round(random.uniform(14, 27), 2),
                "temp_min": round(random.uniform(10, 23), 2),
                "temp_max": round(random.uniform(18, 32), 2),
                "pressure": random.randint(1005, 1020),
                "humidity": random.randint(45, 75)
            },
            "wind": {"speed": round(random.uniform(1, 8), 2)},
            "clouds": {"all": random.randint(10, 90)},
            "dt": int(datetime.now().timestamp()),
            "sys": {"country": city["country"]},
            "id": random.randint(1000000, 9999999),
            "name": city["name"],
            "ingestion_timestamp": datetime.now().isoformat()
        }
        all_data.append(data)
    
    return all_data


def upload_to_s3(data, bucket, prefix):
    """Upload data to S3"""
    s3 = boto3.client('s3')
    
    # Create S3 key with date partitioning
    now = datetime.now()
    s3_key = f"{prefix}year={now.year}/month={now.month:02d}/day={now.day:02d}/weather_{now.strftime('%Y%m%d_%H%M%S')}.json"
    
    # Upload
    s3.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(data, indent=2)
    )
    
    return s3_key


def main():
    print("=" * 70)
    print("  SIMPLE DATA UPLOADER - Test Data to S3")
    print("=" * 70)
    print()
    
    # Generate data
    print("[1/3] Generating synthetic weather data...")
    weather_data = generate_weather_data()
    print(f"✓ Generated data for {len(weather_data)} cities")
    
    # Upload to S3
    print("\n[2/3] Uploading to S3...")
    bucket = "modern-data-stack-bronze"
    prefix = "weather/"
    
    try:
        s3_key = upload_to_s3(weather_data, bucket, prefix)
        print(f"✓ Uploaded to: s3://{bucket}/{s3_key}")
    except Exception as e:
        print(f"✗ Error uploading: {e}")
        print("\nTroubleshooting:")
        print("  - Verify AWS credentials: python scripts/test_aws_connection.py")
        print("  - Check bucket exists: python scripts/setup_aws_buckets.py")
        return False
    
    # Verify
    print("\n[3/3] Verifying upload...")
    try:
        s3 = boto3.client('s3')
        objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        count = objects.get('KeyCount', 0)
        print(f"✓ Found {count} file(s) in s3://{bucket}/{prefix}")
        
        if count > 0:
            print("\nFiles in Bronze layer:")
            for obj in objects.get('Contents', []):
                print(f"  - {obj['Key']}")
    
    except Exception as e:
        print(f"✗ Error verifying: {e}")
        return False
    
    print("\n" + "=" * 70)
    print("✓ SUCCESS! Data uploaded to S3 Bronze layer")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. View data in AWS Console: https://s3.console.aws.amazon.com/")
    print("  2. Run transformations (optional)")
    print("  3. Query with Athena")
    print()
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        exit(0 if success else 1)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        exit(1)
