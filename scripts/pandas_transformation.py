"""
Bronze to Silver Transformation - Pandas Version
Works better on Windows, processes data locally then uploads to S3
"""

import boto3
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
import tempfile


def download_bronze_data(s3_client, bucket, prefix):
    """Download all JSON files from Bronze layer"""
    print("[1/5] Downloading data from S3 Bronze layer...")
    
    # List all objects
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' not in response:
        print("✗ No data found in Bronze layer")
        return None
    
    all_data = []
    for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.json'):
            # Download and parse JSON
            response = s3_client.get_object(Bucket=bucket, Key=key)
            data = json.loads(response['Body'].read())
            
            # If it's a list of records
            if isinstance(data, list):
                all_data.extend(data)
            else:
                all_data.append(data)
    
    print(f"✓ Downloaded {len(all_data)} records")
    return all_data


def transform_to_silver(bronze_data):
    """Transform Bronze data to Silver format"""
    print("\n[2/5] Transforming data...")
    
    # Flatten JSON structure
    records = []
    for item in bronze_data:
        record = {
            'city_id': item.get('id'),
            'city_name': item.get('name'),
            'latitude': item.get('coord', {}).get('lat'),
            'longitude': item.get('coord', {}).get('lon'),
            'temperature_celsius': item.get('main', {}).get('temp'),
            'feels_like_celsius': item.get('main', {}).get('feels_like'),
            'temp_min_celsius': item.get('main', {}).get('temp_min'),
            'temp_max_celsius': item.get('main', {}).get('temp_max'),
            'pressure_hpa': item.get('main', {}).get('pressure'),
            'humidity_percent': item.get('main', {}).get('humidity'),
            'wind_speed_mps': item.get('wind', {}).get('speed'),
            'cloudiness_percent': item.get('clouds', {}).get('all'),
            'weather_condition': item.get('weather', [{}])[0].get('main') if item.get('weather') else None,
            'weather_description': item.get('weather', [{}])[0].get('description') if item.get('weather') else None,
            'measurement_timestamp': item.get('dt'),
            'country_code': item.get('sys', {}).get('country'),
            'ingestion_timestamp': item.get('ingestion_timestamp'),
            'processing_timestamp': datetime.now().isoformat()
        }
        records.append(record)
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Data quality: remove duplicates
    df = df.drop_duplicates(subset=['city_id', 'measurement_timestamp'])
    
    # Data quality: remove records with missing critical fields
    df = df.dropna(subset=['city_id', 'temperature_celsius'])
    
    print(f"✓ Transformed {len(df)} records")
    print("\nSample data:")
    print(df[['city_name', 'temperature_celsius', 'humidity_percent', 'country_code']].head())
    
    return df


def upload_to_silver(df, s3_client, bucket, prefix):
    """Upload DataFrame to S3 Silver layer as Parquet"""
    print("\n[3/5] Writing to S3 Silver layer...")
    
    # Group by country for partitioning
    countries = df['country_code'].unique()
    
    uploaded_files = []
    for country in countries:
        df_country = df[df['country_code'] == country]
        
        # Create temporary file path
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        temp_path = f'temp_weather_{country}_{timestamp}.parquet'
        
        try:
            # Save to Parquet
            df_country.to_parquet(temp_path, index=False, engine='pyarrow')
            
            # Upload to S3
            s3_key = f"{prefix}country_code={country}/weather_cleaned_{timestamp}.parquet"
            s3_client.upload_file(temp_path, bucket, s3_key)
            
            uploaded_files.append(s3_key)
            print(f"  ✓ Uploaded: s3://{bucket}/{s3_key}")
        
        finally:
            # Clean up temp file
            if Path(temp_path).exists():
                Path(temp_path).unlink()
    
    print(f"\n✓ Uploaded {len(uploaded_files)} partition(s)")
    return True


def verify_silver_data(s3_client, bucket, prefix):
    """Verify data in Silver layer"""
    print("\n[4/5] Verifying Silver layer...")
    
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    
    if 'Contents' not in response:
        print("✗ No data found in Silver layer")
        return False
    
    files = [obj['Key'] for obj in response['Contents']]
    print(f"✓ Found {len(files)} file(s) in Silver layer:")
    for f in files[:5]:  # Show first 5
        print(f"  - {f}")
    
    return True


def create_gold_aggregations(df):
    """Create Gold layer aggregations"""
    print("\n[5/5] Creating Gold layer aggregations...")
    
    # Aggregation 1: Average metrics by city
    gold_agg = df.groupby(['city_name', 'country_code']).agg({
        'temperature_celsius': ['mean', 'min', 'max'],
        'humidity_percent': 'mean',
        'pressure_hpa': 'mean',
        'wind_speed_mps': 'mean',
        'city_id': 'count'
    }).reset_index()
    
    # Flatten column names
    gold_agg.columns = [
        'city_name', 'country_code',
        'avg_temperature', 'min_temperature', 'max_temperature',
        'avg_humidity', 'avg_pressure', 'avg_wind_speed',
        'measurement_count'
    ]
    
    # Round decimals
    for col in ['avg_temperature', 'min_temperature', 'max_temperature', 
                'avg_humidity', 'avg_pressure', 'avg_wind_speed']:
        gold_agg[col] = gold_agg[col].round(2)
    
    gold_agg['last_updated'] = datetime.now().isoformat()
    
    print(f"✓ Created aggregations for {len(gold_agg)} cities")
    print("\nGold layer sample:")
    print(gold_agg[['city_name', 'avg_temperature', 'avg_humidity']].head())
    
    return gold_agg


def upload_gold_data(df, s3_client, bucket, prefix):
    """Upload Gold aggregations to S3"""
    print("\nUploading to Gold layer...")
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    temp_path = f'temp_gold_agg_{timestamp}.parquet'
    
    try:
        df.to_parquet(temp_path, index=False, engine='pyarrow')
        
        s3_key = f"{prefix}aggregated_weather_metrics.parquet"
        s3_client.upload_file(temp_path, bucket, s3_key)
        
        print(f"✓ Uploaded: s3://{bucket}/{s3_key}")
    
    finally:
        if Path(temp_path).exists():
            Path(temp_path).unlink()
    
    return True


def main():
    """Main execution"""
    print("=" * 70)
    print("  DATA TRANSFORMATION: BRONZE → SILVER → GOLD")
    print("  Using Pandas (Windows-friendly)")
    print("=" * 70)
    print()
    
    # Configuration
    bronze_bucket = "modern-data-stack-bronze"
    silver_bucket = "modern-data-stack-silver"
    gold_bucket = "modern-data-stack-gold"
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    
    try:
        # Bronze → Silver
        bronze_data = download_bronze_data(s3, bronze_bucket, "weather/")
        if not bronze_data:
            return False
        
        df_silver = transform_to_silver(bronze_data)
        
        upload_to_silver(df_silver, s3, silver_bucket, "weather_cleaned/")
        
        verify_silver_data(s3, silver_bucket, "weather_cleaned/")
        
        # Silver → Gold
        df_gold = create_gold_aggregations(df_silver)
        upload_gold_data(df_gold, s3, gold_bucket, "aggregated_weather/")
        
        print("\n" + "=" * 70)
        print("✓ TRANSFORMATION COMPLETE!")
        print("=" * 70)
        print()
        print("Data locations:")
        print(f"  Silver: s3://{silver_bucket}/weather_cleaned/")
        print(f"  Gold:   s3://{gold_bucket}/aggregated_weather/")
        print()
        print("Next step: Query data in Athena")
        print()
        
        return True
    
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
