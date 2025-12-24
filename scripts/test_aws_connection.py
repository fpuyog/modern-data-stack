"""Test AWS Connection and Configuration"""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import sys


def test_aws_credentials():
    """Test if AWS credentials are configured"""
    print("Testing AWS Credentials...")
    print("-" * 60)
    
    try:
        sts = boto3.client('sts')
        identity = sts.get_caller_identity()
        
        print("✓ AWS Credentials are configured")
        print(f"  Account ID: {identity['Account']}")
        print(f"  User ARN: {identity['Arn']}")
        print(f"  User ID: {identity['UserId']}")
        return True
    
    except NoCredentialsError:
        print("✗ No AWS credentials found")
        print("\nTo fix:")
        print("  1. Run: aws configure")
        print("  2. Enter your AWS Access Key ID and Secret Access Key")
        print("  3. Set region to: us-east-1")
        return False
    
    except Exception as e:
        print(f"✗ Error checking credentials: {e}")
        return False


def test_s3_access():
    """Test S3 access"""
    print("\n\nTesting S3 Access...")
    print("-" * 60)
    
    try:
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        
        print("✓ S3 access is working")
        print(f"  You have {len(response['Buckets'])} bucket(s)")
        
        if response['Buckets']:
            print("\n  Your buckets:")
            for bucket in response['Buckets']:
                print(f"    - {bucket['Name']}")
        
        return True
    
    except Exception as e:
        print(f"✗ Cannot access S3: {e}")
        return False


def test_specific_buckets():
    """Test access to project buckets"""
    print("\n\nTesting Project Buckets...")
    print("-" * 60)
    
    buckets = [
        "modern-data-stack-bronze",
        "modern-data-stack-silver",
        "modern-data-stack-gold"
    ]
    
    s3 = boto3.client('s3')
    found_count = 0
    
    for bucket_name in buckets:
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"✓ Bucket exists: {bucket_name}")
            found_count += 1
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"✗ Bucket not found: {bucket_name}")
            else:
                print(f"✗ Cannot access bucket '{bucket_name}': {error_code}")
    
    print(f"\n  Found {found_count}/{len(buckets)} buckets")
    
    if found_count == 0:
        print("\n  To create buckets, run:")
        print("    python scripts/setup_aws_buckets.py")
    
    return found_count > 0


def test_boto3_version():
    """Check boto3 version"""
    print("\n\nChecking boto3 version...")
    print("-" * 60)
    
    try:
        print(f"✓ boto3 version: {boto3.__version__}")
        return True
    except Exception as e:
        print(f"✗ boto3 not installed: {e}")
        print("\nTo install:")
        print("  pip install boto3")
        return False


def main():
    """Run all tests"""
    print("\n" + "=" * 60)
    print("  AWS CONFIGURATION TEST")
    print("=" * 60)
    
    results = {
        "Credentials": test_aws_credentials(),
        "S3 Access": test_s3_access(),
        "Project Buckets": test_specific_buckets(),
        "boto3": test_boto3_version()
    }
    
    print("\n\n" + "=" * 60)
    print("  TEST SUMMARY")
    print("=" * 60)
    
    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"  {test_name:20} {status}")
    
    all_passed = all(results.values())
    
    print("\n" + "=" * 60)
    
    if all_passed:
        print("✓ All tests passed! You're ready to run the pipeline.")
        print("\nNext step:")
        print("  python scripts/run_pipeline.py")
    else:
        print("✗ Some tests failed. Please fix the issues above.")
        print("\nCommon fixes:")
        print("  1. Configure AWS: aws configure")
        print("  2. Create buckets: python scripts/setup_aws_buckets.py")
        print("  3. Install boto3: pip install boto3")
    
    print("=" * 60 + "\n")
    
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
