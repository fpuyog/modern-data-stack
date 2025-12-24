# AWS Setup Guide

This guide walks you through setting up AWS services for the Modern Data Stack project.

## Prerequisites

- AWS Account (free tier eligible)
- AWS CLI installed and configured
- Basic understanding of AWS services

## Step 1: Configure AWS CLI

```bash
# Install AWS CLI
pip install awscli

# Configure with your credentials
aws configure

# Enter the following when prompted:
# AWS Access Key ID: YOUR_ACCESS_KEY
# AWS Secret Access Key: YOUR_SECRET_KEY
# Default region name: us-east-1
# Default output format: json
```

## Step 2: Create IAM User (Recommended)

Instead of using root account, create an IAM user:

```bash
# Create IAM user
aws iam create-user --user-name data-engineer-user

# Attach necessary policies
aws iam attach-user-policy \
    --user-name data-engineer-user \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-user-policy \
    --user-name data-engineer-user \
    --policy-arn arn:aws:iam::aws:policy/AWSLambda_FullAccess

aws iam attach-user-policy \
    --user-name data-engineer-user \
    --policy-arn arn:aws:iam::aws:policy/AmazonAthenaFullAccess

# Create access key
aws iam create-access-key --user-name data-engineer-user
```

## Step 3: Create S3 Buckets

```bash
# Bronze layer bucket
aws s3 mb s3://modern-data-stack-bronze

# Silver layer bucket
aws s3 mb s3://modern-data-stack-silver

# Gold layer bucket
aws s3 mb s3://modern-data-stack-gold

# Athena results bucket
aws s3 mb s3://modern-data-stack-athena-results

# Verify buckets created
aws s3 ls
```

## Step 4: Configure S3 Bucket Policies (Optional)

Enable versioning for data protection:

```bash
aws s3api put-bucket-versioning \
    --bucket modern-data-stack-bronze \
    --versioning-configuration Status=Enabled
```

## Step 5: Setup AWS Athena

```bash
# Create Athena workgroup (optional)
aws athena create-work-group \
    --name modern-data-stack \
    --configuration ResultConfigurationUpdates={OutputLocation=s3://modern-data-stack-athena-results/}
```

## Step 6: Create Lambda Execution Role

```bash
# Create trust policy file
cat > lambda-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF

# Create IAM role
aws iam create-role \
    --role-name lambda-execution-role \
    --assume-role-policy-document file://lambda-trust-policy.json

# Attach policies
aws iam attach-role-policy \
    --role-name lambda-execution-role \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
    --role-name lambda-execution-role \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
```

## Step 7: Update Configuration Files

Edit `config/aws_config.yaml`:

```yaml
aws:
  region: us-east-1  # Your region
  profile: default

s3:
  bronze_bucket: modern-data-stack-bronze
  silver_bucket: modern-data-stack-silver
  gold_bucket: modern-data-stack-gold
```

## Step 8: Test AWS Connection

```python
python -c "
from src.utils.s3_utils import S3Client
s3 = S3Client()
print('AWS connection successful!')
"
```

## Cost Monitoring

Monitor your AWS costs to stay within free tier:

1. Enable AWS Budgets
2. Set up billing alerts
3. Use AWS Cost Explorer

```bash
# Check current estimated charges
aws ce get-cost-and-usage \
    --time-period Start=2024-12-01,End=2024-12-31 \
    --granularity MONTHLY \
    --metrics BlendedCost
```

## Cleanup (When Done)

To avoid any charges:

```bash
# Empty all buckets
aws s3 rm s3://modern-data-stack-bronze --recursive
aws s3 rm s3://modern-data-stack-silver --recursive
aws s3 rm s3://modern-data-stack-gold --recursive
aws s3 rm s3://modern-data-stack-athena-results --recursive

# Delete buckets
aws s3 rb s3://modern-data-stack-bronze
aws s3 rb s3://modern-data-stack-silver
aws s3 rb s3://modern-data-stack-gold
aws s3 rb s3://modern-data-stack-athena-results
```

## Troubleshooting

### Cannot access S3

- Verify AWS credentials are configured correctly
- Check IAM permissions
- Ensure bucket names are unique globally

### Athena queries failing

- Verify output location is configured
- Check S3 bucket permissions
- Ensure data is in correct format (Parquet)

## Next Steps

After AWS setup is complete:
1. Configure API keys in `config/database_config.yaml`
2. Run data ingestion scripts
3. Start Airflow to orchestrate pipelines
