# CloudFormation Templates

This directory contains the CloudFormation templates used to deploy the NYC Taxi Analytics pipeline infrastructure.

## nyc_taxi_analytics_template.yaml

This template deploys the complete data analytics pipeline with the following components:

- S3 bucket for data storage
- Lambda function for distance calculations
- Glue ETL job for data aggregation
- Step Functions state machine for workflow orchestration
- EventBridge rule for automatic triggering
- EMR cluster for Spark analytics
- Athena workgroup for SQL queries

### Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| BucketName | Name of the S3 bucket | nyc-taxi-analytics-project |
| S3KeyPrefix | Prefix for raw data files | raw-data/ |
| SubnetId | Subnet ID for EMR cluster | (required) |
| KeyPairName | EC2 key pair for SSH access | nyc_keypair |

### Deployment

To deploy this template using the AWS CLI:

```bash
aws cloudformation create-stack \
  --stack-name NYCTaxiAnalyticsPipeline \
  --template-body file://nyc_taxi_analytics_template.yaml \
  --parameters ParameterKey=SubnetId,ParameterValue=subnet-xxx \
               ParameterKey=KeyPairName,ParameterValue=nyc_keypair \
  --capabilities CAPABILITY_NAMED_IAM