# Serverless AWS Pipeline – NYC Taxi Trip Data Analytics 

## Overview
A scalable, serverless analytics pipeline built on AWS for processing and analyzing New York City taxi trip data. This project automates the ingestion, transformation, and querying of taxi data to extract meaningful insights on trip patterns, vendor performance, and route efficiency—all powered by event-driven workflows and managed services.

>  End-to-end data pipeline |  Event-driven processing |  Business intelligence with SQL access

## Features

- **Fully Automated Pipeline**: Event-driven architecture using S3 and EventBridge
- **Serverless Processing**: AWS Lambda and Glue ETL jobs handle data transformations
- **Advanced Analytics**: Spark jobs run on EMR to process large datasets efficiently
- **SQL Access**: Amazon Athena enables stakeholder-friendly querying
- **Scalable Architecture**: Built to handle millions of records with minimal ops overhead
- **Security Best Practices**: Fine-grained IAM roles and encrypted storage

## Architecture

![NYC Taxi Analytics Architecture](images/architecture-diagram.png)  
*Fig 1: NYC Taxi Analytics AWS Architecture*

### Architecture Components:

1. **Amazon S3**:  `nyc-taxi-analytics-project`
   - Raw data stored under `raw-data/`
   - Intermediate outputs in `processed-data/`, `aggregations/`, `emr-results/`
   - Query results saved in `athena-results/`
   - CloudFormation template saved in `cloudformation/`

2. **AWS Lambda**:
   - Function: `NYCTaxiDistanceCalculator`
   - Computes Haversine and Manhattan distances

3. **AWS Glue**:
   - Job: `NYCTaxiETLJob`
   - Performs daily/hourly aggregations and location-based analysis

4. **Step Functions**:
   - State machine orchestrates Lambda and Glue tasks
   - Implements retries, success/failure states, and traceability

5. **Amazon EventBridge**:
   - Rule: `NYCTaxiS3EventRule`
   - Triggers the pipeline when new files are uploaded to S3

6. **Amazon EMR**:
   - Cluster: `NYCTaxiEMRCluster`
   - Runs advanced Spark analytics and generates batch insights

7. **Amazon Athena**:
   - Database: `nyc_taxi_analytics`
   - Used for SQL-based exploration of aggregated datasets

8. **IAM Roles**:
   - Fine-grained permissions for each service component
   - Includes `NYCTaxiGlueRole`, `NYCTaxiLambdaRole`, `NYCTaxiEMRRole`, etc.

## Key Analytics and Insights

- **Trip Distance Calculations**: Both straight-line (Haversine) and grid-style (Manhattan)
- **Hourly/Daily Trends**: Temporal analysis of trip volumes and durations
- **Location Hotspots**: Top pickup/dropoff areas
- **Route Optimization**: Detects slow trips and route inefficiencies
- **Vendor Performance**: Compares service quality across companies

## Data Flow Process

```
1. Raw Data Upload → s3://nyc-taxi-analytics-project/raw-data/
                ↓
2. EventBridge Trigger → Starts Step Functions Workflow
                ↓
3. Lambda Function → Computes Distance Metrics
                ↓
4. Glue Job → ETL and Aggregation
                ↓
5. EMR Cluster → Advanced Analytics with Spark
                ↓
6. Athena Queries → SQL Access to Results
```

## Deployment Process

### Infrastructure as Code

The project infrastructure is fully automated using CloudFormation. The template includes:

- Data storage and processing resources
- Serverless components (Lambda, Step Functions)
- Big data processing (Glue, EMR)
- Analytics capabilities (Athena)
- IAM roles and security configurations
- Event-driven automation (EventBridge)

To deploy the infrastructure:

1. Clone this repository
2. Navigate to the cloudformation directory
3. Deploy using AWS CLI or CloudFormation console

See the [CloudFormation README](cloudformation/README.md) for detailed deployment instructions.

### Recreate Manually

1. Create and configure S3 bucket `nyc-taxi-analytics-project`
2. Deploy Lambda function with required layers (pandas, numpy)
3. Create Glue job and assign IAM role
4. Define Step Functions workflow with Lambda and Glue steps
5. Set up EventBridge rule to trigger on S3 object creation
6. Launch EMR cluster and submit Spark applications
7. Set up Athena database and tables to run analytical queries

## Helper Script for S3 Bucket Management

To streamline the setup and file management process, we include a Python helper script (`s3_helper.py`) that uses Boto3 to automate the creation and population of the `nyc-taxi-analytics-project` S3 bucket.

### What It Does

- **Checks for Bucket Existence**: If the S3 bucket does not already exist, it will be automatically created in your AWS account.
- **Creates Folder Hierarchy**: Allows uploading files to specific subfolders like `raw-data/` etc., by setting the S3 object key accordingly.
- **Uploads Files**: Handles the actual upload of any specified local file to the designated S3 path.

### Example Usage

```python
from s3_helper import upload_to_s3

upload_to_s3(
    file_path='.../data/test_eventbridge.csv',
    bucket_name='nyc-taxi-analytics-project',
    object_name='raw-data/test_eventbridge.csv'
)
```

### Why It Matters

The s3_helper script simplifies our deployment workflow by ensuring the bucket and folder structure are set up correctly before triggering the pipeline. It’s especially useful for local testing or for automating uploads in CI/CD environments.

## System Performance and Scalability

- Processes over **1.45 million records**
- **Serverless design** ensures cost-efficiency
- **Auto-termination on EMR** minimizes idle costs
- **Elastic scalability** with minimal manual intervention

## Future Enhancements

- Real-time ingestion via Amazon Kinesis
- Predictive analytics with Amazon SageMaker
- Business dashboards using Amazon QuickSight
- REST API integration for exposing insights

## How to Use

1. Upload raw CSV files to `s3://nyc-taxi-analytics-project/raw-data/`
2. EventBridge will trigger the automated pipeline
3. Explore processed and aggregated data via Athena
4. Use EMR Spark scripts for custom batch analysis

## License

Distributed under the MIT License. See `LICENSE` for more information.
