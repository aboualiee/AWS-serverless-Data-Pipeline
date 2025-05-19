
# CloudFormation Templates

This is the guideline for the CloudFormation template used to deploy the NYC Taxi Analytics pipeline infrastructure.

## nyc_taxi_analytics_template.yaml

This template deploys the complete data analytics pipeline with the following components:

- S3 bucket for data storage  
- Lambda function for distance calculations  
- Glue ETL job for data aggregation  
- Step Functions state machine for workflow orchestration  
- EventBridge rule for automatic triggering  
- EMR cluster for Spark analytics  
- Athena workgroup for SQL queries  

---

## Deployment Guide

### Prerequisites

Before deploying the infrastructure, ensure you have:

- An AWS account with appropriate permissions  
- An EC2 key pair created for EMR cluster access  
- A subnet ID where the EMR cluster will be launched  
- The CloudFormation template uploaded to your S3 bucket  

---

### Option 1: Deploy from S3 using AWS Console

#### Step 1: Upload Template to S3

1. Navigate to your S3 bucket (`nyc-taxi-analytics-project`)  
2. Upload the `nyc_taxi_analytics_template.yaml` file to the `cloudformation/` folder  
3. Copy the S3 URL of the uploaded template  

#### Step 2: Create Stack from S3 Template

1. Navigate to **CloudFormation** in the AWS Console  
2. Click **Create stack**  
3. Choose **Template Source**:  
   - Select **Amazon S3 URL**  
   - Paste the S3 URL:  
     `https://nyc-taxi-analytics-project.s3.eu-north-1.amazonaws.com/cloudformation/nyc_taxi_analytics_template.yaml`  

4. *(Optional)* Click **View in Infrastructure Composer** for a visual layout and validation  
![Infrastructure Composer View](images/infrastructure_composer.png)  

5. **Specify Stack Details**:  
![Stack Name Configuration](images/stack_name.png)  
   - Stack name: `nycstack`  
   - Parameters:  
     - `BucketName`: `nyc-taxi-analytics-project`  
     - `KeyPairName`: `nyc_keypair`  
     - `S3KeyPrefix`: `raw-data/`  
     - `SubnetId`: *your subnet ID*  

6. **Configure Stack Options**: Adjust tags/permissions as needed  

7. **Review and Create**:  
   - Acknowledge IAM resource creation  
   - Click **Create stack**  

#### Step 3: Monitor Deployment

- Monitor progress in the CloudFormation console  
- Takes 5–10 minutes  
- Verify successful creation of all resources  

---

### Option 2: Deploy using AWS CLI

```bash
# Copy the template to your S3 bucket
aws s3 cp nyc_taxi_analytics_template.yaml s3://nyc-taxi-analytics-project/cloudformation/

# Create the stack
aws cloudformation create-stack \
  --stack-name NYCTaxiAnalyticsPipeline \
  --template-url https://nyc-taxi-analytics-project.s3.eu-north-1.amazonaws.com/cloudformation/nyc_taxi_analytics_template.yaml \
  --parameters ParameterKey=SubnetId,ParameterValue=subnet-xxx \
               ParameterKey=KeyPairName,ParameterValue=nyc_keypair \
               ParameterKey=BucketName,ParameterValue=nyc-taxi-analytics-project \
  --capabilities CAPABILITY_NAMED_IAM
```

---

### Option 3: Deploy by Uploading Template File

```bash
aws cloudformation create-stack \
  --stack-name NYCTaxiAnalyticsPipeline \
  --template-body file://nyc_taxi_analytics_template.yaml \
  --parameters ParameterKey=SubnetId,ParameterValue=subnet-xxx \
               ParameterKey=KeyPairName,ParameterValue=nyc_keypair \
               ParameterKey=BucketName,ParameterValue=nyc-taxi-analytics-project \
  --capabilities CAPABILITY_NAMED_IAM
```

##  Post-Deployment Verification

Verify the creation of the following resources:

### Core Resources

- **S3 Bucket**: `nyc-taxi-analytics-project` with folders:  
  - `raw-data/`  
  - `processed-data/`  
  - `aggregations/`  
  - `emr-results/`  
  - `athena-results/`  
  - `cloudformation/`  

### Processing Components

- **Lambda Function**: `NYCTaxiDistanceCalculator`  
- **Glue Job**: `NYCTaxiETLJob`  
- **EMR Cluster**: `NYCTaxiEMRCluster` (on-demand)  

### Orchestration & Analytics

- **Step Functions**: `NYCTaxiProcessingWorkflow`  
- **EventBridge Rule**: `NYCTaxiS3EventRule`  
- **Athena Database**: `nyc_taxi_analytics`  
- **Athena Workgroup**: `NYCTaxiWorkgroup`  

### Security & IAM

- **Glue Role**: `NYCTaxiGlueRole`  
- **Lambda Role**: `NYCTaxiLambdaRole`  
- **Step Functions Role**: `NYCTaxiStepFunctionsRole`  
- **EMR Service Role**: `NYCTaxiEMRRole`  
- **EventBridge Role**: `NYCTaxiEventBridgeRole`  

---

## Stack Management

### Template Validation

Validate the template before deployment:

```bash
aws cloudformation validate-template \
  --template-url https://nyc-taxi-analytics-project.s3.eu-north-1.amazonaws.com/cloudformation/nyc_taxi_analytics_template.yaml
```

Or for a local file:

```bash
aws cloudformation validate-template \
  --template-body file://nyc_taxi_analytics_template.yaml
```

---

### Stack Updates

To apply changes to the stack:

```bash
aws cloudformation update-stack \
  --stack-name NYCTaxiAnalyticsPipeline \
  --template-url https://nyc-taxi-analytics-project.s3.eu-north-1.amazonaws.com/cloudformation/nyc_taxi_analytics_template.yaml \
  --parameters ParameterKey=SubnetId,ParameterValue=subnet-xxx \
               ParameterKey=KeyPairName,ParameterValue=nyc_keypair \
  --capabilities CAPABILITY_NAMED_IAM
```

---

### Stack Deletion

To remove all created resources:

```bash
aws cloudformation delete-stack --stack-name NYCTaxiAnalyticsPipeline
```

