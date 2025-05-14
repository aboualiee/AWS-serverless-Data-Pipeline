import boto3
import os
from botocore.exceptions import ClientError

def upload_to_s3(file_path, bucket_name, object_name=None):
    """
    Upload a file to an S3 bucket. Creates the bucket if it does not exist.
    
    Parameters:
    - file_path: Path to the file to upload
    - bucket_name: Name of the S3 bucket
    - object_name: S3 object name. If not specified, the file name is used
    """
    if object_name is None:
        object_name = os.path.basename(file_path)

    # Use default region from environment
    session = boto3.session.Session()
    region = session.region_name or 'us-east-1'
    s3 = session.client('s3')

    # Check if bucket exists
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' exists.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ['404', 'NoSuchBucket']:
            print(f"Bucket '{bucket_name}' does not exist. Creating bucket...")
            try:
                if region == 'us-east-1':
                    s3.create_bucket(Bucket=bucket_name)
                else:
                    s3.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={'LocationConstraint': region}
                    )
                print(f"Bucket '{bucket_name}' created.")
            except ClientError as e:
                print(f"Failed to create bucket: {e}")
                return False
        else:
            print(f"Failed to access bucket: {e}")
            return False

    # Upload file
    try:
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"File uploaded to s3://{bucket_name}/{object_name}")
        return True
    except ClientError as e:
        print(f"Failed to upload file: {e}")
        return False
