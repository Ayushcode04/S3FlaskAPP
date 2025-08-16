import boto3
from botocore.exceptions import ClientError
import config

s3_client = boto3.client(
    "s3",
    aws_access_key_id=config.AWS_ACCESS_KEY_ID,
    aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
    region_name=config.AWS_REGION
)

def list_buckets():
    response = s3_client.list_buckets()
    return [bucket["Name"] for bucket in response["Buckets"]]

def create_bucket(bucket_name):
    try:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": config.AWS_REGION}
        )
        return True
    except ClientError as e:
        print(e)
        return False

def delete_bucket(bucket_name):
    try:
        s3_client.delete_bucket(Bucket=bucket_name)
        return True
    except ClientError as e:
        print(e)
        return False
