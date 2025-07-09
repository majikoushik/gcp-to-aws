import os
from google.cloud import storage as gcs
import boto3

# ---------------- CONFIGURATION ---------------- #

# GCP settings
GCP_BUCKET_NAME = os.environ['GCP_BUCKET_NAME']
GCP_BUCKET_PREFIX = ''

# AWS settings
AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']
AWS_REGION = os.environ['AWS_REGION']
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']
S3_BUCKET_PREFIX = ''

# ------------------------------------------------ #


def transfer_gcs_to_s3():
    # Initialize GCP client
    gcp_client = gcs.Client()

    #List all GCP buckets
    print("GCP Bucket List:")
    for bucket in gcp_client.list_buckets():
        print(f"  - {bucket.name}")

    #Initialize AWS S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )

    #List all S3 buckets
    print("\nAWS S3 Bucket List:")
    response = s3_client.list_buckets()
    for bucket in response['Buckets']:
        print(f"  - {bucket['Name']}")

    print("\nStarting transfer from GCS to S3...\n")

    #List blobs in the specified GCP bucket
    blobs = gcp_client.list_blobs(GCP_BUCKET_NAME, prefix=GCP_BUCKET_PREFIX)

    for blob in blobs:
        print(f"Processing: {blob.name}")

        # Download blob content to memory
        data = blob.download_as_bytes()

        # Define destination key
        s3_key = S3_BUCKET_PREFIX + blob.name[len(GCP_BUCKET_PREFIX):]
        print(f"S3_KEY: {s3_key}")

        # Upload to S3
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=data)
        print(f"Uploaded to S3: {s3_key}")

    print("\nTransfer complete.")


if __name__ == '__main__':
    transfer_gcs_to_s3()
