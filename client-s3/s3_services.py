import boto3
import logging

s3 = boto3.client('s3', endpoint_url='http://localhost:4000',
                  aws_access_key_id='S3RVER', aws_secret_access_key='S3RVER')


def create_bucket(bucket_name):
    try:
        s3.create_bucket(Bucket=bucket_name)
    except Exception as e:
        logging.error(e)
        return False

    return True


def list_buckets():
    print('List Existing buckets:')
    response = s3.list_buckets()
    for bucket in response['Buckets']:
        print(bucket['Name'])


def upload_to_s3(file_name_to_upload, bucket, key):
    try:
        s3.upload_file(file_name_to_upload, bucket, key)
    except Exception as e:
        logging.error(e)


def list_objects(bucket):

    response = {}

    try:
        response = s3.list_objects_v2(Bucket=bucket)
    except Exception as e:
        logging.error(e)

    return response