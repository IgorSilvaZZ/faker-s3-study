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

def verify_exists_object(bucket, key):
    response = s3.list_objects_v2(Bucket=bucket, Prefix=key)

    return 'Contents' in response

def list_objects(bucket):

    response = {}

    try:
        response = s3.list_objects_v2(Bucket=bucket)
    except Exception as e:
        logging.error(e)

    return response

def generate_url(bucket_name, key, expiration=3600):
    url = ''
    try:
        url = s3.generate_presigned_url(ClientMethod='get_object', Params={ 'Bucket': bucket_name, 'Key': key }, ExpiresIn=expiration)

        return url
    except Exception as e:
        logging.error(e)

# Melhorar o paginate e verificar antes se o objeto existe no bucket
def list_paginate_objects(bucket, prefix):

    paginator = s3.get_paginator('list_objects_v2')

    paginate = paginator.paginate(Bucket=bucket, Prefix=prefix)

    response = []

    for key_data in paginate:
        if 'Contents' in key_data:
            files = key_data['Contents']

            for file in files:
                if str(file['Key']).lower().endswith('.zip'):
                    response.append(file)

    return response