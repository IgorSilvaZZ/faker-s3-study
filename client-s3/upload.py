import os
from datetime import datetime
from zipfile import ZipFile
import json
import pandas as pd
from os.path import basename

from s3_services import verify_exists_object, upload_to_s3, put_to_s3, list_paginate_objects, generate_url, get_head_object

def verify_file_exists_bucket(bucket, key):
    return verify_exists_object(bucket, key)

def list_paginate(bucket, prefix):
    response = []
    
    files_paginate = list_paginate_objects(bucket, prefix)

    for file in files_paginate:
        key = file['Key']

        url = generate_url(bucket, key)

        name_file = os.path.basename(key)

        new_response_object = { 'name': name_file, 'url': url }

        response.append(new_response_object)
    
    return response

def list_paginate_object(bucket, prefix):
    response = []
    
    files_paginate = list_paginate_objects(bucket, prefix)

    for file in files_paginate:
        key = file['Key']

        url = generate_url(bucket, key)

        name_file = os.path.basename(key)

        object_response = get_head_object(bucket, key)

        new_response_object = { 'name': name_file, 'url': url, "metadatas": object_response['Metadata'] }

        response.append(new_response_object)
    
    print(response)
    return response

def send_create_csv():
    bucket_name = 'my_bucket'

    name = 'igorsilva'

    date = '060223'

    prefix = f"igorsilva/{date}"

    json_file = open('tecs.json')

    technologies = json.load(json_file)

    columns_names = ['Courses', 'Fee','Discount']

    name_csv_file = "courses.csv"

    name_csv_to_zip = "courses.zip"

    compression_options = dict(method='zip', archive_name=name_csv_file)
    
    df = pd.DataFrame(technologies)

    df.to_csv(name_csv_to_zip, index=False, columns=columns_names, compression=compression_options)

    name_zip_to_bucket = f"{name}/{date}/{name_csv_to_zip}"

    metadata = {
        "name": name
    }

    put_to_s3(name_csv_to_zip, bucket_name, name_zip_to_bucket, metadata)

    os.remove(name_csv_to_zip)

    response = list_paginate(bucket_name, prefix)

    print(response)

def create_file_csv_ziped() :
    json_users_file = open('users.json')

    users = json.load(json_users_file)

    columns = ['name', 'city']

    try:
        name_csv_file = "test.csv"

        compression_options = dict(method='zip', archive_name=name_csv_file)

        df = pd.DataFrame(users)

        df.to_csv('course.zip', index=False, columns=columns, compression=compression_options)
    except Exception as e:
        print(str(e))

def bootstrap():
    name = 'igorsilva'

    date = '010223'

    bucket_name = 'my_bucket'

    date_now = datetime.now()

    timestamp = datetime.timestamp(date_now)

    prefix = f"igorsilva/{date}"

    file_upload_name = './upload_file.txt'

    name_zip_base = f"{name}_{date}_{timestamp}.zip"

    name_file_zip = f"./temp_zips/{name_zip_base}"

    zip_file = ZipFile(name_file_zip, 'w')
    zip_file.write(file_upload_name)
    zip_file.close()

    name_zip_to_bucket = f"{name}/{date}/{name_zip_base}"

    upload_to_s3(name_file_zip, bucket_name, name_zip_to_bucket)

    response = list_paginate(bucket_name, prefix)

    print(response)
    
# bootstrap()
send_create_csv()
# list_paginate_object('my_bucket', 'igorsilva/030223')
# create_file_csv_ziped()
# url_object = generate_url('my_bucket', 'igorsilva/030223/courses.zip')
# print(url_object)