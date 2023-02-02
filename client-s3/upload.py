import os
from datetime import datetime
from zipfile import ZipFile

from s3_services import verify_exists_object, upload_to_s3, list_paginate_objects, generate_url

def verify_file_exists_bucket(bucket, key):
    return verify_exists_object(bucket, key)

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

    files_paginate = list_paginate_objects(bucket_name, prefix)

    response_obj = []

    for file in files_paginate:
        key = file['Key']

        url = generate_url(bucket_name, key)

        name_file = os.path.basename(key)

        new_response_object = { 'name': name_file, 'url': url }

        response_obj.append(new_response_object)

    print(response_obj)
    
    
bootstrap()