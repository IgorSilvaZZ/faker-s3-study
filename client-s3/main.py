import os
from datetime import datetime
import random
from zipfile import ZipFile

from s3_services import create_bucket, list_buckets, upload_to_s3, list_objects, list_paginate_objects, generate_url


def upload_archive_to_s3(file_name_to_upload, key):
    print('Upload archive in bucket...')

    upload_to_s3(file_name_to_upload, 'my_bucket', key)

    print('Upload is finish!')


def create_local_archive_folder(name_line, date_formatted, path_tmp, path_for_bucket):
    date_now = datetime.now()

    timestamp_name_path = datetime.timestamp(date_now)

    path_new_archive = f"{path_tmp}/{timestamp_name_path}.txt"

    new_file = open(path_new_archive, 'w')
    new_file.write(str(random.randint(1, 100000)))
    new_file.close()

    name_new_file_zip = f"./temp_zips/{name_line}_{date_formatted}.zip"

    zip_file = ZipFile(name_new_file_zip, 'w')
    zip_file.write(path_new_archive, os.path.basename(path_new_archive))
    zip_file.close()

    name_zip_file_for_bucket = f"{path_for_bucket}/{name_line}_{date_formatted}.zip"

    upload_archive_to_s3(name_new_file_zip, name_zip_file_for_bucket)


def create_directory_temp(lines, date):
    for line in lines:

        path_tmp = f"temp/{line}"

        if not os.path.exists(path_tmp):
            os.makedirs(path_tmp)

        path_for_bucket = f"{line}/{date}"

        create_local_archive_folder(line, date, path_tmp, path_for_bucket)


def read_file():
    archive = open('list_names.txt', 'r')

    lines = []

    for file in archive:
        lines.append(file.strip().lower().replace(' ', ''))

    archive.close()

    return lines

def create_request(date):
    response = read_file()

    create_directory_temp(response, date)

def bootstrap():

    # create_bucket('my_bucket')

    # list_buckets()

    date = "010223"

    dates = ['020223', '010223']

    name = "igorsilva"

    expiration_url = 3600

    # create_request(date)

    # prefix = f"igorsilva/{date}"

    response_obj = []

    for date in dates:

        prefix = f"{name}/{date}"
        
        files_paginate = list_paginate_objects('my_bucket', prefix)

        for file in files_paginate:
            key = file['Key']

            url = generate_url('my_bucket', key, expiration_url)

            name_file = os.path.basename(key)

            new_response_object = { 'name': name_file, 'url': url }

            response_obj.append(new_response_object)
    
    print(response_obj)


bootstrap()
