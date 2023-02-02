import os
from datetime import datetime
import random

from s3_services import create_bucket, list_buckets, upload_to_s3, list_objects


def request_archive():
    archive = open('list_names.txt', 'r')

    lines = []

    for file in archive:
        lines.append(file.strip().lower().replace(' ', ''))

    archive.close()

    return lines


def upload_archive_to_s3(file_name_to_upload, key):
    print('Upload archive in bucket...')

    upload_to_s3(file_name_to_upload, 'my_bucket', key)

    print('Upload is finish!')


def create_local_archive_folder(path):
    date_now = datetime.now()

    timestamp_name_path = datetime.timestamp(date_now)

    path_new_archive = f"{path}/{timestamp_name_path}.txt"

    name_new_achive = f"{timestamp_name_path}.txt"

    new_file = open(path_new_archive, 'w')
    new_file.write(str(random.randint(1, 100000)))
    new_file.close()

    upload_archive_to_s3(path_new_archive, name_new_achive)


def create_directory_temp(lines):
    for line in lines:
        path_tmp = f"temp/{line}"

        if not os.path.exists(path_tmp):
            os.makedirs(path_tmp)

        create_local_archive_folder(path_tmp)


def create_request():
    response = request_archive()

    create_directory_temp(response)


def bootstrap():

    # create_bucket('my_bucket')

    files_to_s3 = list_objects('my_bucket')

    for response_file in files_to_s3:

        if 'Contents' == response_file:
            print(files_to_s3[response_file])


bootstrap()
