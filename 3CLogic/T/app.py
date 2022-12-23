import os

import s3fs

import base64

import boto3

import gzip

import json

import logging

import os

from botocore.exceptions import ClientError



S3_BUCKET = os.environ.get('S3_BUCKET')

S3_STAGING_FOLDER = os.environ.get('S3_STAGING_FOLDER')

S3_PUBLISH_FOLDER = os.environ.get('S3_PUBLISH_FOLDER')

S3_PUBLISH_ARCHIVE_FOLDER = os.environ.get('S3_PUBLISH_ARCHIVE_FOLDER')







logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)



# Archive publish content

def archive_content():

    try:

        print("------------------------------------------")

        s3 = s3fs.S3FileSystem(anon=False)

        s3_bucket_publish_path = S3_BUCKET + '/' + S3_PUBLISH_FOLDER + '/'

        s3_bucket_publish_archive_path = S3_BUCKET + '/' + S3_PUBLISH_ARCHIVE_FOLDER + '/'

        print("S3 Publish bucket path -- {0}".format(s3_bucket_publish_path))

        print("S3 Publish archive bucket path -- {0}".format(s3_bucket_publish_archive_path))

        print("------------------------------------------")

        archive_content_list = s3.find(s3_bucket_publish_path)

        archive_count = 0

        for file in archive_content_list:

            if file.endswith(".csv"):

                print("FileName - {0} is archiving to {1}".format(file, s3_bucket_publish_archive_path))

                s3.mv(file, s3_bucket_publish_archive_path)

                archive_count = archive_count + 1

        print("------------------------------------------")

        if archive_count > 0:

            print("All files archived...")

        else:

            print("No files to archive...")

        print("------------------------------------------")

    except Exception as e:

        print("error occurred - {}".format(e))





# To copy staging content to publish

def move_content_to_publish():

    try:

        print("------------------------------------------")

        s3 = s3fs.S3FileSystem(anon=False)

        s3_bucket_staging_path = S3_BUCKET + '/' + S3_STAGING_FOLDER + '/'

        s3_bucket_publish_path = S3_BUCKET + '/' + S3_PUBLISH_FOLDER + '/'

        print("S3 Staging bucket path -- {0}".format(s3_bucket_staging_path))

        print("S3 Publish bucket path -- {0}".format(s3_bucket_publish_path))

        print("------------------------------------------")

        archive_content_list = s3.find(s3_bucket_staging_path)

        publish_count = 0

        for file in archive_content_list:

            if file.endswith(".csv"):

                print("FileName - {0} is moving to publish folder {1}".format(file, s3_bucket_publish_path))

                s3.cp(file, s3_bucket_publish_path)

                publish_count = publish_count + 1

        print("------------------------------------------")

        if publish_count > 0:

            print("All files copied to publish folder...")

        else:

            print("No files to copy...")

        print("------------------------------------------")

    except Exception as e:

        print("error occurred - {}".format(e))



def logpayload(event):

    #logger.setLevel(logging.DEBUG)

    #logger.debug(event['awslogs']['data'])

    compressed_payload = base64.b64decode(event['awslogs']['data'])

    uncompressed_payload = gzip.decompress(compressed_payload)

    log_payload = json.loads(uncompressed_payload)

    return log_payload





#handler

def staging_to_publish(event,context):

    pload = logpayload(event)

    archive_content()

    move_content_to_publish()

    print("-----3CLogic_T_Ended-----")

    print("Successfull")

    return "Successfull"  
