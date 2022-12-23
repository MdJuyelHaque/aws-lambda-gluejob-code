import boto3
import logging
import sys
import pandas
import json
import datetime
from io import StringIO
import os

# # Variables initialization
BUCKET = os.environ.get('BUCKET')
STAGING_FOLDER = os.environ.get('STAGING_FOLDER')
PUBLISH_FOLDER = os.environ.get('PUBLISH_FOLDER')
PUBLISH_ARCHIVE_FOLDER = os.environ.get('PUBLISH_ARCHIVE_FOLDER')
STAGING_PREFIX = os.environ.get('STAGING_PREFIX')
PUBLISH_PREFIX = os.environ.get('PUBLISH_PREFIX')


# logger initialization
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("----Started----")

# Boto3 resource 
try:
    s3 = boto3.resource('s3')
    s3_bucket = s3.Bucket(BUCKET)
except Exception as e:
    logger.error("ERROR: Unexpected error: Could not connect to S3.")
    logger.error(e)
    sys.exit()   

# Get current Date & Time
def get_current_date_time():
    today = datetime.datetime.now()
    today_date = today.strftime("%Y-%m-%d")
    today_date_time = today.strftime("%Y-%m-%d-%H-%M-%S")
    return today_date,today_date_time

# Get list of files from Staging
def get_files_list_from_staging():
    files_list = []
    try:
        print("Fetching files list from Staging started...")
        logger.info("Fetching files list from Staging started...")
        objs = list(s3_bucket.objects.filter(Prefix=STAGING_PREFIX))
        for obj in objs:
            files_list.append(obj.key)
        logger.info("SUCCESS: Fetching files list from Staging completed.")
        print("SUCCESS: Fetching files list from Staging completed.")
        return files_list
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not get files list from Staging.")
        logger.error(e)
        sys.exit()

# Get Json content of a file
def get_json_content(bucket, key):
    content_object = s3.Object(bucket, key)
    file_content = content_object.get()['Body'].read().decode('utf-8').replace(";","")
    return json.loads(file_content)


def json_to_dataframe(data):
    return pandas.DataFrame(pandas.json_normalize(data))


# ------------------End of Transformation code------------------

# Create Equivalent CSV of a JSON(staging) in Publish folder  
def json_to_csv(s3_file_path):
    try:
        today_str,today_date_time = get_current_date_time()
        if s3_file_path.split('/')[-1].split('.')[-1] == "json" :
            print(s3_file_path)
            json_content = get_json_content(BUCKET, s3_file_path)

            df = json_to_dataframe(json_content)

            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_name = s3_file_path.split("/")[-1].split("-")[0] + "-" + today_date_time + ".csv"
            publish_path = s3_file_path.replace(STAGING_FOLDER, PUBLISH_FOLDER).rsplit('/', 2)[0]
            csv_path = publish_path + "/" + today_date_time + "/" + csv_name
            s3.Object(BUCKET, csv_path).put(Body=csv_buffer.getvalue())
    except Exception as e:
        print("ERROR: Unexpected error: Could not transform `{}` to CSV.".format(s3_file_path))
        logger.error("ERROR: Unexpected error: Could not transform `{}` to CSV.".format(s3_file_path))
        logger.error(e)


# Extract JSONs from Staging, Transform to CSV, Load in Publish
def load_to_publish():
    files_list = get_files_list_from_staging()
    try:
        print("Transforming JSON to CSV & Loading to Publish Started...")
        logger.info("Transforming JSON to CSV & Loading to Publish Started...")
        for file in files_list:
            json_to_csv(file)
        logger.info("SUCCESS: Transforming JSON to CSV & Loading to Publish Completed.")
        print("SUCCESS: Transforming JSON to CSV & Loading to Publish Completed.")
    except Exception as e:
        logger.error("ERROR: Unexpected error: Staging to Publish failed.")
        logger.error(e)

# Moves files / folders from publish to publish_archive 
def archive_publish():
    try:
        print("Archiving Publish started...")
        logger.info("Archiving Publish started...")
        for each_object in s3_bucket.objects.filter(Prefix=PUBLISH_PREFIX):
            print(each_object)
            object_key = each_object.key
            archive_key = object_key.replace(PUBLISH_FOLDER, PUBLISH_ARCHIVE_FOLDER)
            body1 = each_object.get()['Body'].read()
            s3.Object(BUCKET, archive_key).put(Body=body1)
            each_object.delete()
        logger.info("SUCCESS: Archiving Publish completed.")
        print("SUCCESS: Archiving Publish completed.")
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not archive Publish folder.")
        logger.error(e)
        sys.exit()
        
        
# handler
def T_Saved_Report(events, context):
    archive_publish()
    load_to_publish()
    logger.info("-------------Ended------------")
    return "Successful"


