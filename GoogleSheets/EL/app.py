import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pprint import pprint
from json import encoder
import boto3
from datetime import datetime
import pandas as pd
import os
import logging
import time

# Using creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)


# Variable declaration
bucket = os.environ['bucket']
staging_folder = os.environ['staging_folder']
staging_archive_folder = os.environ['staging_archive_folder']
staging_prefix = os.environ['staging_prefix']
ddb_aws_endpoint = os.environ['aws_url']
groupId = 1


# S3 configuration
s3Resource = boto3.resource('s3')
s3Client = boto3.client('s3')
my_bucket = s3Resource.Bucket(bucket)

# DynamoDB configuration
dynamodb = boto3.resource('dynamodb', endpoint_url=ddb_aws_endpoint)

# logger initialization
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.info("----Started----")


def archive_staging():
    try:
        print("Archiving Staging started...")
        logger.info("Archiving Staging started...")
        for each_object in my_bucket.objects.filter(Prefix=staging_prefix):
            print(each_object)
            object_key = each_object.key
            archive_key = object_key.replace(staging_folder, staging_archive_folder)
            body1 = each_object.get()['Body'].read()
            s3Resource.Object(bucket, archive_key).put(Body=body1)
            each_object.delete()
        logger.info("SUCCESS: Archiving Staging completed.")
        print("SUCCESS: Archiving Staging completed.")
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not archive Staging folder.")
        logger.error(e)


def data_extraction():
    try:
        # Getting sheets' key list to extract data from
        sheetKeys = get_sheets_key('bti_config_google_sheets')
        if len(sheetKeys) > 0:
            for each_key in sheetKeys:
                if each_key['groupId'] == groupId and each_key['status'] == True:
                    # Finding a workbook by key and open the sheet
                    print("Extract and Load for sheet " + each_key['key'] + " started")
                    logger.info("Extract and Load for sheet " + each_key['key'] + " started")
                    sheet = client.open_by_key(each_key['key'])
                    sheetTitle = sheet.title
                    sheetKey = each_key['key']
                    worksheets = sheet.worksheets()
                    current_timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                    for i in range(0,len(worksheets)):
                        currentWorksheet = sheet.get_worksheet(i)
                        currentWorksheetTitle = currentWorksheet.title
                        if currentWorksheet.id in each_key['gIds']:
                            # Extracting all of the values
                            list_of_records = currentWorksheet.get_all_records()
                            df = pd.DataFrame.from_dict(list_of_records)
                            list_of_records = df.to_csv(index=False)
                            # Uploading to s3
                            upload(sheetKey, sheetTitle, currentWorksheetTitle, list_of_records, current_timestamp)
                            time.sleep(1)
                    logger.info("Extract and Load for sheet " + each_key['key'] + " ended")
                    print("Extract and Load for sheet " + each_key['key'] + " ended.")
    except Exception as e:
        logger.error(e)
        print("Error Occurred during data_extraction - {}", e)

            

# Uploading a file to S3 following a defined structure
def upload(sheetKey, sheetName, workSheetTitle, content, timestamp):
    try:
        bucket_name = bucket
        folder_name = staging_prefix + sheetKey + "_" + sheetName + "/" + timestamp
        s3Client.put_object(Bucket=bucket_name, Key=(folder_name + '/'))
        filename = workSheetTitle + "-" + timestamp + ".csv"
        key = folder_name + "/" + filename
        s3Client.put_object(Body=content.encode('utf-8'), Bucket=bucket_name, Key= key)
        print("File Successfully Uploaded at s3://" + bucket_name + "/" + key)
        logger.info("File Successfully Uploaded at s3://" + bucket_name + "/" + key)
    except Exception as e:
        logger.error(e)
        print("Error Occurred during file upload - {}", e)


# Getting config data for sheets to extract and load
def get_sheets_key(tableName):
    table = dynamodb.Table(tableName)
    response = table.scan()
    if response:
        data = response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response['Items'])
        return data
    else:
        return []


# handler
def extraction_load_google_sheets(events, context):
    time.sleep(120)
    archive_staging()
    data_extraction()
    print("Successfull")
    return({"message": "Completed"})

