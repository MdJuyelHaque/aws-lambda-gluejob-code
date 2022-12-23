import boto3
import logging
import sys
import pandas
from copy import deepcopy
import json
import datetime
from io import StringIO
import os
import gzip
import base64
import time
from botocore.exceptions import ClientError


# Variables initialization
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

#------------------Transform JSON to Pandas Dataframe------------------
def cross_join(left, right):
    new_rows = [] if right else left
    for left_row in left:
        for right_row in right:
            temp_row = deepcopy(left_row)
            for key, value in right_row.items():
                temp_row[key] = value
            new_rows.append(deepcopy(temp_row))
    return new_rows

def flatten_list(data):
    for elem in data:
        if isinstance(elem, list):
            yield from flatten_list(elem)
        else:
            yield elem

def flatten_json(data, prev_heading=''):
        if isinstance(data, dict):
            rows = [{}]
            for key, value in data.items():
                rows = cross_join(rows, flatten_json(value, prev_heading + '_' + key))
        elif isinstance(data, list):
            rows = []
            for i in range(len(data)):
                [rows.append(elem) for elem in flatten_list(flatten_json(data[i], prev_heading))]
        else:
            rows = [{prev_heading[1:]: data}]
        return rows

def json_to_dataframe(data):
    return pandas.DataFrame(flatten_json(data))


def nested_csv_to_csv(dataframe):
    #data = read_json(filename="transactions-2021-10-26-08-16-11.json")
    #dataframe = create_dataframe(data=data)
    X = dataframe

    # Get columns containing nested data ie, list containg dictonary data
    nestedColumnList = []
    for column in X:
        for each_row in range(0, X[column].size):
            if isinstance(X[column][each_row], list):
                # list should be not be empty and must contain dictionary data inside the list
                if (len(X[column][each_row]) != 0) and type(X[column][each_row][0]) == dict:
                    nestedColumnList.append(column)
                    break

    nested_columns = nestedColumnList
    l1=[]
    # if nested_column list is not empty
    if len(nested_columns) != 0:
        for nested_column_name in nested_columns:
            # row count of individual nested column
            length_row = X[nested_column_name].size
            keylist = []
            # Get the keys of dictionary from the first non zero element from the dict> cell(object) >nested data column
            for each_row in range(0, length_row):
                if isinstance(X[nested_column_name][each_row], list):
                    if (len(X[nested_column_name][each_row]) != 0) and type(X[nested_column_name][each_row][0]) == dict:
                        keylist = list(X[nested_column_name][each_row][0].keys())
                        break
            # iterated through each row of nested_column_name
            for each_row in range(0, length_row):
                if isinstance(X[nested_column_name][each_row], list):
                    # count of dictionaries inside a list
                    dictonaryCount = len(X[nested_column_name][each_row])
                    for j in range(dictonaryCount):
                        for k in keylist:
                            # A. when data inside cell doesnt contain list or dictionary data.. [{},...] , for normal data
                            if type(X[nested_column_name][each_row][j][k]) != dict and type(
                                    X[nested_column_name][each_row][j][k]) != list:
                                if j==0 and each_row==0:
                                    l1.append(k)

                                if X[nested_column_name][each_row] != []:
                                    X.loc[each_row, str(nested_column_name) + '.' + str(k) + '.' + str(j)] = \
                                        X[nested_column_name][each_row][j][k]
                                else:
                                    X.loc[
                                        each_row, str(nested_column_name) + '.' + str(k) + '.' + str(j)] = numpy.nan
                            # A.1 when dictionary is present as key value for the dictionary inside list ..[{{}},...]
                            elif type(X[nested_column_name][each_row][j][k]) == dict:
                                #if j==0 and each_row==0:
                                #    l2.append(k)
                                dictonaryCount2 = len(X[nested_column_name][each_row][j][k])
                                keylist2 = X[nested_column_name][each_row][j][k].keys()
                                for k2 in keylist2:
                                    X.loc[each_row, str(nested_column_name) + '.' + str(k) + '.' + str(k2) + '.' + str(
                                        j)] = \
                                        X[nested_column_name][each_row][j][k][k2]
                            # when list is present as key value for the dictionary inside list  ..[{[]}, ..]
                            elif type(X[nested_column_name][each_row][j][k]) == list:
                                dictonaryCount3 = len(X[nested_column_name][each_row][j][k])
                                keylist3 = X[nested_column_name][each_row][j][k][0].keys()
                                for j3 in range(dictonaryCount3):
                                    for k3 in keylist3:
                                        if type(X[nested_column_name][each_row][j][k][j3][k3]) != dict and type(
                                                X[nested_column_name][each_row][j][k][j3][k3]) != list:
                                            X.loc[each_row, str(nested_column_name) + '.' + str(k) + '.' + str(
                                                k3) + '.' + str(j)] = \
                                                X[nested_column_name][each_row][j][k][j3][k3]
                                        # [{[{}]}]
                                        elif type(X[nested_column_name][each_row][j][k][j3][k3]) == dict:
                                            # if j==0 and each_row==0:
                                            #    l2.append(k)

                                            dictonaryCount2 = len(X[nested_column_name][each_row][j][k][j3][k3])
                                            keylist4 = X[nested_column_name][each_row][j][k][j3][k3].keys()
                                            # for j2 in range(dictonaryCount2):
                                            for k4 in keylist4:
                                                X.loc[each_row, str(nested_column_name) + '.' + str(k) + '.'+str(k3)+'.'+ str(
                                                    k4) + '.' + str(j3)] = \
                                                    X[nested_column_name][each_row][j][k][j3][k3][k4]


    X = X.drop(nested_columns, axis = 1)
    return X


#Transform countries JSON seperately
def json_to_dataframe_countries(data):
    data = data[0]
    country_codes = [country_code for country_code in data]
    country_names = [data[country_code] for country_code in data]
    return pandas.DataFrame({"country_code":country_codes, "country_name": country_names})        

# ------------------End of Transformation code------------------

# Create Equivalent CSV of a JSON(staging) in Publish folder  
def json_to_csv(s3_file_path):
    try:
        today_str,today_date_time = get_current_date_time()
        if s3_file_path.split('/')[-1].split('.')[-1] == "json" and today_str in s3_file_path:
            json_content = get_json_content(BUCKET, s3_file_path)
             
            if "countries" in s3_file_path.split('/')[-1]:
                df = json_to_dataframe_countries(json_content)
            else:
                df = json_to_dataframe(json_content)
                df = nested_csv_to_csv(df)

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


def logpayload(event):
    compressed_payload = base64.b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    log_payload = json.loads(uncompressed_payload)
    return log_payload


# handler
def handler(event, context):
    time.sleep(120)
    #pload = logpayload(event)
    archive_publish()
    load_to_publish()
    #logger.info("-------------Ended------------")
    logger.info("------- Ended InfusionSoft_ZU_T Test -------")
    print("------- Ended InfusionSoft_ZU_T Test-------")
    print("Successfull")
    return "Successfull"



