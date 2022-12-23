
import boto3
import logging
import sys
import pandas as pd
import pymysql
from io import StringIO
import numpy as np
import os
import base64
import json
import gzip
import time
from datetime import datetime
from base64 import b64decode
from botocore.exceptions import ClientError

# Variables initialization
BUCKET = os.environ.get('BUCKET')
PUBLISH_FOLDER = os.environ.get('PUBLISH_FOLDER')
PUBLISH_PREFIX = os.environ.get('PUBLISH_PREFIX')

#RDS settings
HOST = os.environ.get('HOST')
USER_NAME = os.environ.get('USER_NAME')
DB_NAME = os.environ.get('DB_NAME')


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


#  KMS decryption
def kms_decryption(encrypted_data):
    return boto3.client('kms').decrypt(
        CiphertextBlob=b64decode(encrypted_data),
        EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
    )['Plaintext'].decode('utf-8')


# getting kms decrypted values
print("Before password printing")
print(os.environ['PASSWORD'])
print(type(os.environ['PASSWORD']))
PASSWORD = kms_decryption(os.environ['PASSWORD'])
print("---------------------------")
print(PASSWORD)
print("---------------------------")


#Database connection
try:
    mysql_client = pymysql.connect(host=HOST, user=USER_NAME, passwd=PASSWORD, db=DB_NAME, connect_timeout=5)
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

# Get list of files from Staging
def get_files_list_from_publish():
    files_list = []
    try:

        logger.info("Fetching files list from Publish started...")
        objs = list(s3_bucket.objects.filter(Prefix=PUBLISH_PREFIX))
        print("------------------FileList-------------------------")
        for obj in objs:
            files_list.append(obj.key)
            print(obj.key)

        logger.info("SUCCESS: Fetching files list from Publish completed.")
        print("SUCCESS: Fetching files list from Publish completed.")
        #print("File List -- ", files_list)
        print("-------------------------------------------")
        return files_list
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not get files list from Publish.")
        logger.error(e)
        sys.exit()

# Get CSV content of a file
def get_csv_content(bucket, key):
    content_object = s3.Object(bucket, key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(file_content))
    return df.replace({np.nan: None})


# Drop table
def drop_table(table_name):
    try:
        print("--------------------------------------------------------")
        print("drop table name  "+ table_name)
        print("--------------------------------------------------------")
        drop_table_query = "DROP TABLE IF EXISTS " + table_name
        with mysql_client.cursor() as cur:
                cur.execute(drop_table_query)
        mysql_client.commit()
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not drop table {}.".format(table_name))
        logger.error(e)
        sys.exit()

# Create table dynamically from Pandas dataframe
def create_table(table_name, df):
    print("--------------------------------------------------------")
    current_timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    print("-----------------Creating Table-------------------------")
    create_table_query = "CREATE TABLE IF NOT EXISTS " + table_name + " (  "
    try:
        integer_fields = list(df.select_dtypes(['int64']))
        varchar_fields = list(df.select_dtypes(['float64'])) + list(df.select_dtypes(['object']))
        boolean_fields = list(df.select_dtypes(['bool']))

        integer_fields = [field.replace(" ","_").replace(".","_").replace("(","_").replace(")","").replace("-","_").replace("/","or").replace("+","plus").replace("'", "").replace("&", "").replace(":", "") for field in integer_fields]
        varchar_fields = [field.replace(" ","_").replace(".","_").replace("(","_").replace(")","").replace("-","_").replace("/","or").replace("+","plus").replace("'", "").replace("&", "").replace(":", "") for field in varchar_fields]
        boolean_fields = [field.replace(" ","_").replace(".","_").replace("(","_").replace(")","").replace("-","_").replace("/","or").replace("+","plus").replace("'", "").replace("&", "").replace(":", "") for field in boolean_fields]

        #print("--------------------------------------------------------")
        #print("-----------------Integer Fields-------------------------")
        for field in integer_fields:
            create_table_query += field + " bigint, "

        #print("--------------------------------------------------------")
        #print("-----------------Varchar Fields-------------------------")
        for field in varchar_fields:
            create_table_query += field + " text, "

        #print("--------------------------------------------------------")
        #print("-----------------Boolean Fields-------------------------")
        for field in boolean_fields:
            create_table_query += field + " boolean, "

        create_table_query = create_table_query[:-2]
        create_table_query += ");"

        print("------------------------------------------")
        print("Create Table Query -- {0}".format(create_table_query))
        print("------------------------------------------")
        drop_table(table_name)
        with mysql_client.cursor() as cur:
            cur.execute(create_table_query)
        mysql_client.commit()
        return True

    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not create table {}.".format(table_name))
        logger.error(e)
        return False

# Insert data in newly created table dynamically from Pandas dataframe
def insert_data(table_name, df):
    try:
        columns = df.columns.values
        columns_str = ",".join(columns)
        columns_str = columns_str.replace(" ","_").replace(".","_").replace("(","_").replace(")","").replace("-","_").replace("/","or").replace("+","plus").replace("'", "").replace("&", "").replace(":", "")
        placeholders = ",".join(["%s"] * len(columns))
        insert_query = "INSERT INTO " + table_name + " (" + columns_str + ") VALUES (" + placeholders + ");"
        values = df.to_numpy().tolist()


        if len(values) > 0:
            with mysql_client.cursor() as cur:
                cur.executemany(insert_query, values)
            mysql_client.commit()
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not insert data into table {}.".format(table_name))
        logger.error(e)


# Load data to Aurora from one CSV
def load_to_aurora(s3_file_path):
    try:
        if s3_file_path.split('/')[-1].split('.')[-1] == "csv":
            df = get_csv_content(BUCKET, s3_file_path)
            print("----------------------------------------------")
            print("S3 file Path")
            print(s3_file_path)
            print("----------------------------------------------")
            tt=str(s3_file_path)
            #file_name1 = ( tt[ (tt.rfind("/")+1) :tt.rfind("-")]  )
            #file_name1 = file_name1.replace(" ","_")

            file_name1 = ( tt[ (tt.rfind("/")+1) :tt.rfind("-")]  )
            file_name1 = file_name1[:file_name1.find("-")]
            file_name1 = file_name1.replace(" ","_")

            #global TABLE_NAME
            #TABLE_NAME1 = file_name1
            print("TABLE_NAME : "+file_name1)
            create_table_status = create_table(file_name1, df)
            if create_table_status:
               insert_data(file_name1, df)
               print("--------------------Success inserting data --------------------------  "+ file_name1 )
            else:
                print("ERROR: Data are not inserted to the table as table is not created")
                logger.error("ERROR: Data are not inserted to the table as table is not created")
    except pd.errors.EmptyDataError:
            print('Note: {} is empty. Skipping.'.format(s3_file_path))
            logger.info('Note: {} is empty. Skipping.'.format(s3_file_path))
    except Exception as e:
        print("ERROR: Unexpected error: Could not load `{}` to Aurora.".format(s3_file_path))
        logger.error("ERROR: Unexpected error: Could not load `{}` to Aurora.".format(s3_file_path))
        logger.error(e)


# Load data to Aurora from all CSVs
def publish_to_aurora():
    files_list = get_files_list_from_publish()
    #drop_table(TABLE_NAME)

    print("------------------------------------------")
    print("Loading CSV to Aurora Started...")
    logger.info("Loading CSV to Aurora Started...")

    for file in files_list:
        load_to_aurora(file)
        print()
        print()

    logger.info("SUCCESS: Loading CSV to Aurora Completed.")
    print("SUCCESS: Loading CSV to Aurora Completed.")
    print("------------------------------------------")

def logpayload(event):
    compressed_payload = base64.b64decode(event['awslogs']['data'])
    uncompressed_payload = gzip.decompress(compressed_payload)
    log_payload = json.loads(uncompressed_payload)
    return log_payload


# Request Handler
def handler(event, context):
    #time.sleep(120)
    #pload = logpayload(event)
    publish_to_aurora()
    logger.info("------- Ended InfusionSoft_HO_Aurora_Staging -------")
    print("------- Ended InfusionSoft_HO_Aurora_Staging -------")
    print("Successfull")
    return "Successfull"


