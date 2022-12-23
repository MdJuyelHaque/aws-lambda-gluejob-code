
import boto3
import logging
import sys
import pandas as pd
import pymysql
from io import StringIO
import numpy as np
import os
import base64
from base64 import b64decode
import gzip
import json

# # Variables initialization
BUCKET = os.environ.get('BUCKET')
PUBLISH_FOLDER = os.environ.get('PUBLISH_FOLDER')
PUBLISH_PREFIX = os.environ.get('PUBLISH_PREFIX')


# #RDS settings
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


# # getting kms decrypted values
PASSWORD = kms_decryption(os.environ['PASSWORD'])

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
        print("Fetching files list from Publish started...")
        logger.info("Fetching files list from Publish started...")
        objs = list(s3_bucket.objects.filter(Prefix=PUBLISH_PREFIX))
        for obj in objs:
            files_list.append(obj.key)

        logger.info("SUCCESS: Fetching files list from Publish completed.")
        print("SUCCESS: Fetching files list from Publish completed.")
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
    create_table_query = "CREATE TABLE IF NOT EXISTS " + table_name + " ("
    try:
        integer_fields = list(df.select_dtypes(['int64']))
        varchar_fields = list(df.select_dtypes(['float64'])) + list(df.select_dtypes(['object']))
        boolean_fields = list(df.select_dtypes(['bool']))

        integer_fields = [field.replace(" ","_").replace(".","_").replace("(","_").replace(")","").replace("-","_") for field in integer_fields]
        varchar_fields = [field.replace(" ","_").replace(".","_").replace("(","_").replace(")","").replace("-","_") for field in varchar_fields]
        boolean_fields = [field.replace(" ","_").replace(".","_").replace("(","_").replace(")","").replace("-","_") for field in boolean_fields]

        for field in integer_fields:
            create_table_query += field + " bigint, "

        for field in varchar_fields:
            create_table_query += field + " varchar(255), "
        
        for field in boolean_fields:
            create_table_query += field + " boolean, "
        
        create_table_query = create_table_query[:-2]
        create_table_query += ");"

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
        columns_str = columns_str.replace(" ","_").replace(".","_").replace("(","_").replace(")","").replace("-","_")
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
            TABLE_NAME = (s3_file_path.split('/')[2]).replace(" ","")
            drop_table(TABLE_NAME)
            create_table_status = create_table(TABLE_NAME, df)
            if create_table_status:
               insert_data(TABLE_NAME, df)
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
    
    
    print("Loading CSV to Aurora Started...")
    logger.info("Loading CSV to Aurora Started...")

    for file in files_list:
        load_to_aurora(file)
    
    logger.info("SUCCESS: Loading CSV to Aurora Completed.")
    print("SUCCESS: Loading CSV to Aurora Completed.")


# Request Handler
def handler(event, context):
    # pload=logpayload(event)
    publish_to_aurora()
    logger.info("----Ended----")
    print("-----SavedReport_to_Aurora_Staging_Ended-----")
    #user = gluejob(event)
    print("Successfull")
    return "Successful"

