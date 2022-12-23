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

############ For Local
#BUCKET = "dummy-bti-dl"
#STAGING_FOLDER = "staging"
#PUBLISH_FOLDER = "publish"
#PUBLISH_ARCHIVE_FOLDER = "publish_archive"
#STAGING_PREFIX = "bti_infusionSoft/staging/"
#PUBLISH_PREFIX = "bti_infusionSoft/publish/"


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
    
    
def json_to_dataframe_contacts(data: list) -> pandas.DataFrame:
    dataframe = pandas.DataFrame()
    # Looping through each record
    for d in data:
        # Normalize the column levels
        record = pandas.json_normalize(d)
        # Append it to the dataframe
        dataframe = dataframe.append(record, ignore_index=True)
    return dataframe
    
def nested_csv_to_csv(dataframe):
    contact_dataframe = dataframe

    # Get columns containing nested data ie, list containg dictonary data
    nestedColumnList = []
    for column in contact_dataframe:
        for each_row in range(0, contact_dataframe[column].size):
            if isinstance(contact_dataframe[column][each_row], list):
                # list should be not be empty and must contain dictionary data inside the list
                if (len(contact_dataframe[column][each_row]) != 0) and type(contact_dataframe[column][each_row][0]) == dict:
                    nestedColumnList.append(column)
                    break

    nested_columns = nestedColumnList
    l1=[]
    # if nested_column list is not empty
    if len(nested_columns) != 0:
        for nested_column_name in nested_columns:
            # row count of individual nested column
            length_row = contact_dataframe[nested_column_name].size
            keylist = []
            # Get the keys of dictionary from the first non zero element from the dict> cell(object) >nested data column
            for each_row in range(0, length_row):
                if isinstance(contact_dataframe[nested_column_name][each_row], list):
                    if (len(contact_dataframe[nested_column_name][each_row]) != 0) and type(contact_dataframe[nested_column_name][each_row][0]) == dict:
                        keylist = list(contact_dataframe[nested_column_name][each_row][0].keys())
                        break
            # iterated through each row of nested_column_name
            for each_row in range(0, length_row):
                if isinstance(contact_dataframe[nested_column_name][each_row], list):
                    # count of dictionaries inside a list
                    dictonaryCount = len(contact_dataframe[nested_column_name][each_row])
                    for j in range(dictonaryCount):
                        for k in keylist:
                            # A. when data inside cell doesnt contain list or dictionary data.. [{},...] , for normal data
                            if type(contact_dataframe[nested_column_name][each_row][j][k]) != dict and type(
                                    contact_dataframe[nested_column_name][each_row][j][k]) != list:
                                if j==0 and each_row==0:
                                    l1.append(k)

                                if contact_dataframe[nested_column_name][each_row] != []:
                                    contact_dataframe.loc[each_row, str(nested_column_name) + '_' + str(k) + '_' + str(j)] = \
                                        contact_dataframe[nested_column_name][each_row][j][k]
                                else:
                                    contact_dataframe.loc[
                                        each_row, str(nested_column_name) + '_' + str(k) + '_' + str(j)] = numpy.nan
                            # A.1 when dictionary is present as key value for the dictionary inside list ..[{{}},...]
                            elif type(contact_dataframe[nested_column_name][each_row][j][k]) == dict:
                                #if j==0 and each_row==0:
                                #    l2.append(k)
                                dictonaryCount2 = len(contact_dataframe[nested_column_name][each_row][j][k])
                                keylist2 = contact_dataframe[nested_column_name][each_row][j][k].keys()
                                for k2 in keylist2:
                                    contact_dataframe.loc[each_row, str(nested_column_name) + '_' + str(k) + '_' + str(k2) + '_' + str(
                                        j)] = \
                                        contact_dataframe[nested_column_name][each_row][j][k][k2]
                            # when list is present as key value for the dictionary inside list  ..[{[]}, ..]
                            elif type(contact_dataframe[nested_column_name][each_row][j][k]) == list:
                                dictonaryCount3 = len(contact_dataframe[nested_column_name][each_row][j][k])
                                keylist3 = contact_dataframe[nested_column_name][each_row][j][k][0].keys()
                                for j3 in range(dictonaryCount3):
                                    for k3 in keylist3:
                                        if type(contact_dataframe[nested_column_name][each_row][j][k][j3][k3]) != dict and type(
                                                contact_dataframe[nested_column_name][each_row][j][k][j3][k3]) != list:
                                            contact_dataframe.loc[each_row, str(nested_column_name) + '_' + str(k) + '_' + str(
                                                k3) + '_' + str(j)] = \
                                                contact_dataframe[nested_column_name][each_row][j][k][j3][k3]
                                        # [{[{}]}]
                                        elif type(contact_dataframe[nested_column_name][each_row][j][k][j3][k3]) == dict:
                                            # if j==0 and each_row==0:
                                            #    l2.append(k)

                                            dictonaryCount2 = len(contact_dataframe[nested_column_name][each_row][j][k][j3][k3])
                                            keylist4 = contact_dataframe[nested_column_name][each_row][j][k][j3][k3].keys()
                                            # for j2 in range(dictonaryCount2):
                                            for k4 in keylist4:
                                                contact_dataframe.loc[each_row, str(nested_column_name) + '_' + str(k) + '_'+str(k3)+'_'+ str(
                                                    k4) + '_' + str(j3)] = \
                                                    contact_dataframe[nested_column_name][each_row][j][k][j3][k3][k4]

    
    contact_dataframe = contact_dataframe.drop(nested_columns, axis = 1)
    
    #top_columns = []
    #not_more_than_hunderd=0
    #for columns in contact_dataframe:
    #    top_columns.append(columns)
    #    not_more_than_hunderd = not_more_than_hunderd + 1
    #    if not_more_than_hunderd >= 100:
    #        break;
    #contact_dataframe = contact_dataframe[top_columns]
    contact_dataframe = contact_dataframe[["email_opted_in",  
"last_updated",  
"tag_ids",  
"date_created",  
"given_name",  
"last_updated_utc_millis",  
"id",  
"email_addresses_email_0",  
"email_addresses_field_0",  
"custom_fields_id_0",  
"custom_fields_id_1",  
"custom_fields_id_2",  
"custom_fields_id_3",  
"custom_fields_id_4",  
"custom_fields_id_5",  
"custom_fields_id_6",  
"custom_fields_id_7",  
"custom_fields_id_8",  
"custom_fields_id_9",  
"custom_fields_id_10",  
"custom_fields_id_11",  
"custom_fields_id_12",  
"custom_fields_id_13",  
"custom_fields_id_14",  
"custom_fields_id_15",  
"custom_fields_id_16",  
"custom_fields_id_17",  
"custom_fields_id_18",  
"custom_fields_id_19",  
"custom_fields_id_20",  
"custom_fields_id_21",  
"custom_fields_id_22",  
"custom_fields_id_23",  
"custom_fields_id_24",  
"custom_fields_id_25",  
"custom_fields_id_26",  
"custom_fields_id_27",  
"custom_fields_id_28",  
"custom_fields_id_29",  
"custom_fields_id_30",  
"custom_fields_id_31",  
"custom_fields_id_32",  
"custom_fields_id_33",  
"custom_fields_id_34",  
"custom_fields_id_35",  
"custom_fields_id_36",  
"custom_fields_id_37",  
"custom_fields_id_38",  
"custom_fields_id_39",  
"custom_fields_id_40",  
"custom_fields_id_41",  
"custom_fields_id_42",  
"custom_fields_id_43",  
"custom_fields_id_44",  
"custom_fields_id_45",  
"custom_fields_id_46",  
"custom_fields_id_47",  
"custom_fields_id_48",  
"custom_fields_id_49",  
"custom_fields_id_50",  
"custom_fields_id_51",  
"custom_fields_id_52",  
"custom_fields_id_53",  
"custom_fields_id_54",  
"custom_fields_id_55",  
"custom_fields_id_56",  
"custom_fields_id_57",  
"custom_fields_id_58",  
"custom_fields_id_59",  
"custom_fields_id_60",  
"custom_fields_id_61",   
"custom_fields_id_62",   
"custom_fields_id_63",   
"custom_fields_id_64",    
"custom_fields_id_65",  
"custom_fields_id_66",  
"custom_fields_id_67",    
"custom_fields_id_68",   
"custom_fields_id_69",  
"custom_fields_id_70",  
"custom_fields_id_71",  
"custom_fields_id_72",  
"custom_fields_id_73",  
"custom_fields_id_74",  
"custom_fields_id_75",  
"custom_fields_id_76",  
"custom_fields_id_77",  
"custom_fields_id_78",  
"custom_fields_id_79",  
"custom_fields_id_80",  
"custom_fields_id_81",  
"custom_fields_id_82",  
"custom_fields_id_83",  
"custom_fields_id_84",  
"custom_fields_id_85",  
"custom_fields_id_86",  
"custom_fields_id_87",  
"custom_fields_id_88",  
"custom_fields_id_89",  
"custom_fields_id_84"]]
    
    return contact_dataframe    

        
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
             
            #if "countries" in s3_file_path.split('/')[-1]:
                #df = json_to_dataframe_countries(json_content)
               
            if "contacts" in s3_file_path.split('/')[-1]:
                df = json_to_dataframe_contacts(json_content)
                df = nested_csv_to_csv(df)
               
            #else:
               #df = json_to_dataframe(json_content)

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
def handler_ho(event, context):
    time.sleep(120)
    pload = logpayload(event)
    #archive_publish()
    load_to_publish()
    #logger.info("-------------Ended------------")
    logger.info("------- Ended InfusionSoft_HO_T -------")
    print("------- Ended InfusionSoft_HO_T -------")
    print("Successfull")
    return "Successfull"




