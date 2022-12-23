from json import encoder
import os
from pprint import pprint

from requests import api
import boto3
from boto3.dynamodb.conditions import Key
from boto3.session import Session
from cryptography import fernet
from cryptography.fernet import Fernet
import json
import requests
from datetime import datetime, timedelta
import pandas as pd
import base64
import time
from decimal import Decimal
import uuid
import logging
from base64 import b64decode
from infusionsoft.library import InfusionsoftOAuth

encryp_key = os.environ['encryption_secret_key']
host = os.environ['host']
ddb_aws_endpoint = os.environ['aws_url']
bucket_name = os.environ['bucket']
groupId = 3

fernet = Fernet(encryp_key.encode('utf-8'))
dynamodb = boto3.resource('dynamodb', endpoint_url=ddb_aws_endpoint)
s3Resource = boto3.resource('s3')
s3Client = boto3.client('s3')
retryCount = 2
apiLog = []

# logger initialization
logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("----Started----")
print("----Started----")


#  KMS decryption
def kms_decryption(encrypted_data):
    return boto3.client('kms').decrypt(
        CiphertextBlob=b64decode(encrypted_data),
        EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
    )['Plaintext'].decode('utf-8')


# getting kms decrypted values
client_id = kms_decryption(os.environ['client_id'])
client_secret = kms_decryption(os.environ['client_secret'])


# Decrypting an encrypted value using a key
def decrypt(encryptedData):
    decryptedData = fernet.decrypt(encryptedData).decode()
    return decryptedData


# Encrypting a decrypted value using a key
def encrypt(decryptedData):
    encryptedData = fernet.encrypt(decryptedData.encode('utf-8'))
    return encryptedData

    
# since and until information for logging
def since_until_generator(api, data):
    object_to_return = {}
    date_today = datetime.today()
    previous_day = date_today - timedelta(days=1)
    object_to_return['since'] = previous_day.strftime('%Y-%m-%dT00:00:00.00Z')
    object_to_return['until'] = date_today.strftime('%Y-%m-%dT00:00:00.00Z')

    return object_to_return


# Getting acces_token and refresh_token value from file in s3 bucket
def authenticate():
    obj = s3Resource.Object('conf-token', "access-token-infusionSoft")
    body = obj.get()['Body'].read()
    decryptedData = json.loads(decrypt(body))
    accessToken = decryptedData['access_token']
    print(accessToken)
    refreshToken = decryptedData['refresh_token']
    data_extraction(accessToken, refreshToken, False)


# Data extraction of different api for authorized requests
def data_extraction(accessToken, refreshToken, isError):
    try:
        jobHistory = get_data_dynamoDB('bti_job_history_InfusionSoft')
        # If function is called for the first time or after refreshing a token
        if not isError:
            apiDetails = get_data_dynamoDB('bti_job_config_InfusionSoft')
        else:
            # If function is called for the data extraction of failed apis requests
            apiDetails = apiLog
        start = time.time()
        for each_element in apiDetails:
            logger.info("--------- Extraction and load for api " + each_element['api'] + " started")
            print("--------- Extraction and load for api " + each_element['api'] + " started")
            api_failed_status = False
            api_status = False
            rows_extracted = 0
            if 'status' in each_element:
                if each_element['groupId'] == groupId:
                    api_status = each_element['status']
            else:
                if each_element['isExecuted'] == 'no':
                    api_failed_status = True

            if api_status or api_failed_status:
                time_obj = since_until_generator(each_element['api'], jobHistory)
                infusionsoft = InfusionsoftOAuth(accessToken)
                date_today = datetime.today()
                previous_day = date_today - timedelta(days=1)
                previous_day = previous_day.strftime('%Y-%m-%d')
                table = 'Payment'
                returnFields = [
                'ChargeId'	
                ,'Commission'	
                ,'ContactId'
                ,'Id'
                ,'InvoiceId'
                ,'LastUpdated'
                ,'PayAmt'
                ,'PayDate'	
                ,'PayNote'	
                ,'PayType'
                ,'RefundId'
                ,'Synced'
                ,'UserId'
                ]
                from_date = '~>=~'+ previous_day
                query = {'LastUpdated' : from_date}
                limit = 1000
                page = 0


                payments_data = ["data"]
                data_to_upload = []
                while len(payments_data) != 0:
                    payments_data = ( infusionsoft.DataService('query', table, limit, page, query, returnFields ) )
                    if((str(payments_data).find("401 Unauthorized"))>=0): 
                        refresh_token(refreshToken)
                        break
                    if((str(payments_data).find("401 Unauthorized"))==0 and (str(payments_data).find("Error"))>=0):
                        log_data_creator(apiLog, start, each_element['api'], each_element['endpoint'], rows_extracted , time_obj, False)
                        break
                    page += 1
                    data_to_upload += payments_data
                current_timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                upload(each_element['api'], json.dumps(payments_data), current_timestamp, "json")
                logger.info("--------- Extraction and load for api " + each_element['api'] + " ended")
                print("--------- Extraction and load for api " + each_element['api'] + " ended")
                log_data_creator(apiLog, start, each_element['api'], each_element['endpoint'], time_obj, rows_extracted , True)
                count = 0
                for each_log in apiLog:
                    if each_log['isExecuted'] != 'yes' and each_log['retry_count'] < retryCount:
                        data_extraction(accessToken, refreshToken, True)
                        count += 1
                if count == 0:
                    print("----- Extraction and load done for all listed apis --------------")
                    logger.info("----- Extraction and load done for all listed apis --------------")
                    log_generator(apiLog)

    except Exception as e:
        logger.error("Error Occurred during data_extraction - {}", e)
        print("Error Occurred during data_extraction - {}", e)


# Refreshing a token in case it is expired
def refresh_token(refreshToken):
    try:
        print("-------Refreshing Token Started-----------")
        logger.info("-------Refreshing Token Started-----------")
        url = "https://api.infusionsoft.com/token"
        payload = 'grant_type=refresh_token&refresh_token=' + refreshToken
        authString = (client_id + ':' + client_secret).encode('ascii')
        base64_bytes = base64.b64encode(authString)
        base64_string = base64_bytes.decode("ascii")
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': "Basic " + base64_string
        }
        response = requests.request("POST", url, headers=headers, data=payload)
        response = response.json()
        dataToEncrypt = {
            "access_token": response['access_token'],
            "refresh_token": response['refresh_token']
        }
        encryptedData = encrypt(json.dumps(dataToEncrypt)).decode()
        s3Client.put_object(Body=encryptedData, Bucket='conf-token', Key='access-token-infusionSoft')
        data_extraction(response['access_token'], response['refresh_token'], False)
        print("-------Refreshing Token Ended-----------")
        logger.info("-------Refreshing Token Ended-----------")
    except Exception as e:
        logger.error("Error Occurred during refreshing token - {}", e)
        print("Error Occurred during refreshing token - {}", e)


# uploading a file to S3 following a defined structure
def upload(filename, content, timestamp, type):
    try:
        folder_name = "bti_infusionSoft/staging/" + filename + "/" + timestamp
        s3Client.put_object(Bucket=bucket_name, Key=(folder_name + '/'))
        if type == 'csv':
            filename = filename + "-" + timestamp + ".csv"
        else:
            filename = filename + "-" + timestamp + ".json"
        key = folder_name + "/" + filename
        s3Client.put_object(Body=content.encode('utf-8'), Bucket=bucket_name, Key=key)
    except Exception as e:
        print("Error Occurred during file upload - {}", e)


# Getting list of api with different details from DynamnoDB
def get_data_dynamoDB(tableName):
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


# Creates log data locally in dict format for different failed and successful api calls
def log_data_creator(apiLog, start, api, endpoint, time_obj, rows_extracted, status):
    eventId = time.strftime('%Y-%m-%d', time.localtime(start))
    id = uuid.uuid4().int & (1 << 64) - 1
    if len(apiLog) == 0:
        startingEpochTime = start
        startingTime = time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime(start))
        time_taken = time.time() - start
    else:
        startingEpochTime = apiLog[-1]["time_taken"] + apiLog[-1]['startingEpochTime']
        time_taken = time.time() - startingEpochTime
        startingTime = time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime(startingEpochTime))
    # For Successful api calls
    if status == True:
        print("---time_taken is ", time_taken)
        apiLog.append({
            'id': id,
            'event_id': eventId,
            'api': api,
            'endpoint': endpoint,
            'isExecuted': 'yes',
            'retry_count': 0,
            'startingEpochTime': startingEpochTime,
            'startingTime': startingTime,
            'time_taken': time_taken,
            'rows_extracted': rows_extracted,
            'since': time_obj['since'],
            'until': time_obj['until'],
            'output': 'EL process completed successfully'
        })
    # For failed api calls
    else:
        if len(apiLog) == 0:
            apiLog.append({
                'id': id,
                'event_id': eventId,
                'api': api,
                'endpoint': endpoint,
                'isExecuted': 'no',
                'retry_count': 0,
                'startingEpochTime': startingEpochTime,
                'startingTime': startingTime,
                'time_taken': time_taken,
                'rows_extracted': rows_extracted,
                'since': time_obj['since'],
                'until': time_obj['until'],
                'output': 'EL process failed'
            })
        else:
            count = 0
            for each_item in apiLog:
                if each_item['api'] == api:
                    each_item['retry_count'] = each_item['retry_count'] + 1
                    each_item['time_taken'] = each_item['time_taken'] + time_taken
                    count += 1
            if count == 0:
                apiLog.append({
                    'id': id,
                    'event_id': eventId,
                    'api': api,
                    'endpoint': endpoint,
                    'isExecuted': 'no',
                    'retry_count': 0,
                    'startingEpochTime': startingEpochTime,
                    'startingTime': startingTime,
                    'time_taken': time_taken,
                    'rows_extracted': rows_extracted,
                    'since': time_obj['since'],
                    'until': time_obj['until'],
                    'output': 'EL process failed'
                })

# generates log data in a table defined in DynamoDB
def log_generator(log):
    print("-------Logging Data started-----------")
    logger.info("-------Logging Data started-----------")
    table = dynamodb.Table('bti_job_history_InfusionSoft')
    for each_item in log:
        response = table.put_item(
            Item=json.loads(json.dumps(each_item), parse_float=Decimal)
        )
    print("-------Logging Data ended-----------")
    logger.info("-------Logging Data ended-----------")


# handler
def extract_load_infusionSoft(events, context):
    authenticate()
    logger.info("----Ended----")
    print("----Ended----")
    return({"message": "successful"})

