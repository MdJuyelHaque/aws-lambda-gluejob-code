import gzip
from json import encoder
import os
from pprint import pprint
from infusionsoft.library import Infusionsoft
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
bucket_name = os.environ['bucket']
s3_staging_path = os.environ['s3_staging_path']
s3_archive_path = os.environ['s3_archive_path']
userId = os.environ['user_id']
ddb_aws_endpoint = os.environ['aws_url']


fernet = Fernet(encryp_key.encode('utf-8'))
        
s3Resource = boto3.resource('s3')
s3Client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb', endpoint_url=ddb_aws_endpoint)


logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info("----  Started EL for Saved Report Payment ----")
print("----  Started EL for Saved Report Payment ----")


s3Resource = boto3.resource('s3')
s3Client = boto3.client('s3')
my_bucket = s3Resource.Bucket(bucket_name)


s3Client = boto3.client('s3')

# KMS decryption
def kms_decryption(encrypted_data):
    return boto3.client('kms').decrypt(
        CiphertextBlob=b64decode(encrypted_data),
        EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
    )['Plaintext'].decode('utf-8')


#getting kms decrypted values
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


# Getting acces_token and refresh_token value from file in s3 bucket
def authenticate():
    obj = s3Resource.Object('conf-token', "access-token-infusionSoft")
    body = obj.get()['Body'].read()
    decryptedData = json.loads(decrypt(body))
    accessToken = decryptedData['access_token']
    print(accessToken)
    refreshToken = decryptedData['refresh_token']
    data_extraction(accessToken, refreshToken, False)


def upload(filename, content, timestamp, type):
    try:
        folder_name = s3_staging_path + filename + "/" + timestamp
        s3Client.put_object(Bucket=bucket_name, Key=(folder_name + '/'))
        if type == 'csv':
            filename = filename + "-" + timestamp + ".csv"
        else:
            filename = filename + "-" + timestamp + ".json"
        key = folder_name + "/" + filename
        s3Client.put_object(Body=content.encode('utf-8'), Bucket=bucket_name, Key= key)
        print("File Successfully Uploaded at s3://" + bucket_name + "/" + key)
    except Exception as e:
        print("Error Occurred during file upload - {}", e)

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
        print("Error Occurred during refreshing token - ", e)

def data_extraction(accessToken, refreshToken, isError):
    try:
        infusionsoft = InfusionsoftOAuth(accessToken)
        saved_report_details = get_saved_report_details('bti_saved_report_details')
        if len(saved_report_details) > 0:
            for each_key in saved_report_details:
            
                filename_report = (each_key['report_name'])
                saved_search_id = int(each_key['report_id'])
                status = (each_key['status'])
    
                print(filename_report,saved_search_id,status)
                if(status):
                    
                    page = 0
                    Saved_report_data = ["data"]
                    data_to_upload = []
                    logger.info("--------- Extraction and load for Report ID " + str(saved_search_id) + " started")
                    print("--------- Extraction and load for Report ID " + str(saved_search_id) + " started")
                    while len(Saved_report_data) != 0:
                        Saved_report_data = ( infusionsoft.SearchService('getSavedSearchResultsAllFields', saved_search_id, userId,page) )
                        if((str(Saved_report_data).find("401 Unauthorized"))>=0):
                            refresh_token(refreshToken)
                            break
                        print("Hit Number ------->  ",page)
                        page += 1
                        data_to_upload += Saved_report_data
                    current_timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                    print("row Count ---- >>  ",len(data_to_upload))
                
        ## To convert xmlrpc.client.DateTime into Python Date Time Format 
                x=0
                for elements in data_to_upload:
                    for each_element in elements:
                        if str(type(elements[each_element])) == "<class 'xmlrpc.client.DateTime'>":
                            temp=elements[each_element]
                            str_t= str(temp)
                            time_py= str_t[0:4]+'-'+str_t[4:6]+'-'+str_t[6:]
                            elements[each_element]=time_py
                            x+=1
                print("Column Converted From Xml to datetime ---->  ",x)



                json_data = json.dumps(data_to_upload)
                upload(filename_report, json.dumps(data_to_upload), current_timestamp, "json")
                logger.info("--------- Extraction and load for Table  " + filename_report+ " ended")
                print("--------- Extraction and load for Table " + filename_report + " ended")

    except Exception as e:
        #logger.error("Error Occurred during data_extraction - {}", e)
        print("Error Occurred during data_extraction - ", e)        

def get_saved_report_details(tableName):
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


def archive_staging():
    try:
        print("Archiving Staging started...")
        logger.info("Archiving Staging started...")
        for each_object in my_bucket.objects.filter(Prefix=s3_staging_path):
            print(each_object)
            object_key = each_object.key
            archive_key = object_key.replace('staging', 'staging_archive')
            body = each_object.get()['Body'].read()
            s3Resource.Object(bucket_name, archive_key).put(Body=body)
            each_object.delete()
        logger.info("SUCCESS: Archiving Staging completed.")
        print("SUCCESS: Archiving Staging completed.")
    except Exception as e:
        logger.error("ERROR: Unexpected error: Could not archive Staging folder.")
        logger.error(e)

# handler
def EL_Saved_Report(event, context):
    archive_staging()
    authenticate()
    logger.info("----Ended----")
    logger.info("----Ended Saved Report EL----")
    print("----Ended Saved Report EL----")
    print("----Ended----")
    print("Successfull")
    return({"message": "Successfull"})






            

