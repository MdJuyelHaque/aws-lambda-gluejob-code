from json import encoder
import os
import boto3
from boto3.dynamodb.conditions import Key
from cryptography import fernet
from cryptography.fernet import Fernet
import json
import requests
import pandas as pd
import base64
from base64 import b64decode

s3Resource = boto3.resource('s3')
s3Client = boto3.client('s3')
encryp_key = os.environ['encryption_secret_key']

#  KMS decryption
def kms_decryption(encrypted_data):
    return boto3.client('kms').decrypt(
        CiphertextBlob=b64decode(encrypted_data),
        EncryptionContext={'LambdaFunctionName': os.environ['AWS_LAMBDA_FUNCTION_NAME']}
    )['Plaintext'].decode('utf-8')

# getting kms decrypted values
client_id = kms_decryption(os.environ['client_id'])
client_secret = kms_decryption(os.environ['client_secret'])

fernet=Fernet(encryp_key.encode('utf-8'))

# Decrypting an encrypted vakue using a key
def decrypt(encryptedData):
    decryptedData = fernet.decrypt(encryptedData).decode()
    return decryptedData


def encrypt(decryptedData):
    encryptedData = fernet.encrypt(decryptedData.encode('utf-8'))
    return encryptedData


def refresh_token(refreshToken):
    try:
        print("-------Refreshing Token Started-----------")
        #logger.info("-------Refreshing Token Started-----------")
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
        s3Client.put_object(Body=encryptedData, Bucket='conf-token', Key='access-token-infusionSoft_IT')
        #data_extraction(response['access_token'], response['refresh_token'], False)
        print("-------Refreshing Token Ended-----------")
        #logger.info("-------Refreshing Token Ended-----------")
    except Exception as e:
        #logger.error("Error Occurred during refreshing token - {}", e)
        print("Error Occurred during refreshing token - {}", e)


# handler
def refresh_token_it(events, context):
    obj = s3Resource.Object('conf-token', "access-token-infusionSoft_IT")
    body = obj.get()['Body'].read()
    decryptedData = json.loads(decrypt(body))
    accessToken = decryptedData['access_token']
    refreshToken = decryptedData['refresh_token']
    print("old access  "+accessToken)
    print("old Refresh  "+refreshToken)

    refresh_token(refreshToken)

    obj1 = s3Resource.Object('conf-token', "access-token-infusionSoft_IT")
    body = obj1.get()['Body'].read()
    decryptedData = json.loads(decrypt(body))
    accessToken = decryptedData['access_token']
    refreshToken = decryptedData['refresh_token']
    print("New access  "+accessToken)
    print("New refresh  "+refreshToken)

 