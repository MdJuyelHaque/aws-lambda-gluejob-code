import base64
import boto3
import gzip
import json
import logging
import os
import time


from botocore.exceptions import ClientError
time.sleep(120)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
client= boto3.client('glue')
gluejobname = os.environ.get('Gluejobname')
gluejobname1 = os.environ.get('Gluejobname1')



def logpayload(event):
    #   logger.setLevel(logging.DEBUG)
    #  logger.debug(event['awslogs']['data'])
      compressed_payload = base64.b64decode(event['awslogs']['data'])
      uncompressed_payload = gzip.decompress(compressed_payload)
      log_payload = json.loads(uncompressed_payload)
      return log_payload

def gluejob(event):
    response=client.start_job_run(JobName=gluejobname)
    # response1=client.start_job_run(JobName=gluejobname1)
    # logger.info("glu job started"+ gluejobname)
    # logger.info("glu job started"+ gluejobname1)
    return response
    
def gluejob1(event):
    # response=client.start_job_run(JobName=gluejobname)
    response1=client.start_job_run(JobName=gluejobname1)
    # logger.info("glu job started"+ gluejobname)
    # logger.info("glu job started"+ gluejobname1)
    return response1




def lambda_handler(event, context):
    pload = logpayload(event)
    gluejob(event)
    gluejob1(event)

    