import boto3
import logging
import time
import urllib
import json
import textwrap
import re
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)
    
s3 = boto3.client('s3')
teclient = boto3.client('textract')


def get_file_name(s3url):
    return s3url.split('/')[-1]

def image_handler(bucket, key):
    logger.info("Received Image File: %s", key)
    response = teclient.detect_document_text(
        Document={'S3Object': {'Bucket': bucket, 'Name': key}})
    text = ''
    for item in response['Blocks']:
        if item['BlockType'] == 'LINE':
            text += item['Text'] + ' '
    return text
     
def lambda_handler(event,context):
    logger.info("Received event: %s" % json.dumps(event))
    s3Bucket = event.get("s3Bucket")
    s3ObjectKey = event.get("s3ObjectKey")
    filename = os.path.basename(s3ObjectKey)
    filename = filename.split(".")[0]
   
    
    metadata = event.get("metadata")
    file_format = s3ObjectKey.lower().split('.')[-1]
    if (file_format in ["jpg", "png"]):
        afterCDE = image_handler(s3Bucket, s3ObjectKey)
        #filename = s3ObjectKey.split(".")[0]
        new_key = 'cde_output/' + filename + '.txt'
    else:
        documentBeforeCDE = s3.get_object(Bucket = s3Bucket, Key = s3ObjectKey)
        beforeCDE = documentBeforeCDE['Body'].read();
        afterCDE = beforeCDE #Do Nothing for now
        new_key = 'cde_output/' + filename
    s3.put_object(Bucket = s3Bucket, Key = new_key, Body=afterCDE)
    
    return {
        "version" : "v0",
        "s3ObjectKey": new_key,
        "metadataUpdates": []
    }
