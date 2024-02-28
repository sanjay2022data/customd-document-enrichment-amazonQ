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
transcribe = boto3.client('transcribe')

def get_file_name(s3url):
    return s3url.split('/')[-1]

def transcribe_job_name(*args):
    timestamp=time.time()
    job_name = "__".join(args) + "_" + str(timestamp)
    job_name = re.sub(r"[^0-9a-zA-Z._-]+","--",job_name)
    return job_name

def start_media_transcription(job_uri, media_format):
    file_name = get_file_name(job_uri)
    job_name = transcribe_job_name(file_name)
    try:
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': job_uri},
            MediaFormat=media_format,
            LanguageCode='en-US'
        )
    except Exception as e:
        logger.error("Exception while starting: " + job_name)
        logger.error(e)
        return ""
    job_complete = False
    while not job_complete:
        transcription_job = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        if transcription_job['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
            job_complete = True
        time.sleep(5)
    transcript_url = transcription_job['TranscriptionJob']['Transcript']['TranscriptFileUri']
    return transcript_url

def prepare_transcript(transcript_uri):
    response = urllib.request.urlopen(transcript_uri)
    transcript = json.loads(response.read())
    items = transcript["results"]["items"]
    txt = ""
    sentence = ""
    for i in items:
        if (i["type"] == 'punctuation'):
            sentence = sentence + i["alternatives"][0]["content"]
            if (i["alternatives"][0]["content"] == '.'):
                #sentence completed
                txt = txt + " " + sentence + " "
                sentence = ""
        else: 
            if (sentence == ''):
                sentence = "[" + i["start_time"] + "]"
            sentence = sentence + " " + i["alternatives"][0]["content"]
    if (sentence != ""):
        txt = txt + " " + sentence + " "
    out = textwrap.fill(txt, width=70)
    return out

def image_handler(bucket, key):
    logger.info("Received Image File: %s", key)
    response = teclient.detect_document_text(
        Document={'S3Object': {'Bucket': bucket, 'Name': key}})
    output = ''
    for b in response['Blocks']:
        if (b['BlockType'] == 'LINE'):
            output = output + '\n' + b['Text']
    return output
     
def lambda_handler(event, context):
    logger.info("Received event: %s" % json.dumps(event))
    s3Bucket = event.get("s3Bucket")
    s3ObjectKey = event.get("s3ObjectKey")
    #_, file_name = os.path.split(s3ObjectKey)
    metadata = event.get("metadata")
    samplekey = s3ObjectKey.lower()
    file_format = s3ObjectKey.lower().split('.')[-1]
    if (file_format in ["jpg", "png"]):
        afterCDE = image_handler(s3Bucket, s3ObjectKey)
        new_key = 'cde_output/' + s3ObjectKey + '.txt'
    elif (file_format in ["mp3", "mp4"]):
        job_uri = 's3://' + s3Bucket + '/' + s3ObjectKey
        transcript_url = start_media_transcription(job_uri, file_format)
        if (transcript_url != ""):
            logger.info("Transcript URL:" + transcript_url)
            afterCDE = prepare_transcript(transcript_url)
            new_key = 'cde_output/' + samplekey + '.txt'
        else:
            logger.info("Did not get transcript url")
            afterCDE = "Transcript was unsuccessful"
            new_key = 'cde_output/' + samplekey + '.txt'
    else:
        documentBeforeCDE = s3.get_object(Bucket = s3Bucket, Key = s3ObjectKey)
        beforeCDE = documentBeforeCDE['Body'].read();
        afterCDE = beforeCDE #Do Nothing for now
        new_key = 'cde_output/' + samplekey
    s3.put_object(Bucket = s3Bucket, Key = new_key, Body=afterCDE)
    return {
        "version" : "v0",
        "s3ObjectKey": new_key,
        "metadataUpdates": []
    }
