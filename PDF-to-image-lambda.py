import boto3
from pdf2image import convert_from_bytes
#import uuid
from io import BytesIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Get the S3 bucket and key for the PDF file
    #source_bucket = "opensearchdemosanjay"
    #source_key = "EMD_Dec_CACert.pdf"
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    filename = source_key.split(".")[0]
    # Generate a unique prefix for the images to prevent overwrites
    #uid = str(uuid.uuid4())

    # Download the PDF file contents
    pdf_obj = s3.get_object(Bucket=source_bucket, Key=source_key)
    pdf_bytes = pdf_obj['Body'].read()

    # Convert PDF pages to images
    images = convert_from_bytes(pdf_bytes)

    # Define S3 bucket to save images
    dest_bucket = 'document-tender'

    for i, image in enumerate(images):
        # Convert image to bytes
        with BytesIO() as output:
            image.save(output, format='png')
            image_bytes = output.getvalue()

        # Upload image to S3 bucket
        s3.put_object(
            Bucket=dest_bucket,
            Key= f"Data/Media/{filename}page_{i}.png",
            #Key=f"Data/Media/{uid}/page_{i}.png",
            Body=image_bytes,
            ContentType='image/png'
        )

    return {
        'statusCode': 200,
        'body': f'Uploaded {len(images)} images from PDF {source_key}'
    }
    
