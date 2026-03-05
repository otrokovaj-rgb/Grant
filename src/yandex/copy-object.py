import boto3
import json
from botocore.client import Config

def handler(event, context):
    body = json.loads(event['body'])
    source_bucket = body['source_bucket']
    source_key = body['source_key']
    dest_bucket = body['dest_bucket']
    dest_key = body['dest_key']

    session = boto3.session.Session()
    s3 = session.client('s3',
                        endpoint_url='https://storage.yandexcloud.net',
                        region_name='ru-central1',
                        config=Config(signature_version='s3v4'))

    copy_source = {'Bucket': source_bucket, 'Key': source_key}
    s3.copy_object(Bucket=dest_bucket, Key=dest_key, CopySource=copy_source)

    return {
        'statusCode': 200,
        'body': json.dumps({'status': 'copied'})
    }
