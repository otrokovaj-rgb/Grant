import boto3
import json
from botocore.client import Config

def handler(event, context):
    # Получаем параметры из тела запроса
    try:
        body = json.loads(event['body'])
        bucket = body['bucket']
        key = body['key']
    except Exception as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Invalid request'})
        }

    # Настраиваем клиент S3
    session = boto3.session.Session()
    s3 = session.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        region_name='ru-central1',
        config=Config(signature_version='s3v4')
    )

    try:
        s3.delete_object(Bucket=bucket, Key=key)
        return {
            'statusCode': 200,
            'body': json.dumps({'status': 'deleted'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }