import os
import json
import requests
from urllib.parse import urljoin

# Константы
FINANCIAL_FUNCTION_URL = "https://functions.yandexcloud.net/d4e92iv3g1msf6oc6356"
SCRIPT_FUNCTION_URL = "https://functions.yandexcloud.net/d4e5dvrvl4oe4thursdv"

def get_iam_token():
    """Получение IAM-токена из метаданных функции."""
    url = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
    headers = {"Metadata-Flavor": "Google"}
    resp = requests.get(url, headers=headers, timeout=3)
    resp.raise_for_status()
    return resp.json()["access_token"]

def handler(event, context):
    # Логируем входящее событие
    print(f"Dispatcher received event: {json.dumps(event)}")

    # Извлекаем данные из события триггера
    try:
        bucket = event['messages'][0]['details']['bucket_id']
        object_key = event['messages'][0]['details']['object_id']
    except (KeyError, IndexError) as e:
        print(f"Failed to parse event: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps({'status': 'error', 'reason': 'Invalid event format'})
        }

    print(f"Processing file: {object_key} from bucket {bucket}")

    # Определяем тип файла по расширению
    ext = object_key.split('.')[-1].lower()
    if ext in ['xlsx', 'xls']:
        target_url = FINANCIAL_FUNCTION_URL
        print(f"Financial file detected -> calling {target_url}")
    else:
        target_url = SCRIPT_FUNCTION_URL
        print(f"Script file detected -> calling {target_url}")

    # Получаем IAM-токен для вызова целевой функции
    try:
        token = get_iam_token()
    except Exception as e:
        print(f"Failed to get IAM token: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'status': 'error', 'reason': 'IAM token error'})
        }

    # Формируем запрос к целевой функции
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    payload = {
        'bucket': bucket,
        'object': object_key
    }

    try:
        response = requests.post(target_url, json=payload, headers=headers, timeout=55)
        print(f"Target function responded with status {response.status_code}")
        print(f"Response body: {response.text}")

        # Возвращаем ответ целевой функции
        return {
            'statusCode': response.status_code,
            'headers': {'Content-Type': 'application/json'},
            'body': response.text
        }
    except Exception as e:
        print(f"Error calling target function: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'status': 'error', 'reason': str(e)})
        }