import json
import requests

WORKFLOW_ID = "dfqneb50q7h1sn95cc2f"   # скопируйте из консоли Workflows
FOLDER_ID = "b1gjmkcodo66d40u8p9k"     # ваш folder_id

def handler(event, context):
    print("Функция-прокси запущена")
    print("Event:", json.dumps(event, ensure_ascii=False))
    
    try:
        token = context.token["access_token"]
        print("IAM-токен получен")
    except Exception as e:
        print("Ошибка получения токена:", e)
        return {"statusCode": 500, "body": "Token error"}

    # Правильный полный URL
    url = "https://workflows.api.cloud.yandex.net/workflows/v1/execution"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {
        "workflowId": WORKFLOW_ID,
        "input": event
    }
    
    print("Отправка запроса к API...")
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=10)
        print("Статус ответа:", resp.status_code)
        print("Тело ответа:", resp.text)
        if resp.status_code == 200:
            return {"statusCode": 200, "body": "Workflow started"}
        else:
            return {"statusCode": resp.status_code, "body": resp.text}
    except requests.exceptions.RequestException as e:
        print("Исключение при запросе:", str(e))
        return {"statusCode": 500, "body": str(e)}