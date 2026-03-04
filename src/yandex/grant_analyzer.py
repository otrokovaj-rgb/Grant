import json
import requests
import PyPDF2
import io
import urllib.parse
import re

# Константы
SOURCE_BUCKET = "source-documents"
QUARANTINE_BUCKET = "quarantine-files"
RESULT_BUCKET = "processed-results"
REPORT_FOLDER = "reports"
FOLDER_ID = "b1gjmkcodo66d40u8p9k"  # Ваш folder_id

STORAGE_URL = "https://storage.yandexcloud.net"

# Список ключевых слов для дополнительной проверки (можно расширять)
ALCOHOL_KEYWORDS = [
    "самогон", "фляжка", "водка", "пиво", "алкоголь", "пьяный",
    "напиток", "выпил", "глоток", "опьянеть", "бутылка", "налить",
    "средство от ломоты", "яд лютый", "для суставов", "перцу переложила"
]

def get_iam_token():
    """Получает IAM-токен из метаданных функции."""
    url = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
    headers = {"Metadata-Flavor": "Google"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()["access_token"]
    else:
        raise Exception("Не удалось получить IAM-токен")

def storage_request(method, bucket, key, token, data=None, content_type=None):
    """Выполняет запрос к Object Storage с авторизацией по IAM-токену."""
    encoded_key = urllib.parse.quote(key, safe='')
    url = f"{STORAGE_URL}/{bucket}/{encoded_key}"
    headers = {"Authorization": f"Bearer {token}"}
    if content_type:
        headers["Content-Type"] = content_type
    resp = requests.request(method, url, headers=headers, data=data)
    resp.raise_for_status()
    return resp

def extract_json_from_gpt_response(text):
    """Извлекает JSON из ответа GPT, убирая markdown-обёртки."""
    text = text.strip()
    if text.startswith('```') and text.endswith('```'):
        lines = text.split('\n')
        if lines[0].startswith('```'):
            lines = lines[1:]
        if lines and lines[-1].strip() == '```':
            lines = lines[:-1]
        text = '\n'.join(lines).strip()
    return text

def check_additional_violations(text):
    """
    Проверяет текст на наличие ключевых слов, связанных с алкоголем, курением, наркотиками.
    Возвращает список нарушений (каждый элемент — словарь с type и quote) или пустой список.
    """
    violations = []
    # Проверка на алкоголь
    for keyword in ALCOHOL_KEYWORDS:
        pattern = re.compile(re.escape(keyword), re.IGNORECASE)
        match = pattern.search(text)
        if match:
            # Извлекаем предложение или фрагмент вокруг совпадения (можно улучшить)
            start = max(0, match.start() - 30)
            end = min(len(text), match.end() + 30)
            quote = text[start:end].strip()
            violations.append({
                "type": "упоминание алкоголя",
                "quote": quote
            })
            break  # Достаточно одного совпадения, но можно собирать все
    # Здесь можно добавить проверки на курение, наркотики и т.д.
    return violations

def analyze_with_yandexgpt(text, folder_id, token):
    """
    Отправляет текст в YandexGPT и получает структурированный ответ.
    Затем применяет дополнительную проверку на ключевые слова.
    Возвращает словарь с полями theme, status, violations (уже объединёнными).
    """
    prompt = f"""
Ты — эксперт по проверке сценариев на соответствие моральным ценностям РФ.
Проанализируй текст сценария и верни результат строго в формате JSON:
{{
  "theme": "краткая тема сценария (до 5 слов)",
  "status": "accepted" или "rejected",
  "violations": [
    {{"type": "нецензурная лексика", "quote": "цитата из текста"}},
    ...
  ]
}}
Если нарушений нет, поле violations должно быть пустым списком.
Текст сценария:
{text[:6000]}
"""
    url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    data = {
        "modelUri": f"gpt://{folder_id}/yandexgpt-lite",
        "completionOptions": {
            "stream": False,
            "temperature": 0.1,
            "maxTokens": 800
        },
        "messages": [
            {"role": "system", "text": "Ты — строгий цензор, проверяющий сценарии."},
            {"role": "user", "text": prompt}
        ]
    }
    resp = requests.post(url, headers=headers, json=data)
    if resp.status_code == 200:
        result = resp.json()
        answer = result["result"]["alternatives"][0]["message"]["text"].strip()
        json_str = extract_json_from_gpt_response(answer)
        try:
            parsed = json.loads(json_str)
            if "theme" not in parsed or "status" not in parsed or "violations" not in parsed:
                raise ValueError("Ответ не содержит необходимых полей")
        except Exception as e:
            print(f"Не удалось распарсить ответ GPT: {json_str}, ошибка: {e}")
            parsed = {
                "theme": "не определено",
                "status": "rejected",
                "violations": [{"type": "ошибка анализа", "quote": answer[:200]}]
            }
    else:
        raise Exception(f"Ошибка YandexGPT: {resp.status_code} {resp.text}")

    # Дополнительная проверка на ключевые слова (алкоголь и т.д.)
    extra_violations = check_additional_violations(text)
    if extra_violations:
        # Если GPT уже нашёл какие-то нарушения, объединяем списки
        parsed["violations"].extend(extra_violations)
        parsed["status"] = "rejected"  # Принудительно ставим rejected
    return parsed

def handler(event, context):
    try:
        iam_token = get_iam_token()
    except Exception as e:
        print(f"Не удалось получить IAM-токен: {e}")
        return {'statusCode': 500, 'body': 'Token Error'}

    # Разбираем событие от триггера
    try:
        message = event['messages'][0]
        bucket_id = message['details']['bucket_id']
        object_key = message['details']['object_id']
    except Exception as e:
        print(f"Ошибка разбора event: {e}")
        return {'statusCode': 400, 'body': 'Bad Request'}

    print(f"Начинаем проверку файла: {object_key} из бакета {bucket_id}")

    # Скачиваем файл
    try:
        resp = storage_request('GET', SOURCE_BUCKET, object_key, iam_token)
        file_content = resp.content
        print(f"Файл скачан, размер: {len(file_content)} байт")
    except Exception as e:
        print(f"Ошибка скачивания: {e}")
        return {'statusCode': 500, 'body': f'Download Error: {e}'}

    # Определяем тип файла по расширению
    ext = object_key.split('.')[-1].lower()
    is_script = ext in ['pdf', 'txt']

    if is_script and ext == 'pdf':
        # Извлекаем текст из PDF
        text = ""
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        except Exception as e:
            text = ""
            reason = f"Ошибка извлечения текста: {e}"
            status = "error"
            report_text = ""

        if not text.strip():
            status = "error"
            reason = "Не удалось извлечь текст из файла"
            report_text = ""
        else:
            try:
                analysis = analyze_with_yandexgpt(text, FOLDER_ID, iam_token)
                if analysis["status"] == "accepted":
                    status = "success"
                    reason = ""
                    report_text = f"Тема сценария: {analysis['theme']}\nСтатус: принят"
                else:
                    status = "error"
                    # Формируем описание нарушений с цитатами
                    violations_list = []
                    for v in analysis["violations"]:
                        violations_list.append(f"{v['type']}: «{v['quote']}»")
                    reason = "Обнаружены нарушения:\n" + "\n".join(violations_list)
                    report_text = ""
            except Exception as e:
                status = "error"
                reason = f"Ошибка вызова YandexGPT: {e}"
                report_text = ""
    else:
        # Не сценарий или неподдерживаемый формат
        status = "success"
        reason = ""
        report_text = "Файл не является сценарием или неподдерживаемый формат, проверка не выполнялась"

    # Формируем имя отчёта
    report_name = f"report_{object_key}.json"

    if status == "error":
        # Перемещаем исходный файл в карантин и создаём отчёт об ошибке
        report_body = json.dumps({
            "source_file": object_key,
            "status": "error",
            "reason": reason
        }, ensure_ascii=False).encode('utf-8')
        try:
            storage_request('PUT', QUARANTINE_BUCKET, f"{REPORT_FOLDER}/{report_name}",
                            iam_token, data=report_body, content_type='application/json')
            print(f"Отчёт об ошибке загружен в {QUARANTINE_BUCKET}")
        except Exception as e:
            print(f"Ошибка загрузки отчёта об ошибке: {e}")
            return {'statusCode': 500, 'body': 'Report Upload Error'}

        # Копируем исходный файл в карантин
        try:
            storage_request('PUT', QUARANTINE_BUCKET, object_key,
                            iam_token, data=file_content, content_type='application/pdf')
            print(f"Исходный файл скопирован в карантин")
        except Exception as e:
            print(f"Ошибка копирования файла в карантин: {e}")

        # Удаляем исходный файл из source-documents
        try:
            storage_request('DELETE', SOURCE_BUCKET, object_key, iam_token)
            print(f"Исходный файл удалён из {SOURCE_BUCKET}")
        except Exception as e:
            print(f"Ошибка удаления исходного файла: {e}")
            return {'statusCode': 500, 'body': 'Delete Error'}

    else:
        # Всё хорошо — сохраняем отчёт в processed-results
        report_body = json.dumps({
            "source_file": object_key,
            "status": "success",
            "report": report_text
        }, ensure_ascii=False).encode('utf-8')
        try:
            storage_request('PUT', RESULT_BUCKET, f"{REPORT_FOLDER}/{report_name}",
                            iam_token, data=report_body, content_type='application/json')
            print(f"Отчёт успешно загружен в {RESULT_BUCKET}")
        except Exception as e:
            print(f"Ошибка загрузки отчёта: {e}")
            return {'statusCode': 500, 'body': 'Report Upload Error'}

    return {'statusCode': 200, 'body': 'OK'}
