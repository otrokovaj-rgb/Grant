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
FOLDER_ID = "b1gjmkcodo66d40u8p9k"  # замените на ваш folder_id

STORAGE_URL = "https://storage.yandexcloud.net"

# Ключевые слова для резервной проверки
BANNED_KEYWORDS = [
    # Алкоголь
    "самогон", "фляжка", "водка", "пиво", "алкоголь", "пьяный", "опьянеть",
    # Курение
    "сигарета", "курить", "закурить", "папироса", "вейп",
    # Наркотики
    "наркотик", "доза", "ширево",
    # Нецензурная лексика (простые варианты, можно дополнить)
    "ёпрст", "хрен", "дурак", "идиот"
]

def get_iam_token():
    url = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"
    headers = {"Metadata-Flavor": "Google"}
    resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        return resp.json()["access_token"]
    else:
        raise Exception("Не удалось получить IAM-токен")

def storage_request(method, bucket, key, token, data=None, content_type=None):
    encoded_key = urllib.parse.quote(key, safe='')
    url = f"{STORAGE_URL}/{bucket}/{encoded_key}"
    headers = {"Authorization": f"Bearer {token}"}
    if content_type:
        headers["Content-Type"] = content_type
    resp = requests.request(method, url, headers=headers, data=data)
    resp.raise_for_status()
    return resp

def check_banned_keywords(text):
    """Проверяет текст на наличие запрещённых ключевых слов."""
    violations = []
    for keyword in BANNED_KEYWORDS:
        pattern = re.compile(re.escape(keyword), re.IGNORECASE)
        match = pattern.search(text)
        if match:
            start = max(0, match.start() - 30)
            end = min(len(text), match.end() + 30)
            quote = text[start:end].strip()
            violations.append({
                "keyword": keyword,
                "quote": quote
            })
            # Можно добавить все, но для первого достаточно
            return violations
    return violations

def analyze_with_yandexgpt(text, folder_id, token):
    prompt_template = """
Ты — эксперт отдела контроля контента анимационной студии. Твоя задача — проверить предоставленный текст (сценарий мультфильма) на соответствие законодательству РФ и правилам платформы.

Критерии проверки:
- Пропаганда деструктивного поведения: сцены, демонстрирующие опасный для жизни образ действий без последствий, если это не является частью поучительной истории.
- Дети должны видеть в мультфильме настоящие жизненные ситуации, эмоции, взаимоотношения (включая дружбу, ссоры, грусть, преодоление трудностей). Это нормально.
- Недопустима «жесть»: сцены жестокости, физического насилия, крови, ужасов, издевательств, опасных действий без последствий, которые могут травмировать или напугать ребёнка.
- Запрещены: сцены употребления алкоголя, табака, наркотиков; любые сексуальные намёки; нецензурная лексика и оскорбления.

Инструкция:
1. Прочитай сценарий.
2. Если в нём есть нарушения по пунктам «жесть» или прямые запрещённые сцены (алкоголь, секс, мат и т.п.) — вынеси вердикт «Не принят».
3. Если сценарий содержит жизненные ситуации, конфликты или эмоции, но без жестокости и запрещённых элементов — вердикт «Принят».
4. Если встречается сцена, где показано опасное поведение (например, герой лезет на крышу), но при этом явно показаны последствия или это осуждается — это допустимо как жизненный урок, нарушения нет.

Формат вывода (строго соблюдай структуру):
Вердикт: [Принят / Не принят]
Тематика: *[если Принят, кратко о чем мультфильм, 1 предложение]*
Нарушение: [если Не принят, точная цитата и тип нарушения; иначе оставь пусто]

Текст сценария:
{}
"""
    prompt = prompt_template.format(text[:6000])

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
            "maxTokens": 300
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
        return answer
    else:
        raise Exception(f"Ошибка YandexGPT: {resp.status_code} {resp.text}")

def parse_gpt_response(response):
    """Парсит ответ GPT и возвращает (verdict, theme, quote)"""
    lines = response.split('\n')
    verdict = ""
    theme = ""
    quote = ""
    for line in lines:
        line = line.strip()
        if line.lower().startswith("вердикт:"):
            verdict = line[8:].strip().lower()
        elif line.lower().startswith("тематика:"):
            theme = line[9:].strip()
        elif line.lower().startswith("нарушение:"):
            quote = line[10:].strip()
    return verdict, theme, quote

def handler(event, context):
    try:
        iam_token = get_iam_token()
    except Exception as e:
        print(f"Не удалось получить IAM-токен: {e}")
        return {'statusCode': 500, 'body': 'Token Error'}

    # Разбираем событие
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

    # Извлекаем текст
    ext = object_key.split('.')[-1].lower()
    text = ""
    try:
        if ext == 'pdf':
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
        elif ext == 'txt':
            text = file_content.decode('utf-8', errors='ignore')
        else:
            # Неподдерживаемый формат – считаем ошибкой
            text = ""
            print(f"Неподдерживаемый формат файла: {ext}")
    except Exception as e:
        print(f"Ошибка извлечения текста: {e}")
        text = ""

    if not text.strip():
        # Нет текста – в карантин
        status = "error"
        reason = "Не удалось извлечь текст из файла (возможно, пустой PDF или неподдерживаемый формат)"
        report_text = ""
    else:
        # 1. Резервная проверка по ключевым словам
        keyword_violations = check_banned_keywords(text)
        if keyword_violations:
            print("Обнаружены запрещённые ключевые слова:", keyword_violations)
            status = "error"
            # Формируем причину с цитатой
            v = keyword_violations[0]
            reason = f"Обнаружено запрещённое слово '{v['keyword']}' в контексте: ...{v['quote']}..."
            report_text = ""
        else:
            # 2. Проверка через YandexGPT
            try:
                gpt_response = analyze_with_yandexgpt(text, FOLDER_ID, iam_token)
                print(f"Ответ GPT:\n{gpt_response}")
                verdict, theme, quote = parse_gpt_response(gpt_response)

                if verdict == "принят":
                    status = "success"
                    reason = ""
                    report_text = f"принят, тематика: {theme if theme else 'не указана'}"
                elif verdict == "не принят":
                    status = "error"
                    reason = f"не принят, цитата: {quote if quote else 'не указана'}"
                    report_text = ""
                else:
                    # Не удалось распарсить
                    status = "error"
                    reason = f"Неожиданный ответ GPT (не удалось извлечь вердикт): {gpt_response[:200]}"
                    report_text = ""
            except Exception as e:
                print(f"Ошибка вызова YandexGPT: {e}")
                status = "error"
                reason = f"Ошибка вызова YandexGPT: {e}"
                report_text = ""

    # Формируем имя отчёта
    report_name = f"report_{object_key}.json"

    if status == "error":
        # Отправляем в карантин
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
                            iam_token, data=file_content, content_type='application/octet-stream')
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
        # Успех — отчёт в processed-results, файл остаётся на месте
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