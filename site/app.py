import os
import boto3
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
from datetime import datetime
import urllib.parse
import json

# Загружаем переменные окружения
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv('SECRET_KEY', 'default-secret-key-change-this')

# Настройки S3
RESULT_S3_BUCKET = os.getenv('RESULT_S3_BUCKET')
UPLOAD_S3_BUCKET = os.getenv('UPLOAD_S3_BUCKET')
S3_ENDPOINT = os.getenv('S3_ENDPOINT', None)
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')

def get_s3_client():
    """Создает и возвращает S3 клиент"""
    try:
        session = boto3.session.Session()
        
        # Параметры подключения
        params = {
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
            'region_name': AWS_REGION
        }
        
        # Если указан кастомный endpoint (для S3-совместимых хранилищ)
        if S3_ENDPOINT:
            params['endpoint_url'] = S3_ENDPOINT
        
        return session.client('s3', **params)
    except Exception as e:
        print(f"Ошибка при создании S3 клиента: {e}")
        return None

def upload_file_to_s3(file, filename):
    """Загружает файл в S3"""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False, "Не удалось подключиться к S3"
        
        # Загружаем файл
        s3_client.upload_fileobj(
            file,
            UPLOAD_S3_BUCKET,
            filename,
            ExtraArgs={
                'ContentType': file.content_type or 'application/octet-stream'
            }
        )
        return True, f"Файл '{filename}' успешно загружен"
        
    except NoCredentialsError:
        return False, "Ошибка: Не найдены учетные данные для S3. Проверьте AWS_ACCESS_KEY_ID и AWS_SECRET_ACCESS_KEY"
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            return False, f"Ошибка: Бакет '{UPLOAD_S3_BUCKET}' не существует"
        elif error_code == 'AccessDenied':
            return False, "Ошибка: Нет прав доступа к бакету"
        else:
            return False, f"Ошибка S3: {e.response['Error']['Message']}"
    except Exception as e:
        return False, f"Ошибка при загрузке: {str(e)}"

def list_files_in_s3():
    """Получает список всех файлов в S3 бакете"""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return []
        
        files = []
        continuation_token = None
        
        while True:
            # Параметры для list_objects_v2
            params = {
                'Bucket': RESULT_S3_BUCKET,
                'MaxKeys': 1000
            }
            
            if continuation_token:
                params['ContinuationToken'] = continuation_token
            
            response = s3_client.list_objects_v2(**params)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Получаем метаданные файла
                    try:
                        head_response = s3_client.head_object(
                            Bucket=RESULT_S3_BUCKET, 
                            Key=obj['Key']
                        )
                        response = s3_client.get_object(Bucket=RESULT_S3_BUCKET, Key=obj['Key'])
                        object_content = response["Body"].read().decode("utf-8")
                        data = json.loads(object_content)

                        content = data
                        content_type = head_response.get('ContentType', 'unknown')
                    except:
                        content_type = 'unknown'
                        content = ''
                    result = os.path.basename(obj['Key']).replace("report_", "").replace(".json", "")
                    files.append({
                        'name': result,
                        'last_modified': obj['LastModified'],
                        'content': content,
                        'status': 'success' if content != "" else 'error'
                    })
            
            # Проверяем, есть ли еще файлы
            if not response.get('IsTruncated'):
                break
            continuation_token = response.get('NextContinuationToken')
        
        # Сортируем по дате изменения (новые сверху)
        files.sort(key=lambda x: x['last_modified'], reverse=True)
        
        return files
        
    except ClientError as e:
        print(f"Ошибка S3 при получении списка файлов: {e}")
        return []
    except Exception as e:
        print(f"Ошибка при получении списка файлов: {e}")
        return []

def format_size(size):
    """Форматирует размер файла в человекочитаемый вид"""
    for unit in ['Б', 'КБ', 'МБ', 'ГБ']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} ТБ"

def delete_file_from_s3(filename):
    """Удаляет файл из S3"""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return False, "Не удалось подключиться к S3"
        
        s3_client.delete_object(Bucket=RESULT_S3_BUCKET, Key=filename)
        return True, f"Файл '{filename}' успешно удален"
        
    except ClientError as e:
        return False, f"Ошибка S3 при удалении: {e.response['Error']['Message']}"
    except Exception as e:
        return False, f"Ошибка при удалении: {str(e)}"

@app.route('/')
def index():
    """Главная страница с формой загрузки и списком файлов"""
    files = list_files_in_s3()
    return render_template('index.html', files=files, bucket_name=RESULT_S3_BUCKET)

# Измените функцию upload, чтобы добавить параметр success
@app.route('/upload', methods=['POST'])
def upload():
    """Обработчик загрузки файла"""
    if 'file' not in request.files:
        flash('Файл не выбран', 'error')
        return redirect(url_for('index'))
    
    file = request.files['file']
    
    if file.filename == '':
        flash('Файл не выбран', 'error')
        return redirect(url_for('index'))
    
    # Загружаем файл в S3
    success, message = upload_file_to_s3(file, file.filename)
    
    if success:
        flash(message, 'success')
        # Добавляем параметр для анимации
        return redirect(url_for('index') + '?upload=success')
    else:
        flash(message, 'error')
    
    return redirect(url_for('index'))

@app.template_filter('pretty_json_string')
def pretty_json_string(value):
    try:
        return json.dumps(value, indent=2, ensure_ascii=False)
    except (json.JSONDecodeError, TypeError):
        return value

@app.route('/delete/<path:filename>', methods=['POST'])
def delete_file(filename):
    """Удаление файла из S3"""
    success, message = delete_file_from_s3(filename)
    
    if success:
        flash(message, 'success')
    else:
        flash(message, 'error')
    
    return redirect(url_for('index'))

@app.route('/api/files')
def api_files():
    """API для получения списка файлов в формате JSON"""
    files = list_files_in_s3()
    return jsonify(files)

@app.route('/check_connection')
def check_connection():
    """Проверка подключения к S3"""
    try:
        s3_client = get_s3_client()
        if not s3_client:
            return jsonify({'status': 'error', 'message': 'Не удалось создать клиент S3'})
        
        # Пытаемся получить информацию о бакете
        s3_client.head_bucket(Bucket=RESULT_S3_BUCKET)
        return jsonify({'status': 'success', 'message': f'Подключение к бакету {RESULT_S3_BUCKET} успешно'})
    except ClientError as e:
        return jsonify({'status': 'error', 'message': f'Ошибка подключения: {e.response["Error"]["Message"]}'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)