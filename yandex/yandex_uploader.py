#!/usr/bin/env python3
"""
Загрузка файлов в Яндекс Облако Object Storage и создание поискового индекса для Yandex AI Search
"""

import os
import sys
import json
import logging
import tempfile
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional
import argparse

# Yandex Cloud SDK для Object Storage (только boto3)
import boto3
from botocore.client import Config

# Yandex AI Studio SDK
from yandex_ai_studio_sdk import AIStudio
from yandex_ai_studio_sdk.search_indexes import (
    StaticIndexChunkingStrategy,
    TextSearchIndexType,
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class YandexCloudUploader:
    """Класс для загрузки файлов в Яндекс Облако и создания поискового индекса"""
    
    def __init__(
        self,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        bucket_name: str,
        folder_id: str,
        ai_studio_token: str,
        region: str = "ru-central1",
        endpoint_url: str = "https://storage.yandexcloud.net"
    ):
        """
        Инициализация загрузчика
        
        Args:
            aws_access_key_id: Статический ключ доступа Yandex Cloud
            aws_secret_access_key: Секретный ключ доступа Yandex Cloud
            bucket_name: Имя бакета Object Storage
            folder_id: Идентификатор каталога Yandex Cloud
            ai_studio_token: API-ключ Yandex AI Studio
            region: Регион (по умолчанию ru-central1)
            endpoint_url: URL endpoint Object Storage
        """
        self.bucket_name = bucket_name
        self.folder_id = folder_id
        self.ai_studio_token = ai_studio_token
        
        # Инициализация клиента Object Storage (S3)
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region,
            config=Config(s3={'addressing_style': 'virtual'})
        )
        
        # Инициализация AI Studio SDK
        self.sdk = AIStudio(
            folder_id=folder_id,
            auth=ai_studio_token,
        )
        
        logger.info(f"Инициализирован клиент для бакета: {bucket_name}")
    
    def upload_file_to_storage(
        self, 
        local_file_path: str, 
        s3_key: Optional[str] = None
    ) -> str:
        """
        Загружает файл в Object Storage
        
        Args:
            local_file_path: Локальный путь к файлу
            s3_key: Ключ в S3 (путь в бакете). Если None, используется имя файла
            
        Returns:
            S3 ключ загруженного файла
        """
        try:
            if not os.path.exists(local_file_path):
                raise FileNotFoundError(f"Файл не найден: {local_file_path}")
            
            # Определяем S3 ключ
            if s3_key is None:
                s3_key = Path(local_file_path).name
            
            file_size = os.path.getsize(local_file_path)
            logger.info(f"Загрузка файла {local_file_path} ({file_size} байт) в s3://{self.bucket_name}/{s3_key}")
            
            # Загрузка файла
            self.s3_client.upload_file(
                local_file_path,
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'ACL': 'private',
                    'ContentType': self._get_content_type(local_file_path)
                }
            )
            
            logger.info(f"Файл успешно загружен: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла {local_file_path}: {e}")
            raise
    
    def upload_directory_to_storage(
        self, 
        local_dir_path: str, 
        s3_prefix: str = ""
    ) -> List[str]:
        """
        Рекурсивно загружает директорию в Object Storage
        
        Args:
            local_dir_path: Локальная директория
            s3_prefix: Префикс для ключей S3
            
        Returns:
            Список S3 ключей загруженных файлов
        """
        uploaded_keys = []
        
        try:
            local_dir = Path(local_dir_path)
            
            if not local_dir.exists():
                raise FileNotFoundError(f"Директория не найдена: {local_dir_path}")
            
            if not local_dir.is_dir():
                raise ValueError(f"Путь не является директорией: {local_dir_path}")
            
            # Рекурсивный обход директории
            for file_path in local_dir.rglob('*'):
                if file_path.is_file():
                    # Относительный путь для S3 ключа
                    relative_path = file_path.relative_to(local_dir)
                    s3_key = str(Path(s3_prefix) / relative_path)
                    
                    # Загрузка файла
                    s3_key = self.upload_file_to_storage(str(file_path), s3_key)
                    uploaded_keys.append(s3_key)
            
            logger.info(f"Загружено {len(uploaded_keys)} файлов из {local_dir_path}")
            return uploaded_keys
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке директории {local_dir_path}: {e}")
            raise
    
    def upload_files_to_ai_studio(
        self,
        file_paths: List[str],
        ttl_days: int = 30
    ) -> List:
        """
        Загружает файлы напрямую в AI Studio (без Object Storage)
        
        Args:
            file_paths: Список путей к файлам
            ttl_days: Количество дней хранения файлов
            
        Returns:
            Список объектов загруженных файлов
        """
        uploaded_files = []
        
        try:
            for file_path in file_paths:
                if not os.path.exists(file_path):
                    logger.warning(f"Файл не найден: {file_path}")
                    continue
                
                logger.info(f"Загрузка файла в AI Studio: {file_path}")
                
                file = self.sdk.files.upload(
                    file_path,
                    ttl_days=ttl_days,
                    expiration_policy="static",
                )
                uploaded_files.append(file)
                logger.info(f"Файл загружен с ID: {file.id}")
            
            return uploaded_files
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке файлов в AI Studio: {e}")
            raise
    
    def create_search_index(
        self,
        uploaded_files: List,
        index_name: Optional[str] = None,
        chunk_size_tokens: int = 1000,
        chunk_overlap_tokens: int = 200
    ) -> Dict:
        """
        Создает поисковый индекс из загруженных файлов
        
        Args:
            uploaded_files: Список файлов, загруженных в AI Studio
            index_name: Имя индекса
            chunk_size_tokens: Размер фрагмента в токенах
            chunk_overlap_tokens: Перекрытие фрагментов в токенах
            
        Returns:
            Информация о созданном индексе
        """
        try:
            if not uploaded_files:
                raise ValueError("Нет файлов для создания индекса")
            
            # Создание имени индекса
            if index_name is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                index_name = f"search_index_{timestamp}"
            
            logger.info(f"Создание поискового индекса: {index_name}")
            
            # Создание поискового индекса
            operation = self.sdk.search_indexes.create_deferred(
                uploaded_files,
                index_type=TextSearchIndexType(
                    chunking_strategy=StaticIndexChunkingStrategy(
                        max_chunk_size_tokens=chunk_size_tokens,
                        chunk_overlap_tokens=chunk_overlap_tokens,
                    )
                ),
            )
            
            # Ожидание создания индекса
            search_index = operation.wait()
            
            logger.info(f"Индекс создан: ID={search_index.id}")
            
            return {
                "index_name": index_name,
                "index_id": search_index.id,
                "index_object": search_index,
                "files": [{"id": f.id, "name": Path(f.original_name or "").name} for f in uploaded_files],
                "chunk_size": chunk_size_tokens,
                "chunk_overlap": chunk_overlap_tokens,
                "created_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Ошибка при создании индекса: {e}")
            raise
    
    def create_search_assistant(
        self,
        search_index,
        assistant_name: Optional[str] = None
    ) -> Dict:
        """
        Создает ассистента для поиска в индексе
        
        Args:
            search_index: Объект поискового индекса
            assistant_name: Имя ассистента
            
        Returns:
            Информация об ассистенте
        """
        try:
            if assistant_name is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                assistant_name = f"search_assistant_{timestamp}"
            
            # Создание инструмента поиска
            tool = self.sdk.tools.search_index(
                search_index,
                call_strategy={
                    "type": "function",
                    "function": {
                        "name": "document_search",
                        "instruction": "Используй этот инструмент для поиска информации в документах."
                    },
                },
            )
            
            # Создание ассистента
            assistant = self.sdk.assistants.create(
                "yandexgpt-latest",
                name=assistant_name,
                instruction="""Ты — ассистент по поиску в документах. 
                Используй инструмент поиска для ответов на вопросы пользователя.
                Если информация не найдена в документах, сообщи об этом.""",
                tools=[tool],
            )
            
            logger.info(f"Ассистент создан: ID={assistant.id}")
            
            return {
                "assistant_name": assistant_name,
                "assistant_id": assistant.id,
                "assistant_object": assistant,
                "created_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Ошибка при создании ассистента: {e}")
            raise
    
    def upload_and_create_index(
        self,
        file_paths: List[str],
        index_name: Optional[str] = None,
        chunk_size_tokens: int = 1000,
        chunk_overlap_tokens: int = 200,
        create_assistant: bool = True
    ) -> Dict:
        """
        Полный процесс: загрузка файлов и создание поискового индекса
        
        Args:
            file_paths: Список путей к файлам
            index_name: Имя индекса
            chunk_size_tokens: Размер фрагмента в токенах
            chunk_overlap_tokens: Перекрытие фрагментов в токенах
            create_assistant: Создавать ли ассистента
            
        Returns:
            Полная информация о созданных ресурсах
        """
        try:
            # 1. Загрузка файлов в AI Studio
            uploaded_files = self.upload_files_to_ai_studio(file_paths)
            
            # 2. Создание поискового индекса
            index_info = self.create_search_index(
                uploaded_files,
                index_name,
                chunk_size_tokens,
                chunk_overlap_tokens
            )
            
            result = {
                **index_info,
                "assistant_info": None
            }
            
            # 3. Создание ассистента (опционально)
            if create_assistant:
                assistant_info = self.create_search_assistant(
                    index_info["index_object"],
                    f"{index_info['index_name']}_assistant"
                )
                result["assistant_info"] = assistant_info
            
            # 4. Сохранение информации
            self._save_index_info(result)
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка в полном процессе: {e}")
            raise
    
    def create_index_from_storage(
        self,
        s3_keys: List[str],
        index_name: Optional[str] = None,
        download_dir: Optional[str] = None,
        create_assistant: bool = True
    ) -> Dict:
        """
        Создает индекс из файлов в Object Storage
        
        Args:
            s3_keys: Список ключей S3
            index_name: Имя индекса
            download_dir: Директория для временного хранения файлов
            create_assistant: Создавать ли ассистента
            
        Returns:
            Информация о созданном индексе
        """
        temp_files = []
        
        try:
            # Создаем временную директорию, если не указана
            if download_dir is None:
                temp_dir = tempfile.mkdtemp(prefix="yandex_index_")
                download_dir = temp_dir
            else:
                os.makedirs(download_dir, exist_ok=True)
            
            # Скачиваем файлы из Object Storage
            local_paths = []
            for s3_key in s3_keys:
                local_path = Path(download_dir) / Path(s3_key).name
                
                logger.info(f"Скачивание {s3_key} в {local_path}")
                self.s3_client.download_file(
                    self.bucket_name,
                    s3_key,
                    str(local_path)
                )
                
                local_paths.append(str(local_path))
                temp_files.append(str(local_path))
            
            # Создаем индекс из скачанных файлов
            result = self.upload_and_create_index(
                local_paths,
                index_name=index_name,
                create_assistant=create_assistant
            )
            
            # Добавляем информацию о S3 источниках
            result["s3_sources"] = [f"s3://{self.bucket_name}/{key}" for key in s3_keys]
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при создании индекса из S3: {e}")
            raise
        finally:
            # Очистка временных файлов
            if temp_files:
                for file_path in temp_files:
                    try:
                        os.remove(file_path)
                    except:
                        pass
    
    def list_bucket_files(self, prefix: str = "") -> List[Dict]:
        """
        Список файлов в бакете
        
        Args:
            prefix: Префикс для фильтрации
            
        Returns:
            Список файлов с информацией
        """
        try:
            files = []
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        files.append({
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].isoformat() if 'LastModified' in obj else None
                        })
            
            return files
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка файлов: {e}")
            raise
    
    def list_search_indexes(self) -> List[Dict]:
        """
        Список созданных поисковых индексов
        
        Returns:
            Список индексов
        """
        try:
            indexes = self.sdk.search_indexes.list()
            
            result = []
            for index in indexes:
                result.append({
                    'id': index.id,
                    'created_at': getattr(index, 'created_at', 'unknown'),
                    'status': getattr(index, 'status', 'unknown')
                })
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при получении списка индексов: {e}")
            return []
    
    def delete_index(self, index_id: str, delete_files: bool = True):
        """
        Удаляет поисковый индекс
        
        Args:
            index_id: ID индекса
            delete_files: Удалять ли связанные файлы
        """
        try:
            # Получение информации об индексе
            indexes = self.sdk.search_indexes.list()
            
            for index in indexes:
                if index.id == index_id:
                    # Удаление индекса
                    index.delete()
                    logger.info(f"Индекс {index_id} удален")
                    
                    if delete_files:
                        # Получаем список файлов в AI Studio и удаляем связанные
                        # В реальном приложении нужно хранить связь файлов с индексами
                        logger.info("Опция delete_files требует дополнительной реализации")
                    
                    return
            
            logger.warning(f"Индекс {index_id} не найден")
            
        except Exception as e:
            logger.error(f"Ошибка при удалении индекса: {e}")
            raise
    
    def search_in_index(
        self, 
        index_id: str, 
        query: str, 
        assistant_id: Optional[str] = None,
        limit: int = 5
    ) -> List[Dict]:
        """
        Поиск в индексе
        
        Args:
            index_id: ID индекса
            query: Поисковый запрос
            assistant_id: ID ассистента (если не указан, создается временный)
            limit: Максимальное количество результатов
            
        Returns:
            Список результатов поиска
        """
        try:
            # Находим индекс
            indexes = self.sdk.search_indexes.list()
            search_index = None
            
            for index in indexes:
                if index.id == index_id:
                    search_index = index
                    break
            
            if not search_index:
                raise ValueError(f"Индекс с ID {index_id} не найден")
            
            assistant = None
            thread = None
            
            try:
                if assistant_id:
                    # Используем существующего ассистента
                    assistant = self.sdk.assistants.get(assistant_id)
                else:
                    # Создаем временного ассистента
                    tool = self.sdk.tools.search_index(
                        search_index,
                        call_strategy={
                            "type": "function",
                            "function": {
                                "name": "search_documents",
                                "instruction": "Поиск в документах"
                            },
                        },
                    )
                    
                    assistant = self.sdk.assistants.create(
                        "yandexgpt-latest",
                        instruction="Используй инструмент поиска для ответа на запрос.",
                        tools=[tool],
                    )
                
                # Создаем тред и выполняем поиск
                thread = self.sdk.threads.create()
                thread.write(query)
                
                run = assistant.run(thread)
                result = run.wait()
                
                # Извлекаем результаты поиска
                search_results = []
                if hasattr(result, 'citations') and result.citations:
                    for citation in result.citations:
                        if hasattr(citation, 'sources'):
                            for source in citation.sources:
                                if source.type == "filechunk":
                                    search_results.append({
                                        'text': source.parts,
                                        'file_id': source.file.id if hasattr(source.file, 'id') else None,
                                        'index_id': source.search_index.id if hasattr(source.search_index, 'id') else None
                                    })
                
                return search_results[:limit]
                
            finally:
                # Очистка временных ресурсов
                if thread and (not assistant_id or not assistant):
                    thread.delete()
                if assistant and (not assistant_id):
                    assistant.delete()
            
        except Exception as e:
            logger.error(f"Ошибка при поиске: {e}")
            raise
    
    def _get_content_type(self, file_path: str) -> str:
        """Определяет Content-Type по расширению файла"""
        ext = Path(file_path).suffix.lower()
        
        content_types = {
            '.pdf': 'application/pdf',
            '.doc': 'application/msword',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.txt': 'text/plain',
            '.csv': 'text/csv',
            '.json': 'application/json',
            '.xml': 'application/xml',
            '.html': 'text/html',
            '.md': 'text/markdown',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.png': 'image/png',
            '.gif': 'image/gif',
        }
        
        return content_types.get(ext, 'application/octet-stream')
    
    def _save_index_info(self, index_info: Dict):
        """Сохраняет информацию об индексе в JSON файл"""
        info_dir = Path("./index_info")
        info_dir.mkdir(exist_ok=True)
        
        info_file = info_dir / f"index_info_{index_info['index_id']}.json"
        
        with open(info_file, 'w', encoding='utf-8') as f:
            # Преобразуем объекты в строки для сериализации
            serializable_info = index_info.copy()
            if 'index_object' in serializable_info:
                del serializable_info['index_object']
            if 'assistant_info' in serializable_info and serializable_info['assistant_info']:
                if 'assistant_object' in serializable_info['assistant_info']:
                    del serializable_info['assistant_info']['assistant_object']
            
            json.dump(serializable_info, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Информация об индексе сохранена в {info_file}")

def main():
    """Основная функция CLI"""
    parser = argparse.ArgumentParser(
        description='Загрузка файлов в Яндекс Облако и создание поискового индекса',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Примеры использования:
  # Загрузка файлов и создание индекса (прямо в AI Studio)
  python yandex_uploader.py --upload file1.pdf file2.docx --create-index
  
  # Загрузка файлов в Object Storage
  python yandex_uploader.py --upload-to-s3 file1.pdf --s3-key myfolder/file1.pdf
  
  # Создание индекса из файлов в S3
  python yandex_uploader.py --s3-keys document.pdf report.docx --from-s3 --create-index
  
  # Список файлов в бакете
  python yandex_uploader.py --list-files --s3-prefix documents/
  
  # Список созданных индексов
  python yandex_uploader.py --list-indexes
  
  # Поиск в индексе
  python yandex_uploader.py --search "ключевые слова" --index-id index_id_123
  
  # Удаление индекса
  python yandex_uploader.py --delete-index index_id_123
        '''
    )
    
    # Группа аутентификации
    auth_group = parser.add_argument_group('Аутентификация')
    auth_group.add_argument('--aws-key-id', 
                          help='AWS Access Key ID (статический ключ Yandex Cloud)')
    auth_group.add_argument('--aws-secret', 
                          help='AWS Secret Access Key')
    auth_group.add_argument('--bucket',
                          help='Имя бакета Object Storage')
    auth_group.add_argument('--folder-id', required=True,
                          help='Идентификатор каталога Yandex Cloud')
    auth_group.add_argument('--ai-token', required=True,
                          help='API-ключ Yandex AI Studio')
    
    # Группа загрузки
    upload_group = parser.add_argument_group('Загрузка файлов')
    upload_group.add_argument('--upload', nargs='+',
                            help='Загрузить файлы в AI Studio и создать индекс')
    upload_group.add_argument('--upload-to-s3', nargs='+',
                            help='Загрузить файлы в Object Storage')
    upload_group.add_argument('--s3-prefix', default='',
                            help='Префикс для файлов в S3')
    upload_group.add_argument('--s3-key', 
                            help='Конкретный S3 ключ для загрузки файла')
    
    # Группа индекса
    index_group = parser.add_argument_group('Управление индексом')
    index_group.add_argument('--create-index', action='store_true',
                           help='Создать поисковый индекс из загруженных файлов')
    index_group.add_argument('--index-name',
                           help='Имя для создаваемого индекса')
    index_group.add_argument('--s3-keys', nargs='+',
                           help='S3 ключи для создания индекса')
    index_group.add_argument('--from-s3', action='store_true',
                           help='Использовать файлы из S3 для создания индекса')
    index_group.add_argument('--chunk-size', type=int, default=1000,
                           help='Размер фрагмента в токенах')
    index_group.add_argument('--chunk-overlap', type=int, default=200,
                           help='Перекрытие фрагментов в токенах')
    index_group.add_argument('--no-assistant', action='store_true',
                           help='Не создавать ассистента')
    
    # Группа управления
    manage_group = parser.add_argument_group('Управление')
    manage_group.add_argument('--list-files', action='store_true',
                            help='Список файлов в бакете')
    manage_group.add_argument('--list-indexes', action='store_true',
                            help='Список созданных индексов')
    manage_group.add_argument('--search',
                            help='Поиск в индексе')
    manage_group.add_argument('--index-id',
                            help='ID индекса для поиска или удаления')
    manage_group.add_argument('--assistant-id',
                            help='ID ассистента для поиска')
    manage_group.add_argument('--delete-index', action='store_true',
                            help='Удалить индекс')
    manage_group.add_argument('--limit', type=int, default=5,
                            help='Лимит результатов поиска')
    
    args = parser.parse_args()
    
    try:
        # Проверка обязательных аргументов для работы с S3
        if (args.upload_to_s3 or args.from_s3 or args.list_files) and not (args.aws_key_id and args.aws_secret and args.bucket):
            print("Для работы с Object Storage требуется указать --aws-key-id, --aws-secret и --bucket")
            return
        
        # Инициализация загрузчика
        uploader = YandexCloudUploader(
            aws_access_key_id=args.aws_key_id or "",
            aws_secret_access_key=args.aws_secret or "",
            bucket_name=args.bucket or "",
            folder_id=args.folder_id,
            ai_studio_token=args.ai_token
        )
        
        print("\n" + "="*60)
        print("Яндекс Облако: Загрузчик файлов и создание индексов")
        print("="*60)
        
        # Загрузка файлов в S3
        uploaded_keys = []
        if args.upload_to_s3:
            print(f"\nЗагрузка файлов в Object Storage...")
            for file_path in args.upload_to_s3:
                s3_key = args.s3_key if args.s3_key else None
                s3_key = uploader.upload_file_to_storage(file_path, s3_key)
                uploaded_keys.append(s3_key)
        
        # Список файлов в бакете
        if args.list_files:
            print(f"\nСписок файлов в бакете {args.bucket}:")
            files = uploader.list_bucket_files(args.s3_prefix)
            for i, file_info in enumerate(files, 1):
                print(f"{i}. {file_info['key']} ({file_info['size']} байт)")
        
        # Список индексов
        if args.list_indexes:
            print(f"\nСписок поисковых индексов:")
            indexes = uploader.list_search_indexes()
            if indexes:
                for i, index_info in enumerate(indexes, 1):
                    print(f"{i}. ID: {index_info['id']}, Статус: {index_info['status']}")
            else:
                print("Индексы не найдены")
        
        # Создание индекса
        if args.create_index:
            print("\nСоздание поискового индекса...")
            
            if args.from_s3 and args.s3_keys:
                print(f"Использование файлов из S3: {args.s3_keys}")
                result = uploader.create_index_from_storage(
                    s3_keys=args.s3_keys,
                    index_name=args.index_name,
                    chunk_size_tokens=args.chunk_size,
                    chunk_overlap_tokens=args.chunk_overlap,
                    create_assistant=not args.no_assistant
                )
            elif args.upload:
                print(f"Загрузка файлов в AI Studio: {args.upload}")
                result = uploader.upload_and_create_index(
                    file_paths=args.upload,
                    index_name=args.index_name,
                    chunk_size_tokens=args.chunk_size,
                    chunk_overlap_tokens=args.chunk_overlap,
                    create_assistant=not args.no_assistant
                )
            else:
                print("Нет файлов для создания индекса. Используйте --upload или --s3-keys с --from-s3")
                return
            
            print("\n" + "="*60)
            print("ИНДЕКС УСПЕШНО СОЗДАН:")
            print("="*60)
            print(f"Имя индекса: {result['index_name']}")
            print(f"ID индекса: {result['index_id']}")
            if result.get('assistant_info'):
                print(f"ID ассистента: {result['assistant_info']['assistant_id']}")
            print(f"Количество файлов: {len(result['files'])}")
            print(f"Размер фрагмента: {result['chunk_size']} токенов")
            print(f"Перекрытие фрагментов: {result['chunk_overlap']} токенов")
            print("="*60)
            
            # Сохранение информации в файл
            info_file = f"{result['index_name']}_info.json"
            with open(info_file, 'w', encoding='utf-8') as f:
                json.dump({k: v for k, v in result.items() if not k.endswith('_object')}, f, ensure_ascii=False, indent=2)
            print(f"\nИнформация сохранена в {info_file}")
        
        # Поиск в индексе
        if args.search and args.index_id:
            print(f"\nПоиск в индексе {args.index_id}: {args.search}")
            results = uploader.search_in_index(
                index_id=args.index_id,
                query=args.search,
                assistant_id=args.assistant_id,
                limit=args.limit
            )
            
            print(f"\nНайдено результатов: {len(results)}")
            for i, result in enumerate(results, 1):
                print(f"\n{i}. Результат:")
                # Обрезаем длинный текст для вывода
                text_preview = result['text'][:300] + "..." if len(result['text']) > 300 else result['text']
                print(f"Текст: {text_preview}")
                if result.get('file_id'):
                    print(f"ID файла: {result['file_id']}")
        
        # Удаление индекса
        if args.delete_index and args.index_id:
            print(f"\nУдаление индекса {args.index_id}...")
            confirm = input("Вы уверены? (y/n): ")
            if confirm.lower() == 'y':
                uploader.delete_index(args.index_id, delete_files=True)
                print("Индекс удален")
        
        if not any([args.upload_to_s3, args.list_files, args.list_indexes, 
                   args.create_index, args.search, args.delete_index]):
            print("\nНе указана операция. Используйте --help для справки.")
        
        print("\nОперация завершена!")
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()