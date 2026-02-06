import os
import sys
import json
import logging
import asyncio
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Callable, Optional
import boto3
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, MessageHandler, filters, ContextTypes


@dataclass
class S3Config:
    """Configuration class for S3 connection"""

    upload_bucket_name: str
    remote_file_path: str = ""
    key_id: str = ""
    key_secret: str = ""
    endpoint: str = "https://storage.yandexcloud.net/"


class S3Poller:
    """Класс для опроса S3 bucket на наличие новых файлов"""

    def __init__(
        self,
        config: S3Config,
        bucket_name: str,
        prefix: str = "",
        callback: Optional[Callable] = None,
    ):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=config.key_id,
            aws_secret_access_key=config.key_secret,
            endpoint_url=config.endpoint,
        )
        self.bucket = bucket_name
        self.prefix = prefix
        self.callback = callback
        self.seen_files = set()

    def check_new_files(self, since_minutes: int = 5) -> list:
        """Проверяет новые файлы, созданные за последние N минут"""
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
        new_files = []

        try:
            paginator = self.s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket, Prefix=self.prefix)

            for page in pages:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    if obj["LastModified"] > cutoff_time:
                        if obj["Key"] not in self.seen_files:
                            new_files.append(obj["Key"])
                            self.seen_files.add(obj["Key"])

        except Exception as e:
            print(f"Ошибка при проверке S3: {e}")

        return new_files


class S3Client:
    """S3 client class for file operations."""

    def __init__(self, config: S3Config, logger):
        self.config = config
        self.logger = logger

        try:
            self.client = boto3.client(
                "s3",
                aws_access_key_id=config.key_id,
                aws_secret_access_key=config.key_secret,
                endpoint_url=config.endpoint,
            )
        except Exception as e:
            self.logger.error(f"Error initializing S3 client: {e}")
            raise

    def check_file_exists(self, bucket_name, file_path) -> bool:
        """Check if file exists in S3 bucket."""
        try:
            print(file_path)
            self.client.head_object(Bucket=bucket_name, Key=file_path)
            return True
        except:
            return False

    def upload_file(self, bucket_name, file_name) -> bool:
        """Upload local file to S3 bucket."""
        try:
            if not os.path.exists(file_name):
                raise FileNotFoundError(f"File {file_name} not found")

            file_size = os.path.getsize(file_name)
            self.logger.info(f"File size to upload: {file_size} bytes")

            self.client.upload_file(file_name, bucket_name, file_name)

            self.logger.info(f"File successfully uploaded to {bucket_name}/{file_name}")

            if self.check_file_exists(bucket_name, file_name):
                self.logger.info("Upload verification: file exists in S3")
                return True
            else:
                self.logger.error("File not found in S3 after upload")
                return False

        except Exception as e:
            self.logger.error(f"Error uploading file: {e}")
            raise

    def get_response(self, bucket_name, file_path) -> str:
        """Get response from S3."""
        try:
            print(file_path)
            response = self.client.get_object(Bucket=bucket_name, Key=file_path)
            object_content = response["Body"].read().decode("utf-8")
            data = json.loads(object_content)
            return data["report"]
        except Exception as e:
            self.logger.error(f"Error getting response: {e}")
            raise


# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


async def wait_for_file_and_get_response(uploader, filename, max_attempts=20, delay=10):
    """Асинхронно ждет появления файла и получает ответ"""
    for attempt in range(max_attempts):
        if uploader.check_file_exists("processed-results", filename):
            text = uploader.get_response("processed-results", filename)
            return text
        logger.info(f"Ожидание файла... попытка {attempt + 1}/{max_attempts}")
        await asyncio.sleep(delay)

    return "Превышено время ожидания обработки файла"


async def handle_wrong_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает полученные документы."""
    await update.message.reply_text(
        "Не верный формат документа", reply_to_message_id=update.message.id
    )


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает полученные документы."""
    try:
        # Получаем объект файла
        file = await update.message.document.get_file()

        # Формируем имя файла
        original_filename = update.message.document.file_name
        safe_filename = Path(original_filename).name
        file_path = str(safe_filename)

        # Скачиваем файл
        await file.download_to_drive(file_path)
        logger.info(f"Файл {safe_filename} сохранен в {file_path}")

        await update.message.reply_text(f"Файл получен. Начинаю обработку...")

        # Инициализируем S3 клиент
        s3_config = S3Config(
            key_id=os.getenv("KEY_ID"),
            key_secret=os.getenv("KEY_SECRET"),
            upload_bucket_name=os.getenv("UPLOAD_BUCKET_NAME"),
        )

        uploader = S3Client(s3_config, logger)

        # Загружаем файл в S3
        uploader.upload_file("source-documents", file_path)

        # Ожидаем результат
        text = await wait_for_file_and_get_response(
            uploader, f"reports/report_{safe_filename}.json"
        )

        await update.message.reply_text(text, reply_to_message_id=update.message.id)

        # Опционально удаляем локальный файл после обработки
        os.remove(file_path)
        logger.info(f"Локальный файл {file_path} удален")

    except Exception as e:
        logger.error(f"Ошибка при обработке документа: {e}")
        await update.message.reply_text(
            f"Произошла ошибка при обработке файла: {str(e)}"
        )


def Main():
    load_dotenv()

    # Создаем приложение
    application = Application.builder().token(os.getenv("BOT_TOKEN")).build()

    # Регистрируем обработчики
    application.add_handler(MessageHandler(filters.Document.PDF, handle_document))
    application.add_handler(MessageHandler(filters.Document.ALL, handle_wrong_document))

    # Запускаем бота
    logger.info("Бот запущен и готов к работе...")
    application.run_polling(allowed_updates=Update.ALL_TYPES)


