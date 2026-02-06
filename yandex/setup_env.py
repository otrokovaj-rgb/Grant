#!/usr/bin/env python3
"""
Скрипт для настройки окружения
"""

import os
import json
from pathlib import Path

def setup_environment():
    """Настройка переменных окружения"""
    
    print("Настройка окружения Яндекс Облака")
    print("="*50)
    
    # Получение данных от пользователя
    config = {
        "AWS_ACCESS_KEY_ID": input("AWS Access Key ID (статический ключ): ").strip(),
        "AWS_SECRET_ACCESS_KEY": input("AWS Secret Access Key: ").strip(),
        "YC_BUCKET_NAME": input("Имя бакета Object Storage: ").strip(),
        "YC_FOLDER_ID": input("Идентификатор каталога Yandex Cloud: ").strip(),
        "YC_AI_STUDIO_TOKEN": input("API-ключ Yandex AI Studio: ").strip(),
    }
    
    # Сохранение в файл .env
    env_file = Path(".env")
    with open(env_file, "w") as f:
        for key, value in config.items():
            f.write(f"{key}={value}\n")
    
    # Установка переменных окружения
    for key, value in config.items():
        os.environ[key] = value
    
    # Создание директорий
    Path("temp_downloads").mkdir(exist_ok=True)
    Path("index_info").mkdir(exist_ok=True)
    Path("logs").mkdir(exist_ok=True)
    
    print(f"\nНастройка завершена. Конфигурация сохранена в {env_file}")
    print("\nПроверьте созданные директории:")
    print("  - temp_downloads/ - для временных файлов")
    print("  - index_info/ - для информации об индексах")
    print("  - logs/ - для логов")
    
    # Проверка доступности сервисов
    print("\nДля проверки работы выполните:")
    print("  python yandex_uploader.py --list-files")

if __name__ == "__main__":
    setup_environment()
    