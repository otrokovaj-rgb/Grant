#!/usr/bin/env python3

from __future__ import annotations

import json
import pandas as pd
from datetime import datetime
from yandex_ai_studio_sdk import AIStudio

# Конфигурация
FOLDER_ID = "<идентификатор_каталога>"
API_TOKEN = "<API-ключ>"

# Инструкция для модели соответствует промту из предыдущего ответа
FINANCE_CHECK_INSTRUCTION = """
Ты — финансовый контролер. Проанализируй предоставленные финансовые отчеты.

ТЕКУЩИЙ ОТЧЕТ: {current_report_data}
ПРЕДЫДУЩИЙ ОТЧЕТ: {previous_report_data}

Выполни строго следующие проверки:
1. Проверь, что итоговая сумма в текущем отчете рассчитана верно.
2. Убедись, что все статьи расходов из отчета за предыдущий квартал присутствуют в текущем отчете.
3. Проверь по каждой статье расходов: сумма в текущем отчете должна быть равна или больше, чем в отчете за предыдущий квартал.

Формат ответа: Только один из двух вариантов:
- `Верно: все проверки пройдены.`
- `Ошибка: [Укажи, какая конкретно проверка не пройдена: неверная итоговая сумма / отсутствует статья "X" / снижение по статье "Y"].`

Ответ должен быть предельно лаконичным, без пояснений.
"""

def parse_financial_report(file_path: str) -> dict:
    """Парсит финансовый отчет из различных форматов"""
    file_path = file_path.lower()
    
    if file_path.endswith('.json'):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    elif file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
        return {
            'items': df.to_dict('records'),
            'total': df['amount'].sum() if 'amount' in df.columns else 0
        }
    
    elif file_path.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file_path)
        return {
            'items': df.to_dict('records'),
            'total': df['amount'].sum() if 'amount' in df.columns else 0
        }
    
    else:
        raise ValueError(f"Неподдерживаемый формат файла: {file_path}")

def format_report_for_prompt(report_data: dict) -> str:
    """Форматирует данные отчета для включения в промт"""
    if not report_data:
        return "Данные отсутствуют"
    
    formatted = []
    
    if 'items' in report_data and report_data['items']:
        formatted.append("Статьи расходов:")
        for item in report_data['items']:
            name = item.get('name', item.get('article', item.get('item', 'Без названия')))
            amount = item.get('amount', item.get('sum', item.get('value', 0)))
            formatted.append(f"  - {name}: {amount} руб.")
    
    if 'total' in report_data:
        formatted.append(f"Итоговая сумма: {report_data['total']} руб.")
    
    return "\n".join(formatted)

def analyze_financial_reports(current_report_path: str, previous_report_path: str):
    """Анализирует финансовые отчеты с помощью Yandex GPT"""
    
    # Инициализация SDK
    sdk = AIStudio(
        folder_id=FOLDER_ID,
        auth=API_TOKEN,
    )
    
    try:
        # Чтение и парсинг отчетов
        print("Чтение отчетов...")
        current_report = parse_financial_report(current_report_path)
        previous_report = parse_financial_report(previous_report_path)
        
        # Форматирование для промта
        current_text = format_report_for_prompt(current_report)
        previous_text = format_report_for_prompt(previous_report)
        
        # Создание промта с данными отчетов
        prompt = FINANCE_CHECK_INSTRUCTION.format(
            current_report_data=current_text,
            previous_report_data=previous_text
        )
        
        print("Анализ отчетов с помощью YandexGPT...")
        
        # Создание ассистента для финансового анализа
        assistant = sdk.assistants.create(
            "yandexgpt",
            instruction="Ты — финансовый контролер. Анализируй финансовые отчеты строго по инструкции.",
        )
        
        # Создание треда и отправка запроса
        thread = sdk.threads.create()
        thread.write(prompt)
        
        # Запуск ассистента
        run = assistant.run(thread)
        result = run.wait()
        
        # Вывод результата
        print("\n" + "="*50)
        print("РЕЗУЛЬТАТ ПРОВЕРКИ:")
        print("="*50)
        print(result.text)
        print("="*50)
        
        # Дополнительная проверка расчетов локально (опционально)
        perform_local_verification(current_report, previous_report)
        
        # Очистка
        thread.delete()
        assistant.delete()
        
    except FileNotFoundError as e:
        print(f"Ошибка: Файл не найден - {e}")
    except ValueError as e:
        print(f"Ошибка: {e}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")

def perform_local_verification(current_report: dict, previous_report: dict):
    """Дополнительная локальная проверка (для двойной проверки)"""
    print("\nДОПОЛНИТЕЛЬНАЯ ПРОВЕРКА:")
    
    try:
        # Проверка 1: Расчет итоговой суммы
        if 'items' in current_report:
            calculated_total = sum(item.get('amount', 0) for item in current_report['items'])
            reported_total = current_report.get('total', 0)
            
            if abs(calculated_total - reported_total) > 0.01:
                print(f"⚠️  Расхождение в итоговой сумме: {reported_total} руб. (в отчете) vs {calculated_total} руб. (расчет)")
        
        # Проверка 2: Наличие всех статей
        if 'items' in current_report and 'items' in previous_report:
            current_items = {item.get('name', '') for item in current_report['items']}
            previous_items = {item.get('name', '') for item in previous_report['items']}
            
            missing_items = previous_items - current_items
            if missing_items:
                print(f"⚠️  Отсутствующие статьи: {', '.join(missing_items)}")
        
        # Проверка 3: Сравнение сумм по статьям
        if 'items' in current_report and 'items' in previous_report:
            previous_dict = {item.get('name', ''): item.get('amount', 0) for item in previous_report['items']}
            
            for item in current_report['items']:
                name = item.get('name', '')
                current_amount = item.get('amount', 0)
                previous_amount = previous_dict.get(name, 0)
                
                if current_amount < previous_amount:
                    print(f"⚠️  Снижение по статье '{name}': {previous_amount} → {current_amount} руб.")
    
    except Exception as e:
        print(f"Ошибка при локальной проверке: {e}")

def main():
    """Основная функция"""
    print("ФИНАНСОВЫЙ КОНТРОЛЬ v1.0")
    print("="*50)
    
    # Ввод путей к файлам (можно заменить на аргументы командной строки)
    current_report_path = input("Путь к текущему отчету: ").strip()
    previous_report_path = input("Путь к отчету за предыдущий квартал: ").strip()
    
    # Проверка существования файлов
    import os
    if not os.path.exists(current_report_path):
        print(f"Ошибка: Файл '{current_report_path}' не найден")
        return
    
    if not os.path.exists(previous_report_path):
        print(f"Ошибка: Файл '{previous_report_path}' не найден")
        return
    
    # Анализ отчетов
    analyze_financial_reports(current_report_path, previous_report_path)
    
    # Генерация отчета о проверке
    generate_verification_report(current_report_path, previous_report_path)

def generate_verification_report(current_path: str, previous_path: str):
    """Генерирует отчет о проверке"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    report = f"""
ОТЧЕТ О ПРОВЕРКЕ ФИНАНСОВЫХ ОТЧЕТОВ
Дата проверки: {timestamp}
Текущий отчет: {current_path}
Предыдущий отчет: {previous_path}
Статус: Проверка завершена
"""
    
    # Сохранение отчета в файл
    report_filename = f"verification_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    with open(report_filename, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"\nОтчет о проверке сохранен в файл: {report_filename}")

if __name__ == "__main__":
    main()