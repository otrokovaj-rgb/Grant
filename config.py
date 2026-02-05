# config.py
# Конфигурационные параметры

FOLDER_ID = "ваш_идентификатор_каталога"
API_TOKEN = "ваш_api_ключ"

# Настройки для различных форматов отчетов
REPORT_CONFIG = {
    'required_columns': ['name', 'amount'],
    'amount_column_aliases': ['amount', 'sum', 'value', 'total'],
    'name_column_aliases': ['name', 'article', 'item', 'description'],
    'default_currency': 'руб.'
}