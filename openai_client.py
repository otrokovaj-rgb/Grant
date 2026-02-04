import openai
import os
from os import getenv
from dotenv import load_dotenv, find_dotenv

# find the .env file and load it 
load_dotenv(find_dotenv())
# access environment variable 

YANDEX_CLOUD_FOLDER =  getenv("YANDEX_CLOUD_FOLDER")
YANDEX_CLOUD_API_KEY = getenv("YANDEX_CLOUD_API_KEY")
YANDEX_CLOUD_MODEL = getenv("YANDEX_CLOUD_MODEL")

INSTRUCTION = """Ты — эксперт по анализу контента. Проанализируй предоставленный сценарий по строгим правилам:

1.  Тема: Определи основную тему сценария и вырази её одним обобщающим словом или короткой фразой (например: семья, патриотизм, экология, здоровый образ жизни, историческая память).
2.  Проверка ценностей: Проверь, соответствует ли сценарий традиционным ценностям РФ (жизнь, достоинство, патриотизм, крепкая семья, духовное над материальным, гуманизм, справедливость, историческая память и другие из списка). Если нет — сценарий отклоняется.
3.  Проверка на запреты: Отклони сценарий, если в нем есть прямое или косвенное описание, пропаганда или одобрение: секса, наркотиков, злоупотребления алкоголем, нецензурной лексики, насилия, разжигания розни, унижения традиционных ценностей.

Формат ответа: Только одна строка.
*   Если сценарий прошел: «ОДОБРЕНО: [Тема]».
*   Если сценарий отклонен: «ОТКЛОНЕНО: [Причина: несоответствие ценностям/запретный контент, цитата из текста]».

Ответ строго до 150 символов. Никаких пояснений."""

client = openai.OpenAI(
    api_key=YANDEX_CLOUD_API_KEY,
    base_url="https://rest-assistant.api.cloud.yandex.net/v1",
    project=YANDEX_CLOUD_FOLDER
)


def GetData(input):
    response = client.responses.create(
        model=f"gpt://{YANDEX_CLOUD_FOLDER}/{YANDEX_CLOUD_MODEL}",
        temperature=0.3,
        instructions=INSTRUCTION,
        input=input,
        max_output_tokens=500)
    return response.output_text 

