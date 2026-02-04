import openai
import os
from os import getenv
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

YANDEX_CLOUD_FOLDER =  getenv("YANDEX_CLOUD_FOLDER")
YANDEX_CLOUD_API_KEY = getenv("YANDEX_CLOUD_API_KEY")
YANDEX_CLOUD_MODEL = getenv("YANDEX_CLOUD_MODEL")

INSTRUCTION = getenv("INSTRUCTION")

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

