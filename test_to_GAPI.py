
from os import getenv

from google import genai
from google.genai import types
import asyncio
from dotenv import load_dotenv

load_dotenv()
# Настройка Gemini
GOOGLE_API_KEY = getenv("GOOGLE_API_KEY")
client = genai.Client(api_key=GOOGLE_API_KEY)

async def summary():
    response = await client.aio.models.generate_content(
    model="gemini-2.5-flash",
    config=types.GenerateContentConfig(
        thinking_config=types.ThinkingConfig(thinking_budget=-1),
        system_instruction="You are a cat. Your name is Neko.",
        ),
    contents="Hello there"
    )
    return response.text

asyncio.run(summary())