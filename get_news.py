from fastapi import FastAPI
from dotenv import load_dotenv
from os import getenv
import httpx

load_dotenv()

app = FastAPI()

auth = getenv('NEWS_API')

@app.get('/politics_government_ru')
async def get_news_ru():
    if not auth:
        return {"error": "NEWS_API key is not configured in the environment or .env file."}
    news = []
    async with httpx.AsyncClient() as client:
        response = await client.get(
            'https://newsapi.org/v2/everything',
            params={'q': 'Татнефть', 'language': 'ru', 'apiKey': auth}
        )
        news.append(response.json())
    return news

@app.get('/politics_government_en')
async def get_news_en():
    if not auth:
        return {"error": "NEWS_API key is not configured in the environment or .env file."}
    news = []
    async with httpx.AsyncClient() as client:
        response = await client.get(
            'https://newsapi.org/v2/everything',
            params={'q': 'Tatneft', 'language': 'en', 'apiKey': auth}
        )
        news.append(response.json())
    return news

@app.get('/politics_government_de')
async def get_news_de():
    if not auth:
        return {"error": "NEWS_API key is not configured in the environment or .env file."}
    news = []
    async with httpx.AsyncClient() as client:
        response = await client.get(
            'https://newsapi.org/v2/everything',
            params={'q': 'Tatneft', 'language': 'de', 'apiKey': auth}
        )
        news.append(response.json())
    return news