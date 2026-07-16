from fastapi import FastAPI
from dotenv import load_dotenv
from os import getenv
import httpx
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

load_dotenv()

app = FastAPI()

auth = getenv('NEWS_API')

# --- RU ---
@app.get('/politics_government_ru')
async def get_news_ru():
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')

    async with httpx.AsyncClient() as client:
        # NewsAPI
        resp_newsapi = await client.get(
            'https://newsapi.org/v2/everything',
            params={
                'q': 'Татнефть',
                'language': 'ru',
                'from': week_ago_str,
                'sortBy': 'publishedAt',
                'apiKey': auth
            },
            timeout=10.0
        )
        resp_newsapi.raise_for_status()
        news_api = resp_newsapi.json()

        # Google News RSS
        resp_google = await client.get(
            'https://news.google.com/rss/search',
            params={
                'q': 'intitle:Татнефть+when:7d',
                'hl': 'ru',
                'gl': 'RU',
                'ceid': 'RU:ru'
            },
            timeout=10.0,
            follow_redirects=True
        )
        resp_google.raise_for_status()

        root = ET.fromstring(resp_google.text)
        channel = root.find('channel')
        items = []

        for item in channel.findall('item'):
            pub_date_str = item.findtext('pubDate', default='')
            try:
                pub_date = parsedate_to_datetime(pub_date_str)
                if pub_date < week_ago:
                    continue
            except (ValueError, TypeError):
                pass

            items.append({
                'title': item.findtext('title', default=''),
                'link': item.findtext('link', default=''),
                'pubDate': pub_date_str,
                'description': item.findtext('description', default=''),
                'source': item.findtext('source', default=''),
            })

    return {
        'newsapi': news_api,
        'google': items
    }


# --- US ---
@app.get('/politics_government_en')
async def get_news_en():
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')

    async with httpx.AsyncClient() as client:
        # NewsAPI
        resp_newsapi = await client.get(
            'https://newsapi.org/v2/everything',
            params={
                'q': 'Tatneft',
                'language': 'en',
                'from': week_ago_str,
                'sortBy': 'publishedAt',
                'apiKey': auth
            },
            timeout=10.0
        )
        resp_newsapi.raise_for_status()
        news_api = resp_newsapi.json()

        # Google News RSS
        resp_google = await client.get(
            'https://news.google.com/rss/search',
            params={
                'q': 'intitle:Tatneft+when:7d',
                'hl': 'en-US',
                'gl': 'US',
                'ceid': 'US:en'
            },
            timeout=10.0,
            follow_redirects=True
        )
        resp_google.raise_for_status()

        root = ET.fromstring(resp_google.text)
        channel = root.find('channel')
        items = []

        for item in channel.findall('item'):
            pub_date_str = item.findtext('pubDate', default='')
            try:
                pub_date = parsedate_to_datetime(pub_date_str)
                if pub_date < week_ago:
                    continue
            except (ValueError, TypeError):
                pass

            items.append({
                'title': item.findtext('title', default=''),
                'link': item.findtext('link', default=''),
                'pubDate': pub_date_str,
                'description': item.findtext('description', default=''),
                'source': item.findtext('source', default=''),
            })

    return {
        'newsapi': news_api,
        'google': items
    }

# --- DE ---
@app.get('/business_tatneft_de')
async def get_news_de():
    # Дата 7 дней назад
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')

    async with httpx.AsyncClient() as client:
        # NewsAPI — фильтруем сразу на их стороне
        resp_newsapi = await client.get(
            'https://newsapi.org/v2/everything',
            params={
                'q': 'Tatneft',
                'language': 'de',
                'from': week_ago_str,      # ← только за последние 7 дней
                'sortBy': 'publishedAt',   # ← свежие сверху
                'apiKey': auth
            },
            timeout=10.0
        )
        resp_newsapi.raise_for_status()
        news_api = resp_newsapi.json()

        # Google News RSS — фильтруем через when:7d
        resp_google = await client.get(
            'https://news.google.com/rss/search',
            params={
                'q': 'intitle:Tatneft+when:7d',  # ← только за 7 дней
                'hl': 'de',
                'gl': 'DE',
                'ceid': 'DE:de'
            },
            timeout=10.0,
            follow_redirects=True
        )
        resp_google.raise_for_status()

        # XML → JSON + фильтр по дате на стороне Python
        root = ET.fromstring(resp_google.text)
        channel = root.find('channel')
        items = []

        for item in channel.findall('item'):
            pub_date_str = item.findtext('pubDate', default='')

            # Парсим дату RSS (Mon, 16 Jul 2026 12:00:00 GMT)
            try:
                pub_date = parsedate_to_datetime(pub_date_str)
                if pub_date < week_ago:
                    continue  # Пропускаем старше недели
            except (ValueError, TypeError):
                pass  # Если не распарсилось — оставляем

            items.append({
                'title': item.findtext('title', default=''),
                'link': item.findtext('link', default=''),
                'pubDate': pub_date_str,
                'description': item.findtext('description', default=''),
                'source': item.findtext('source', default=''),
            })

    return {
        'newsapi': news_api,
        'google': items
    }