import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from dotenv import load_dotenv
import httpx
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

load_dotenv()
app = FastAPI()
scheduler = AsyncIOScheduler()

auth = os.getenv('NEWS_API')
NEWSAPI_DAILY_LIMIT = 100        # лимит NewsAPI
COUNTRIES_COUNT = 3              # RU, EN, DE
CYCLES_PER_DAY = NEWSAPI_DAILY_LIMIT // COUNTRIES_COUNT  # 33

# --- Google News RSS (каждые 5 мин, 9:00–22:00) ---
async def fetch_google_ru():
    await fetch_google_news('Татнефть', 'ru', 'RU', 'RU:ru')

async def fetch_google_en():
    await fetch_google_news('Tatneft', 'en-US', 'US', 'US:en')

async def fetch_google_de():
    await fetch_google_news('Tatneft', 'de', 'DE', 'DE:de')

async def fetch_google_news(query, hl, gl, ceid):
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            'https://news.google.com/rss/search',
            params={
                'q': f'intitle:{query}+when:7d',
                'hl': hl,
                'gl': gl,
                'ceid': ceid
            },
            timeout=10.0,
            follow_redirects=True
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        items = []
        for item in root.find('channel').findall('item'):
            pub_str = item.findtext('pubDate', default='')
            try:
                if parsedate_to_datetime(pub_str) < week_ago:
                    continue
            except Exception:
                pass
            items.append({
                'title': item.findtext('title', default=''),
                'link': item.findtext('link', default=''),
                'pubDate': pub_str,
                'description': item.findtext('description', default=''),
                'source': item.findtext('source', default=''),
            })
        print(f"[{datetime.now()}] Google {gl}: {len(items)} articles")

# --- NewsAPI (равномерно, 33 цикла в день) ---
async def fetch_newsapi_ru():
    await fetch_newsapi('Татнефть', 'ru')

async def fetch_newsapi_en():
    await fetch_newsapi('Tatneft', 'en')

async def fetch_newsapi_de():
    await fetch_newsapi('Tatneft', 'de')

async def fetch_newsapi(query, lang):
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            'https://newsapi.org/v2/everything',
            params={
                'q': query,
                'language': lang,
                'from': week_ago_str,
                'sortBy': 'publishedAt',
                'pageSize': 20,
                'apiKey': auth
            },
            timeout=10.0
        )
        resp.raise_for_status()
        data = resp.json()
        print(f"[{datetime.now()}] NewsAPI {lang}: {data.get('totalResults', 0)} total, {len(data.get('articles', []))} fetched")

# --- FastAPI endpoints (для ручного вызова / отладки) ---
@app.get('/politics_government_ru')
async def get_news_ru():
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')
    async with httpx.AsyncClient() as client:
        resp_newsapi = await client.get(
            'https://newsapi.org/v2/everything',
            params={'q': 'Татнефть', 'language': 'ru', 'from': week_ago_str, 'sortBy': 'publishedAt', 'apiKey': auth},
            timeout=10.0
        )
        resp_newsapi.raise_for_status()

        resp_google = await client.get(
            'https://news.google.com/rss/search',
            params={'q': 'intitle:Татнефть+when:7d', 'hl': 'ru', 'gl': 'RU', 'ceid': 'RU:ru'},
            timeout=10.0, follow_redirects=True
        )
        resp_google.raise_for_status()

        root = ET.fromstring(resp_google.text)
        items = []
        for item in root.find('channel').findall('item'):
            pub_str = item.findtext('pubDate', default='')
            try:
                if parsedate_to_datetime(pub_str) < week_ago:
                    continue
            except Exception:
                pass
            items.append({
                'title': item.findtext('title', ''),
                'link': item.findtext('link', ''),
                'pubDate': pub_str,
                'description': item.findtext('description', ''),
                'source': item.findtext('source', ''),
            })

    return {'newsapi': resp_newsapi.json(), 'google': items}

@app.get('/politics_government_en')
async def get_news_en():
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')
    async with httpx.AsyncClient() as client:
        resp_newsapi = await client.get(
            'https://newsapi.org/v2/everything',
            params={'q': 'Tatneft', 'language': 'en', 'from': week_ago_str, 'sortBy': 'publishedAt', 'apiKey': auth},
            timeout=10.0
        )
        resp_newsapi.raise_for_status()

        resp_google = await client.get(
            'https://news.google.com/rss/search',
            params={'q': 'intitle:Tatneft+when:7d', 'hl': 'en-US', 'gl': 'US', 'ceid': 'US:en'},
            timeout=10.0, follow_redirects=True
        )
        resp_google.raise_for_status()

        root = ET.fromstring(resp_google.text)
        items = []
        for item in root.find('channel').findall('item'):
            pub_str = item.findtext('pubDate', default='')
            try:
                if parsedate_to_datetime(pub_str) < week_ago:
                    continue
            except Exception:
                pass
            items.append({
                'title': item.findtext('title', ''),
                'link': item.findtext('link', ''),
                'pubDate': pub_str,
                'description': item.findtext('description', ''),
                'source': item.findtext('source', ''),
            })

    return {'newsapi': resp_newsapi.json(), 'google': items}

@app.get('/business_tatneft_de')
async def get_news_de():
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')
    async with httpx.AsyncClient() as client:
        resp_newsapi = await client.get(
            'https://newsapi.org/v2/everything',
            params={'q': 'Tatneft', 'language': 'de', 'from': week_ago_str, 'sortBy': 'publishedAt', 'apiKey': auth},
            timeout=10.0
        )
        resp_newsapi.raise_for_status()

        resp_google = await client.get(
            'https://news.google.com/rss/search',
            params={'q': 'intitle:Tatneft+when:7d', 'hl': 'de', 'gl': 'DE', 'ceid': 'DE:de'},
            timeout=10.0, follow_redirects=True
        )
        resp_google.raise_for_status()

        root = ET.fromstring(resp_google.text)
        items = []
        for item in root.find('channel').findall('item'):
            pub_str = item.findtext('pubDate', default='')
            try:
                if parsedate_to_datetime(pub_str) < week_ago:
                    continue
            except Exception:
                pass
            items.append({
                'title': item.findtext('title', ''),
                'link': item.findtext('link', ''),
                'pubDate': pub_str,
                'description': item.findtext('description', ''),
                'source': item.findtext('source', ''),
            })

    return {'newsapi': resp_newsapi.json(), 'google': items}

# --- Запуск scheduler при старте ---
@app.on_event("startup")
async def start_scheduler():
    # Google News: каждые 5 минут с 9:00 до 22:00 (включительно)
    scheduler.add_job(fetch_google_ru, CronTrigger(minute='*/5', hour='9-22'))
    scheduler.add_job(fetch_google_en, CronTrigger(minute='*/5', hour='9-22'))
    scheduler.add_job(fetch_google_de, CronTrigger(minute='*/5', hour='9-22'))

    newsapi_interval = (24 * 60) // CYCLES_PER_DAY  # 43 минуты

    scheduler.add_job(fetch_newsapi_ru, CronTrigger(minute='0,24,48', hour='9-22'))
    scheduler.add_job(fetch_newsapi_en, CronTrigger(minute='0,24,48', hour='9-22'))
    scheduler.add_job(fetch_newsapi_de, CronTrigger(minute='0,24,48', hour='9-22'))

    scheduler.start()
    print(f"Scheduler started. NewsAPI interval: {newsapi_interval} min. Google: every 5 min (9-22).")

@app.on_event("shutdown")
async def stop_scheduler():
    scheduler.shutdown()