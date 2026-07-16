import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from dotenv import load_dotenv
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import httpx
from supabase_handler import save_newsapi_batch, save_google_batch

load_dotenv()
app = FastAPI()
scheduler = AsyncIOScheduler()

auth = os.getenv('NEWS_API')

GOOGLE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
}


def parse_google_rss(xml_text: str, week_ago: datetime):
    items = []
    try:
        root = ET.fromstring(xml_text)
        channel = root.find('channel')
        if channel is None:
            return items
        for item in channel.findall('item'):
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
    except ET.ParseError:
        pass
    return items


# --- Google News (фоновая задача) ---
async def fetch_google_news(query, hl, gl, ceid):
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    try:
        async with httpx.AsyncClient(headers=GOOGLE_HEADERS, timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(
                'https://news.google.com/rss/search',
                params={'q': query, 'hl': hl, 'gl': gl, 'ceid': ceid}
            )
            resp.raise_for_status()
            items = parse_google_rss(resp.text, week_ago)

            # ← СОХРАНЕНИЕ В SUPABASE (автоматически)
            await save_google_batch(items, query)

            print(f"[{datetime.now()}] Google {gl}: {len(items)} articles")
    except Exception as e:
        print(f"[{datetime.now()}] Google {gl} error: {e}")


# --- NewsAPI (фоновая задача) ---
async def fetch_newsapi(query, lang):
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                'https://newsapi.org/v2/everything',
                params={
                    'q': query,
                    'language': lang,
                    'from': week_ago_str,
                    'sortBy': 'publishedAt',
                    'pageSize': 20,
                    'apiKey': auth
                }
            )
            resp.raise_for_status()
            data = resp.json()

            # ← СОХРАНЕНИЕ В SUPABASE (автоматически)
            await save_newsapi_batch(data.get("articles", []), query)

            print(f"[{datetime.now()}] NewsAPI {lang}: {data.get('totalResults', 0)} total")
    except Exception as e:
        print(f"[{datetime.now()}] NewsAPI {lang} error: {e}")


# --- Scheduler wrappers ---
async def fetch_google_ru(): await fetch_google_news('Татнефть', 'ru', 'RU', 'RU:ru')


async def fetch_google_en(): await fetch_google_news('Tatneft', 'en-US', 'US', 'US:en')


async def fetch_google_de(): await fetch_google_news('Tatneft', 'de', 'DE', 'DE:de')


async def fetch_newsapi_ru(): await fetch_newsapi('Татнефть', 'ru')


async def fetch_newsapi_en(): await fetch_newsapi('Tatneft', 'en')


async def fetch_newsapi_de(): await fetch_newsapi('Tatneft', 'de')


# --- Endpoints (для ручной проверки) ---
@app.get('/politics_government_ru')
async def get_news_ru():
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')
    google_items = []
    newsapi_data = None

    try:
        async with httpx.AsyncClient(headers=GOOGLE_HEADERS, timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(
                'https://news.google.com/rss/search',
                params={'q': 'Татнефть', 'hl': 'ru', 'gl': 'RU', 'ceid': 'RU:ru'}
            )
            resp.raise_for_status()
            google_items = parse_google_rss(resp.text, week_ago)
            await save_google_batch(google_items, "Татнефть")
    except Exception as e:
        print(f"[endpoint] Google RU error: {e}")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                'https://newsapi.org/v2/everything',
                params={'q': 'Татнефть', 'language': 'ru', 'from': week_ago_str, 'sortBy': 'publishedAt',
                        'apiKey': auth}
            )
            resp.raise_for_status()
            newsapi_data = resp.json()
            await save_newsapi_batch(newsapi_data.get("articles", []), "Татнефть")
    except Exception as e:
        print(f"[endpoint] NewsAPI RU error: {e}")

    return {'google': google_items, 'newsapi': newsapi_data}


@app.get('/politics_government_en')
async def get_news_en():
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')
    google_items = []
    newsapi_data = None

    try:
        async with httpx.AsyncClient(headers=GOOGLE_HEADERS, timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(
                'https://news.google.com/rss/search',
                params={'q': 'Tatneft', 'hl': 'en-US', 'gl': 'US', 'ceid': 'US:en'}
            )
            resp.raise_for_status()
            google_items = parse_google_rss(resp.text, week_ago)
            await save_google_batch(google_items, "Tatneft")
    except Exception as e:
        print(f"[endpoint] Google EN error: {e}")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                'https://newsapi.org/v2/everything',
                params={'q': 'Tatneft', 'language': 'en', 'from': week_ago_str, 'sortBy': 'publishedAt', 'apiKey': auth}
            )
            resp.raise_for_status()
            newsapi_data = resp.json()
            await save_newsapi_batch(newsapi_data.get("articles", []), "Tatneft")
    except Exception as e:
        print(f"[endpoint] NewsAPI EN error: {e}")

    return {'google': google_items, 'newsapi': newsapi_data}


@app.get('/business_tatneft_de')
async def get_news_de():
    week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    week_ago_str = week_ago.strftime('%Y-%m-%d')
    google_items = []
    newsapi_data = None

    try:
        async with httpx.AsyncClient(headers=GOOGLE_HEADERS, timeout=30.0, follow_redirects=True) as client:
            resp = await client.get(
                'https://news.google.com/rss/search',
                params={'q': 'Tatneft', 'hl': 'de', 'gl': 'DE', 'ceid': 'DE:de'}
            )
            resp.raise_for_status()
            google_items = parse_google_rss(resp.text, week_ago)
            await save_google_batch(google_items, "Tatneft")
    except Exception as e:
        print(f"[endpoint] Google DE error: {e}")

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.get(
                'https://newsapi.org/v2/everything',
                params={'q': 'Tatneft', 'language': 'de', 'from': week_ago_str, 'sortBy': 'publishedAt', 'apiKey': auth}
            )
            resp.raise_for_status()
            newsapi_data = resp.json()
            await save_newsapi_batch(newsapi_data.get("articles", []), "Tatneft")
    except Exception as e:
        print(f"[endpoint] NewsAPI DE error: {e}")

    return {'google': google_items, 'newsapi': newsapi_data}


# --- Scheduler ---
@app.on_event("startup")
async def start_scheduler():
    # Google News: каждые 5 минут с 9:00 до 22:00
    scheduler.add_job(fetch_google_ru, CronTrigger(minute='*/5', hour='9-22'))
    scheduler.add_job(fetch_google_en, CronTrigger(minute='*/5', hour='9-22'))
    scheduler.add_job(fetch_google_de, CronTrigger(minute='*/5', hour='9-22'))

    # NewsAPI: 3 раза в час (9:00–22:00)
    scheduler.add_job(fetch_newsapi_ru, CronTrigger(minute='0,24,48', hour='9-22'))
    scheduler.add_job(fetch_newsapi_en, CronTrigger(minute='0,24,48', hour='9-22'))
    scheduler.add_job(fetch_newsapi_de, CronTrigger(minute='0,24,48', hour='9-22'))

    scheduler.start()
    print("Scheduler started. Google: every 5 min (9-22). NewsAPI: phased (9-22).")


@app.on_event("shutdown")
async def stop_scheduler():
    scheduler.shutdown()