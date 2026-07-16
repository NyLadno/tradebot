import os
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import httpx
from email.utils import parsedate_to_datetime

load_dotenv()

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL").rstrip("/")
SUPABASE_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY")
SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "news_alerts")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL и SUPABASE_KEY должны быть в .env")

REST_URL = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}"

HEADERS_GET = {
    "apikey": SUPABASE_KEY,
    "Accept-Profile": "public",
}

HEADERS_POST = {
    "apikey": SUPABASE_KEY,
    "Content-Type": "application/json",
    "Content-Profile": "public",
    "Prefer": "return=minimal",
}


async def is_duplicate(article_url: str) -> bool:
    if not article_url:
        return False
    try:
        async with httpx.AsyncClient(headers=HEADERS_GET, timeout=30.0) as client:
            resp = await client.get(
                REST_URL,
                params={"select": "id", "article_url": f"eq.{article_url}", "limit": 1},
            )
            resp.raise_for_status()
            return len(resp.json()) > 0
    except Exception as e:
        print(f"[DB] Ошибка проверки дубликата: {e}")
        return False


async def insert_news(
        source_api: str,
        query_keywords: str,
        article_title: str,
        article_url: str,
        article_published_at: Optional[datetime],
        raw_article: Dict[Any, Any],
) -> bool:
    if await is_duplicate(article_url):
        print(f"[DB] Пропущен дубликат: {article_url[:80]}...")
        return False

    try:
        now = datetime.now(timezone.utc)
        payload = {
            "source_api": source_api,
            "query_keywords": query_keywords or "",
            "article_title": article_title,
            "article_url": article_url,
            "article_published_at": article_published_at.isoformat() if article_published_at else None,
            "raw_article": raw_article,
            "initial_cooldown_until": (now + timedelta(hours=1)).isoformat(),
            "block_until": (now + timedelta(hours=1)).isoformat(),
            "is_blocked": True,
            "block_source": "AUTO",
            "telegram_sent": False,
        }
        async with httpx.AsyncClient(headers=HEADERS_POST, timeout=30.0) as client:
            resp = await client.post(REST_URL, json=payload)

            # ← ДЕБАГ: выводим тело ошибки
            if resp.status_code >= 400:
                print(f"[DB] HTTP {resp.status_code}: {resp.text[:500]}")
                return False

            resp.raise_for_status()
        print(f"[DB] Сохранено: {article_title[:60]}...")
        return True
    except httpx.TimeoutException:
        print(f"[DB] Таймаут при сохранении")
        return False
    except Exception as e:
        print(f"[DB] Ошибка вставки: {e}")
        return False


async def save_newsapi_batch(articles: List[Dict], query: str) -> int:
    saved = 0
    for art in articles:
        url = art.get("url")
        title = art.get("title")
        if not url or not title:
            continue

        # Парсим дату публикации из NewsAPI
        dt = datetime.now(timezone.utc)  # fallback — сейчас, если не распарсится
        published = art.get("publishedAt")
        if published:
            try:
                # Формат: 2026-07-16T12:00:00Z
                dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
            except Exception:
                pass

        if await insert_news("newsapi", query, title, url, dt, art):
            saved += 1
    return saved


async def save_google_batch(items: List[Dict], query: str) -> int:
    saved = 0
    for item in items:
        url = item.get("link")
        title = item.get("title")
        if not url or not title:
            continue

        # Парсим дату публикации из RSS
        dt = datetime.now(timezone.utc)  # fallback — сейчас, если не распарсится
        pub_str = item.get("pubDate")
        if pub_str:
            try:
                # Формат: Mon, 16 Jul 2026 12:00:00 GMT
                dt = parsedate_to_datetime(pub_str)
            except Exception:
                pass

        if await insert_news("google", query, title, url, dt, item):
            saved += 1
    return saved