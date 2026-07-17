import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Set
from dotenv import load_dotenv
import httpx
from email.utils import parsedate_to_datetime

# Настройка логирования
logger = logging.getLogger("tradebot.supabase")
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

load_dotenv()

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL", "").rstrip("/")
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


async def get_duplicate_urls(urls: List[str], client: httpx.AsyncClient) -> Set[str]:
    """
    Проверяет, какие из переданных URL уже существуют в базе данных, за ОДИН запрос.
    """
    if not urls:
        return set()

    # Экранируем двойные кавычки для синтаксиса PostgREST "in"
    escaped_urls = []
    for url in urls:
        escaped = url.replace('"', '""')
        escaped_urls.append(f'"{escaped}"')

    in_filter = f"in.({','.join(escaped_urls)})"

    try:
        resp = await client.get(
            REST_URL,
            headers=HEADERS_GET,
            params={"select": "article_url", "article_url": in_filter},
            timeout=30.0
        )
        resp.raise_for_status()
        existing = {row["article_url"] for row in resp.json() if "article_url" in row}
        return existing
    except Exception as e:
        logger.error(f"[DB] Ошибка при проверке дубликатов пакетом: {e}")
        return set()


async def insert_single_news(payload: Dict[str, Any], client: httpx.AsyncClient) -> bool:
    """
    Вспомогательная функция для вставки одной записи (используется при фолбеке).
    """
    try:
        url = payload.get("article_url")
        # Дополнительная проверка на дубликат перед одиночной вставкой
        if url:
            resp_check = await client.get(
                REST_URL,
                headers=HEADERS_GET,
                params={"select": "id", "article_url": f"eq.{url}", "limit": 1},
                timeout=10.0
            )
            if resp_check.status_code == 200 and len(resp_check.json()) > 0:
                logger.info(f"[DB] Пропущен дубликат (фолбек): {url[:80]}...")
                return False

        resp = await client.post(REST_URL, headers=HEADERS_POST, json=payload, timeout=20.0)
        if resp.status_code >= 400:
            logger.error(f"[DB] Ошибка вставки одиночной статьи. HTTP {resp.status_code}: {resp.text[:500]}")
            return False
        resp.raise_for_status()
        logger.info(f"[DB] Сохранено (фолбек): {payload.get('article_title', '')[:60]}...")
        return True
    except Exception as e:
        logger.error(f"[DB] Ошибка при вставке одиночной новости: {e}")
        return False


async def insert_news_batch(payloads: List[Dict[str, Any]], client: httpx.AsyncClient) -> int:
    """
    Выполняет пакетную вставку записей в базу данных за один запрос.
    Если пакетный запрос падает, автоматически переходит на поштучную вставку (фолбек).
    """
    if not payloads:
        return 0

    try:
        resp = await client.post(REST_URL, headers=HEADERS_POST, json=payloads, timeout=30.0)
        if resp.status_code >= 400:
            logger.warning(f"[DB] Пакетная вставка вернула HTTP {resp.status_code}: {resp.text[:500]}. Переход на поштучный режим.")
            # Фолбек на одиночную вставку
            inserted_count = 0
            for payload in payloads:
                if await insert_single_news(payload, client):
                    inserted_count += 1
            return inserted_count

        resp.raise_for_status()
        logger.info(f"[DB] Успешно сохранено пакетом: {len(payloads)} новостей.")
        return len(payloads)
    except Exception as e:
        logger.error(f"[DB] Ошибка пакетной вставки: {e}. Переход на поштучный режим.")
        inserted_count = 0
        for payload in payloads:
            if await insert_single_news(payload, client):
                inserted_count += 1
        return inserted_count


async def save_newsapi_batch(articles: List[Dict[str, Any]], query: str, client: Optional[httpx.AsyncClient] = None) -> int:
    """
    Мапит, фильтрует дубликаты и сохраняет пакет статей из NewsAPI.
    """
    if not articles:
        return 0

    if client is None:
        async with httpx.AsyncClient() as temp_client:
            return await _save_newsapi_batch_impl(articles, query, temp_client)
    return await _save_newsapi_batch_impl(articles, query, client)


async def _save_newsapi_batch_impl(articles: List[Dict[str, Any]], query: str, client: httpx.AsyncClient) -> int:
    payloads = []
    urls = []
    now = datetime.now(timezone.utc)

    for art in articles:
        url = art.get("url")
        title = art.get("title")
        if not url or not title:
            continue

        # Парсим дату публикации из NewsAPI
        dt = now
        published = art.get("publishedAt")
        if published:
            try:
                dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
            except Exception:
                pass

        payload = {
            "source_api": "newsapi",
            "query_keywords": query or "",
            "article_title": title,
            "article_url": url,
            "article_published_at": dt.isoformat(),
            "raw_article": art,
            "initial_cooldown_until": (now + timedelta(hours=1)).isoformat(),
            "block_until": (now + timedelta(hours=1)).isoformat(),
            "is_blocked": True,
            "block_source": "AUTO",
            "telegram_sent": False,
        }
        payloads.append(payload)
        urls.append(url)

    if not payloads:
        return 0

    # Проверяем дубликаты в БД за 1 запрос
    duplicates = await get_duplicate_urls(urls, client)

    # Фильтруем дубликаты в памяти, включая локальные дубликаты внутри одного батча
    seen_urls = set()
    filtered_payloads = []
    for p in payloads:
        u = p["article_url"]
        if u not in duplicates and u not in seen_urls:
            seen_urls.add(u)
            filtered_payloads.append(p)

    if not filtered_payloads:
        logger.info(f"[DB] Все новости ({len(payloads)}) из NewsAPI батча уже существуют в базе. Пропущено.")
        return 0

    return await insert_news_batch(filtered_payloads, client)


async def save_google_batch(items: List[Dict[str, Any]], query: str, client: Optional[httpx.AsyncClient] = None) -> int:
    """
    Мапит, фильтрует дубликаты и сохраняет пакет статей из Google RSS.
    """
    if not items:
        return 0

    if client is None:
        async with httpx.AsyncClient() as temp_client:
            return await _save_google_batch_impl(items, query, temp_client)
    return await _save_google_batch_impl(items, query, client)


async def _save_google_batch_impl(items: List[Dict[str, Any]], query: str, client: httpx.AsyncClient) -> int:
    payloads = []
    urls = []
    now = datetime.now(timezone.utc)

    for item in items:
        url = item.get("link")
        title = item.get("title")
        if not url or not title:
            continue

        # Парсим дату публикации из RSS
        dt = now
        pub_str = item.get("pubDate")
        if pub_str:
            try:
                dt = parsedate_to_datetime(pub_str)
            except Exception:
                pass

        payload = {
            "source_api": "google",
            "query_keywords": query or "",
            "article_title": title,
            "article_url": url,
            "article_published_at": dt.isoformat(),
            "raw_article": item,
            "initial_cooldown_until": (now + timedelta(hours=1)).isoformat(),
            "block_until": (now + timedelta(hours=1)).isoformat(),
            "is_blocked": True,
            "block_source": "AUTO",
            "telegram_sent": False,
        }
        payloads.append(payload)
        urls.append(url)

    if not payloads:
        return 0

    # Проверяем дубликаты в БД за 1 запрос
    duplicates = await get_duplicate_urls(urls, client)

    # Фильтруем дубликаты в памяти, включая локальные дубликаты внутри одного батча
    seen_urls = set()
    filtered_payloads = []
    for p in payloads:
        u = p["article_url"]
        if u not in duplicates and u not in seen_urls:
            seen_urls.add(u)
            filtered_payloads.append(p)

    if not filtered_payloads:
        logger.info(f"[DB] Все новости ({len(payloads)}) из Google батча уже существуют в базе. Пропущено.")
        return 0

    return await insert_news_batch(filtered_payloads, client)


async def save_russian_rss_batch(items: List[Dict[str, Any]], client: Optional[httpx.AsyncClient] = None) -> int:
    """
    Мапит, фильтрует дубликаты и сохраняет пакет статей из российских RSS лент.
    Каждая статья уже приведена к схеме выходных данных RSS-новостей.
    """
    if not items:
        return 0

    if client is None:
        async with httpx.AsyncClient() as temp_client:
            return await _save_russian_rss_batch_impl(items, temp_client)
    return await _save_russian_rss_batch_impl(items, client)


async def _save_russian_rss_batch_impl(items: List[Dict[str, Any]], client: httpx.AsyncClient) -> int:
    payloads = []
    urls = []
    now = datetime.now(timezone.utc)

    for item in items:
        url = item.get("link")
        title = item.get("title")
        if not url or not title:
            continue

        published_at = item.get("published_at")
        if not published_at:
            published_at = now.isoformat()

        payload = {
            "source_api": "rss_russian",
            "query_keywords": item.get("source", ""),
            "article_title": title,
            "article_url": url,
            "article_published_at": published_at,
            "raw_article": item,
            "initial_cooldown_until": (now + timedelta(hours=1)).isoformat(),
            "block_until": (now + timedelta(hours=1)).isoformat(),
            "is_blocked": True,
            "block_source": "AUTO",
            "telegram_sent": False,
        }
        payloads.append(payload)
        urls.append(url)

    if not payloads:
        return 0

    # Проверяем дубликаты в БД за 1 запрос
    duplicates = await get_duplicate_urls(urls, client)

    # Фильтруем дубликаты в памяти, включая локальные дубликаты внутри одного батча
    seen_urls = set()
    filtered_payloads = []
    for p in payloads:
        u = p["article_url"]
        if u not in duplicates and u not in seen_urls:
            seen_urls.add(u)
            filtered_payloads.append(p)

    if not filtered_payloads:
        logger.info(f"[DB] Все новости ({len(payloads)}) из российского RSS батча уже существуют в базе. Пропущено.")
        return 0

    return await insert_news_batch(filtered_payloads, client)



