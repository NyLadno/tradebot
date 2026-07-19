"""FastAPI application and scheduled news fetchers."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Dict, List

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Query

from app.config import GOOGLE_JOBS, NEWSAPI_JOBS, settings
from app.http_client import close_http_client, get_http_client
from app.logging_setup import get_logger
from app.sources.google import fetch_google_news
from app.sources.newsapi import fetch_newsapi
from app.sources.russian_rss import fetch_russian_rss

logger = get_logger("tradebot.main")

scheduler = AsyncIOScheduler(timezone="Europe/Moscow")


def _add_windowed_job(func, args: list, minute_interval: str) -> None:
    """
    Schedule a job to run every ``minute_interval`` within 09:00–22:00 MSK.

    APScheduler's CronTrigger uses inclusive ranges, so we use two triggers:
    one for 09:00–21:55 and a single 22:00 fire to respect the exact window.
    """
    scheduler.add_job(func, CronTrigger(minute=minute_interval, hour="9-21"), args=args)
    scheduler.add_job(func, CronTrigger(minute="0", hour="22"), args=args)


async def _scheduled_google_fetch(
    query: str, hl: str, gl: str, ceid: str
) -> None:
    """Scheduler wrapper that always uses the current shared HTTP client."""
    await fetch_google_news(query, hl, gl, ceid, get_http_client())


async def _scheduled_newsapi_fetch(query: str, lang: str) -> None:
    """Scheduler wrapper that always uses the current shared HTTP client."""
    await fetch_newsapi(query, lang, get_http_client())


async def _scheduled_russian_rss_fetch() -> None:
    """Scheduler wrapper that always uses the current shared HTTP client."""
    await fetch_russian_rss(get_http_client())


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app lifecycle: start HTTP client, scheduler; shutdown cleanly."""
    client = get_http_client()

    for query, hl, gl, ceid in GOOGLE_JOBS:
        _add_windowed_job(
            _scheduled_google_fetch,
            args=[query, hl, gl, ceid],
            minute_interval="*/5",
        )

    for query, lang in NEWSAPI_JOBS:
        _add_windowed_job(
            _scheduled_newsapi_fetch,
            args=[query, lang],
            minute_interval="0,24,48",
        )

    _add_windowed_job(
        _scheduled_russian_rss_fetch,
        args=[],
        minute_interval="*/5",
    )

    scheduler.start()
    logger.info(
        "Планировщик запущен (MSK 09:00–22:00). "
        "Google/RSS: каждые 5 мин. NewsAPI: 0/24/48 мин."
    )

    yield

    scheduler.shutdown()
    logger.info("Планировщик остановлен.")
    await close_http_client()
    logger.info("HTTP-клиент закрыт.")


app = FastAPI(lifespan=lifespan)


async def _get_and_save_news(
    query: str,
    lang: str,
    hl: str,
    gl: str,
    ceid: str,
) -> Dict[str, Any]:
    """Fetch Google RSS and NewsAPI in parallel and persist new articles."""
    import asyncio

    client = get_http_client()
    google_items, newsapi_articles = await asyncio.gather(
        fetch_google_news(query, hl, gl, ceid, client),
        fetch_newsapi(query, lang, client),
    )
    return {
        "google": google_items,
        "newsapi": {
            "status": "ok",
            "totalResults": len(newsapi_articles),
            "articles": newsapi_articles,
        },
    }


@app.get("/politics_government_ru")
async def get_news_ru() -> Dict[str, Any]:
    """Fetch Russian-language Tatneft news."""
    return await _get_and_save_news("Татнефть", "ru", "ru", "RU", "RU:ru")


@app.get("/politics_government_en")
async def get_news_en() -> Dict[str, Any]:
    """Fetch English-language Tatneft news."""
    return await _get_and_save_news("Tatneft", "en", "en-US", "US", "US:en")


@app.get("/business_tatneft_de")
async def get_news_de() -> Dict[str, Any]:
    """Fetch German-language Tatneft news."""
    return await _get_and_save_news("Tatneft", "de", "de", "DE", "DE:de")


@app.get("/news")
async def get_news_dynamic(
    query: str = Query(..., description="Ключевое слово для поиска"),
    lang: str = Query("ru", description="Язык новостей (ru, en, de, etc.)"),
    country: str = Query("RU", description="Двухсимвольный код страны (RU, US, DE, etc.)"),
) -> Dict[str, Any]:
    """Dynamic endpoint to search and save arbitrary news."""
    hl = f"{lang}-{country}" if lang == "en" and country == "US" else lang
    gl = country
    ceid = f"{country}:{lang}"
    return await _get_and_save_news(query, lang, hl, gl, ceid)


@app.get("/fetch_russian_rss")
async def get_russian_rss() -> Dict[str, Any]:
    """Manually trigger the Russian RSS fetch."""
    client = get_http_client()
    items = await fetch_russian_rss(client)
    return {
        "status": "ok",
        "total_parsed": len(items),
        "articles": items,
    }
