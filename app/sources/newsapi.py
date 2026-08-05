"""NewsAPI fetcher."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import httpx

from app.config import settings
from app.logging_setup import get_logger
from app.retry import fetch_with_retry
from app.storage.pipeline import save_newsapi_batch

logger = get_logger("tradebot.sources.newsapi")


async def fetch_newsapi(
    query: str,
    lang: str,
    client: httpx.AsyncClient,
) -> List[Dict[str, Any]]:
    """Download NewsAPI articles and persist new ones."""
    week_ago = datetime.now(timezone.utc) - timedelta(days=settings.news_lookback_days)
    week_ago_str = week_ago.strftime("%Y-%m-%d")
    try:
        resp = await fetch_with_retry(
            client,
            "GET",
            "https://newsapi.org/v2/everything",
            params={
                "q": query,
                "language": lang,
                "from": week_ago_str,
                "sortBy": "publishedAt",
                "pageSize": 20,
                "apiKey": settings.news_api_key,
            },
        )
        data = resp.json()
        articles = data.get("articles", [])

        saved_count = await save_newsapi_batch(articles, query, client)
        logger.info(
            "NewsAPI %s: получено %s из %s, сохранено %s новых",
            lang,
            len(articles),
            data.get("totalResults", 0),
            saved_count,
        )
        return articles
    except Exception as e:
        logger.error("Ошибка получения новостей NewsAPI %s: %s", lang, e)
        return []
