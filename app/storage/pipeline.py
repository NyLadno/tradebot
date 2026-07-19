"""Shared news save pipeline: map → dedupe → LLM evaluate → insert → notify."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, List, Optional

import httpx

from app.config import settings
from app.logging_setup import get_logger
from app.storage.supabase import get_duplicate_urls, insert_news_batch

logger = get_logger("tradebot.pipeline")

Mapper = Callable[[Dict[str, Any], datetime], Optional[Dict[str, Any]]]

_llm_evaluator = None


def get_llm_evaluator():
    """Lazy singleton for LLMRiskEvaluator (optional)."""
    global _llm_evaluator
    if _llm_evaluator is not None:
        return _llm_evaluator
    if not settings.enable_llm_evaluation:
        return None
    try:
        from app.llm.evaluator import LLMRiskEvaluator

        _llm_evaluator = LLMRiskEvaluator()
    except Exception as exc:
        logger.error("[LLM] Не удалось инициализировать LLMRiskEvaluator: %s", exc)
        return None
    return _llm_evaluator


def _base_payload(
    *,
    source_api: str,
    query_keywords: str,
    title: str,
    url: str,
    published_at: str,
    raw_article: Dict[str, Any],
    now: datetime,
) -> Dict[str, Any]:
    """Create a base payload for the news_alerts table.

    Fail-safe default: every article starts blocked until the LLM decides it is
    safe.  This prevents a disabled or failing LLM from silently allowing
    dangerous news through.
    """
    cooldown = (now + timedelta(hours=settings.cooldown_hours)).isoformat()
    return {
        "source_api": source_api,
        "query_keywords": query_keywords,
        "article_title": title,
        "article_url": url,
        "article_published_at": published_at,
        "raw_article": raw_article,
        "initial_cooldown_until": cooldown,
        "block_until": cooldown,
        "is_blocked": True,
        "block_source": "AUTO",
        "telegram_sent": False,
    }


def map_newsapi_article(
    art: Dict[str, Any],
    query: str,
    now: datetime,
) -> Optional[Dict[str, Any]]:
    """Map a NewsAPI article to the internal payload schema."""
    url = art.get("url")
    title = art.get("title")
    if not url or not title:
        return None

    dt = now
    published = art.get("publishedAt")
    if published:
        try:
            dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
        except Exception:
            pass

    return _base_payload(
        source_api="newsapi",
        query_keywords=query or "",
        title=title,
        url=url,
        published_at=dt.isoformat(),
        raw_article=art,
        now=now,
    )


def map_google_item(
    item: Dict[str, Any],
    query: str,
    now: datetime,
) -> Optional[Dict[str, Any]]:
    """Map a Google RSS item to the internal payload schema."""
    url = item.get("link")
    title = item.get("title")
    if not url or not title:
        return None

    dt = now
    pub_str = item.get("pubDate")
    if pub_str:
        try:
            dt = parsedate_to_datetime(pub_str)
        except Exception:
            pass

    return _base_payload(
        source_api="google",
        query_keywords=query or "",
        title=title,
        url=url,
        published_at=dt.isoformat(),
        raw_article=item,
        now=now,
    )


def map_russian_rss_item(
    item: Dict[str, Any],
    now: datetime,
) -> Optional[Dict[str, Any]]:
    """Map a Russian RSS item to the internal payload schema."""
    url = item.get("link")
    title = item.get("title")
    if not url or not title:
        return None

    published_at = item.get("published_at") or now.isoformat()
    return _base_payload(
        source_api="rss_russian",
        query_keywords=item.get("source", ""),
        title=title,
        url=url,
        published_at=published_at,
        raw_article=item,
        now=now,
    )


async def process_and_save(
    payloads: List[Dict[str, Any]],
    client: httpx.AsyncClient,
    source_label: str,
) -> int:
    """Dedupe payloads against DB, optionally run LLM evaluation, then insert."""
    if not payloads:
        return 0

    urls = [p["article_url"] for p in payloads]
    duplicates = await get_duplicate_urls(urls, client)

    seen_urls: set = set()
    filtered: List[Dict[str, Any]] = []
    for p in payloads:
        u = p["article_url"]
        if u not in duplicates and u not in seen_urls:
            seen_urls.add(u)
            filtered.append(p)

    if not filtered:
        logger.info(
            "[DB] Все новости (%s) из %s батча уже существуют в базе. Пропущено.",
            len(payloads),
            source_label,
        )
        return 0

    evaluator = get_llm_evaluator()
    if evaluator:
        logger.info(
            "[LLM] Запуск оценки для %s новых статей из %s...",
            len(filtered),
            source_label,
        )
        filtered = await evaluator.evaluate_news_batch(filtered, http_client=client)

    return await insert_news_batch(filtered, client)


async def _with_client(
    client: Optional[httpx.AsyncClient],
    coro_factory,
):
    """Run a coroutine with the supplied client or a temporary one."""
    if client is None:
        async with httpx.AsyncClient() as temp_client:
            return await coro_factory(temp_client)
    return await coro_factory(client)


async def save_newsapi_batch(
    articles: List[Dict[str, Any]],
    query: str,
    client: Optional[httpx.AsyncClient] = None,
) -> int:
    """Map NewsAPI articles and run the shared save pipeline."""

    async def _run(c: httpx.AsyncClient) -> int:
        now = datetime.now(timezone.utc)
        payloads = [
            p
            for art in articles
            if (p := map_newsapi_article(art, query, now)) is not None
        ]
        return await process_and_save(payloads, c, "NewsAPI")

    if not articles:
        return 0
    return await _with_client(client, _run)


async def save_google_batch(
    items: List[Dict[str, Any]],
    query: str,
    client: Optional[httpx.AsyncClient] = None,
) -> int:
    """Map Google RSS items and run the shared save pipeline."""

    async def _run(c: httpx.AsyncClient) -> int:
        now = datetime.now(timezone.utc)
        payloads = [
            p for item in items if (p := map_google_item(item, query, now)) is not None
        ]
        return await process_and_save(payloads, c, "Google")

    if not items:
        return 0
    return await _with_client(client, _run)


async def save_russian_rss_batch(
    items: List[Dict[str, Any]],
    client: Optional[httpx.AsyncClient] = None,
) -> int:
    """Map Russian RSS items and run the shared save pipeline."""

    async def _run(c: httpx.AsyncClient) -> int:
        now = datetime.now(timezone.utc)
        payloads = [
            p for item in items if (p := map_russian_rss_item(item, now)) is not None
        ]
        return await process_and_save(payloads, c, "российского RSS")

    if not items:
        return 0
    return await _with_client(client, _run)
