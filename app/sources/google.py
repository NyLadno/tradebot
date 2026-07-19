"""Google News RSS fetcher."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List

import httpx

from app.config import RSS_HEADERS, settings
from app.logging_setup import get_logger
from app.retry import fetch_with_retry
from app.storage.pipeline import save_google_batch

logger = get_logger("tradebot.sources.google")


def parse_google_rss(xml_text: str, week_ago: datetime) -> List[Dict[str, Any]]:
    """Parse Google RSS XML and drop items older than week_ago."""
    items: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        # Try to recover from malformed XML by wrapping in a root element.
        try:
            root = ET.fromstring(f"<root>{xml_text}</root>")
        except ET.ParseError as exc:
            logger.warning("Ошибка парсинга XML Google RSS: %s", exc)
            return items

    channel = root.find("channel")
    if channel is None:
        return items

    for item in channel.findall("item"):
        pub_str = item.findtext("pubDate", default="")
        try:
            if parsedate_to_datetime(pub_str) < week_ago:
                continue
        except Exception:
            pass

        items.append(
            {
                "title": item.findtext("title", default=""),
                "link": item.findtext("link", default=""),
                "pubDate": pub_str,
                "description": item.findtext("description", default=""),
                "source": item.findtext("source", default=""),
            }
        )
    return items


async def fetch_google_news(
    query: str,
    hl: str,
    gl: str,
    ceid: str,
    client: httpx.AsyncClient,
) -> List[Dict[str, Any]]:
    """Download Google News RSS, parse, and persist new articles."""
    week_ago = datetime.now(timezone.utc) - timedelta(days=settings.news_lookback_days)
    try:
        resp = await fetch_with_retry(
            client,
            "GET",
            "https://news.google.com/rss/search",
            params={"q": query, "hl": hl, "gl": gl, "ceid": ceid},
            headers=RSS_HEADERS,
        )
        items = parse_google_rss(resp.text, week_ago)

        saved_count = await save_google_batch(items, query, client)
        logger.info(
            "Google %s: найдено %s, сохранено %s новых",
            gl,
            len(items),
            saved_count,
        )
        return items
    except Exception as e:
        logger.error("Ошибка получения новостей Google %s: %s", gl, e)
        return []
