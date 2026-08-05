"""Russian media RSS feed fetcher and parser."""

from __future__ import annotations

import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Dict, List, Optional

import httpx

from app.config import RSS_HEADERS, RUSSIAN_RSS_SOURCES
from app.logging_setup import get_logger
from app.retry import fetch_with_retry
from app.sources.filters import is_relevant_to_tatneft
from app.sources.rss_utils import (
    clean_html,
    extract_image_url_from_html,
    find_ns_text,
    normalize_categories,
)
from app.storage.pipeline import save_russian_rss_batch

logger = get_logger("tradebot.sources.russian_rss")

RIA_NS = {"ria": "http://rian.ru/ns"}


def _parse_published_at(pub_date_str: str, source_name: str) -> str:
    """Parse RSS pubDate to ISO format, falling back to UTC now."""
    if pub_date_str:
        try:
            return parsedate_to_datetime(pub_date_str).isoformat()
        except Exception as exc:
            logger.warning(
                "[%s] Error parsing date '%s': %s",
                source_name,
                pub_date_str,
                exc,
            )
    return datetime.now(timezone.utc).isoformat()


def parse_rss_to_schema(
    xml_bytes: bytes,
    source_name: str,
    source_url: str,
) -> List[Dict[str, Any]]:
    """Parse a Russian RSS feed into the internal article schema."""
    items: List[Dict[str, Any]] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        # Last-ditch attempt: decode as utf-8 replacing errors and wrap in a root.
        try:
            text = xml_bytes.decode("utf-8", errors="replace")
            root = ET.fromstring(f"<root>{text}</root>")
        except ET.ParseError as exc:
            logger.error("Error parsing RSS for source %s: %s", source_name, exc)
            return items

    channel = root.find("channel")
    if channel is None:
        return items

    for item in channel.findall("item"):
        title = (item.findtext("title", default="") or "").strip()
        link = (item.findtext("link", default="") or "").strip()
        guid = (item.findtext("guid", default="") or "").strip()
        pub_date_str = (item.findtext("pubDate", default="") or "").strip()

        categories = [
            cat.text.strip() for cat in item.findall("category") if cat.text
        ]
        categories = normalize_categories(categories)

        author = item.findtext("author")
        if author:
            author = author.strip()

        description_raw = item.findtext("description", default="") or ""
        description = clean_html(description_raw)

        if not is_relevant_to_tatneft(title, description):
            continue

        image_url: Optional[str] = None
        enclosure = item.find("enclosure")
        if enclosure is not None:
            enc_url = enclosure.get("url")
            if enc_url:
                image_url = enc_url
        if not image_url and description_raw:
            image_url = extract_image_url_from_html(description_raw)

        published_at = _parse_published_at(pub_date_str, source_name)

        priority_val = find_ns_text(item, "priority", RIA_NS)
        priority: Any = None
        if priority_val is not None:
            try:
                priority = int(priority_val)
            except ValueError:
                priority = priority_val

        type_val = find_ns_text(item, "type", RIA_NS)
        pdalink = item.findtext("pdalink")
        if pdalink:
            pdalink = pdalink.strip()

        items.append(
            {
                "source": source_name,
                "source_url": source_url,
                "title": title,
                "link": link,
                "guid": guid,
                "published_at": published_at,
                "description": description,
                "categories": categories,
                "image_url": image_url,
                "author": author,
                "priority": priority,
                "type": type_val,
                "pdalink": pdalink,
            }
        )
    return items


async def _fetch_one_russian_rss(
    name: str,
    url: str,
    client: httpx.AsyncClient,
) -> List[Dict[str, Any]]:
    """Fetch and parse a single Russian RSS source."""
    try:
        logger.info("Начало запроса RSS-ленты %s...", name)
        resp = await fetch_with_retry(client, "GET", url, headers=RSS_HEADERS)

        # Use bytes so ElementTree can honour the XML encoding declaration.
        items = parse_rss_to_schema(resp.content, name, url)
        saved_count = await save_russian_rss_batch(items, client)
        logger.info(
            "RSS %s: найдено %s, сохранено %s новых",
            name,
            len(items),
            saved_count,
        )
        return items
    except Exception as exc:
        logger.error("Ошибка при обработке источника RSS %s: %s", name, exc)
        return []


async def fetch_russian_rss(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Download and parse Russian media RSS feeds in parallel, saving new articles."""
    all_parsed_items: List[Dict[str, Any]] = []

    async with asyncio.TaskGroup() as tg:
        tasks = [
            tg.create_task(_fetch_one_russian_rss(name, url, client))
            for name, url in RUSSIAN_RSS_SOURCES.items()
        ]

    for task in tasks:
        all_parsed_items.extend(task.result())

    return all_parsed_items
