"""Low-level Supabase REST helpers."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Union

import httpx

from app.config import settings
from app.logging_setup import get_logger

logger = get_logger("tradebot.supabase")

settings.validate_supabase()

HEADERS_GET = {
    "apikey": settings.supabase_key,
    "Accept-Profile": "public",
}

HEADERS_POST = {
    "apikey": settings.supabase_key,
    "Content-Type": "application/json",
    "Content-Profile": "public",
    "Prefer": "return=representation",
}

HEADERS_PATCH = {
    "apikey": settings.supabase_key,
    "Content-Type": "application/json",
    "Content-Profile": "public",
    "Prefer": "return=minimal",
}


def _build_in_filter(urls: List[str]) -> str:
    """Build a PostgREST 'in.' filter safely escaping double quotes."""
    escaped = [f'"{url.replace(chr(34), chr(34) * 2)}"' for url in urls]
    return f"in.({','.join(escaped)})"


async def select_rows(
    table: str,
    client: httpx.AsyncClient,
    *,
    select: str = "*",
    filters: Optional[Dict[str, str]] = None,
    order: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Generic PostgREST SELECT against any table."""
    params: Dict[str, Any] = {"select": select}
    if filters:
        params.update(filters)
    if order:
        params["order"] = order
    if limit is not None:
        params["limit"] = limit

    resp = await client.get(
        settings.rest_url(table), headers=HEADERS_GET, params=params, timeout=30.0
    )
    resp.raise_for_status()
    return resp.json()


async def insert_rows(
    table: str,
    payload: Union[Dict[str, Any], List[Dict[str, Any]]],
    client: httpx.AsyncClient,
) -> List[Dict[str, Any]]:
    """Generic PostgREST INSERT (single row or batch) against any table."""
    resp = await client.post(
        settings.rest_url(table), headers=HEADERS_POST, json=payload, timeout=30.0
    )
    resp.raise_for_status()
    return resp.json()


async def upsert_rows(
    table: str,
    payload: Union[Dict[str, Any], List[Dict[str, Any]]],
    client: httpx.AsyncClient,
    *,
    on_conflict: str,
    ignore_duplicates: bool = False,
) -> List[Dict[str, Any]]:
    """Generic PostgREST UPSERT against a unique constraint.

    ``on_conflict`` is a comma-separated column list matching a unique index,
    e.g. ``"symbol,timestamp"`` for the candles table. Set
    ``ignore_duplicates`` to keep the existing row instead of overwriting it.
    """
    resolution = "ignore-duplicates" if ignore_duplicates else "merge-duplicates"
    headers = {**HEADERS_POST, "Prefer": f"return=representation,resolution={resolution}"}
    resp = await client.post(
        settings.rest_url(table),
        headers=headers,
        params={"on_conflict": on_conflict},
        json=payload,
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()


async def update_rows(
    table: str,
    filters: Dict[str, str],
    patch: Dict[str, Any],
    client: httpx.AsyncClient,
) -> None:
    """Generic PostgREST UPDATE (matching rows) against any table."""
    resp = await client.patch(
        settings.rest_url(table),
        headers=HEADERS_PATCH,
        params=filters,
        json=patch,
        timeout=30.0,
    )
    resp.raise_for_status()


async def get_duplicate_urls(urls: List[str], client: httpx.AsyncClient) -> Set[str]:
    """Return the subset of URLs that already exist in the database (one request)."""
    if not urls:
        return set()

    try:
        resp = await client.get(
            settings.supabase_rest_url,
            headers=HEADERS_GET,
            params={"select": "article_url", "article_url": _build_in_filter(urls)},
            timeout=30.0,
        )
        resp.raise_for_status()
        return {row["article_url"] for row in resp.json() if "article_url" in row}
    except Exception as exc:
        logger.error("[DB] Ошибка при проверке дубликатов пакетом: %s", exc)
        return set()


async def _maybe_notify_telegram(payload: Dict[str, Any], client: httpx.AsyncClient) -> None:
    """Notify Telegram for blocked records that were just inserted."""
    if not payload.get("is_blocked"):
        return

    try:
        # Import here to avoid a circular import at module load time.
        from app.telegram_bot import get_telegram_bot

        bot = get_telegram_bot()
        await bot.notify_blocked_news(payload, client)
    except Exception as exc:
        logger.error("[Telegram] Не удалось отправить уведомление: %s", exc)


def _assign_ids_from_response(
    payloads: List[Dict[str, Any]],
    inserted: List[Dict[str, Any]],
) -> None:
    """Match inserted DB rows back to payloads by URL and attach the generated id."""
    url_to_payload = {p["article_url"]: p for p in payloads}
    for record in inserted:
        url = record.get("article_url")
        payload = url_to_payload.get(url)
        if payload is not None and "id" in record:
            payload["id"] = record["id"]


async def insert_single_news(payload: Dict[str, Any], client: httpx.AsyncClient) -> bool:
    """Insert one row; used as a fallback when batch insert fails."""
    try:
        url = payload.get("article_url")
        if url:
            resp_check = await client.get(
                settings.supabase_rest_url,
                headers=HEADERS_GET,
                params={"select": "id", "article_url": f"eq.{url}", "limit": 1},
                timeout=10.0,
            )
            if resp_check.status_code == 200 and len(resp_check.json()) > 0:
                logger.info("[DB] Пропущен дубликат (фолбек): %s...", url[:80])
                # The existing record is responsible for its own notification.
                return False

        resp = await client.post(
            settings.supabase_rest_url,
            headers=HEADERS_POST,
            json=payload,
            timeout=20.0,
        )
        if resp.status_code >= 400:
            logger.error(
                "[DB] Ошибка вставки одиночной статьи. HTTP %s: %s",
                resp.status_code,
                resp.text[:500],
            )
            return False
        resp.raise_for_status()

        inserted = resp.json()
        if inserted and isinstance(inserted, list) and len(inserted) > 0:
            payload["id"] = inserted[0].get("id")

        # Telegram notification for newly inserted blocked records only.
        if payload.get("is_blocked"):
            await _maybe_notify_telegram(payload, client)

        logger.info(
            "[DB] Сохранено (фолбек): %s...",
            (payload.get("article_title") or "")[:60],
        )
        return True
    except Exception as exc:
        logger.error("[DB] Ошибка при вставке одиночной новости: %s", exc)
        return False


async def insert_news_batch(payloads: List[Dict[str, Any]], client: httpx.AsyncClient) -> int:
    """
    Batch-insert rows.  Falls back to one-by-one insert on failure.
    Notifies Telegram for newly inserted blocked records.
    """
    if not payloads:
        return 0

    try:
        resp = await client.post(
            settings.supabase_rest_url,
            headers=HEADERS_POST,
            json=payloads,
            timeout=30.0,
        )
        if resp.status_code >= 400:
            logger.warning(
                "[DB] Пакетная вставка вернула HTTP %s: %s. Переход на поштучный режим.",
                resp.status_code,
                resp.text[:500],
            )
            return await _insert_one_by_one(payloads, client)

        resp.raise_for_status()

        inserted = resp.json()
        if inserted and isinstance(inserted, list):
            _assign_ids_from_response(payloads, inserted)

        # Notify only for records we just inserted and that are blocked.
        for payload in payloads:
            if payload.get("is_blocked") and payload.get("id"):
                await _maybe_notify_telegram(payload, client)

        logger.info("[DB] Успешно сохранено пакетом: %s новостей.", len(payloads))
        return len(payloads)
    except Exception as exc:
        logger.error(
            "[DB] Ошибка пакетной вставки: %s. Переход на поштучный режим.", exc
        )
        return await _insert_one_by_one(payloads, client)


async def _insert_one_by_one(
    payloads: List[Dict[str, Any]], client: httpx.AsyncClient
) -> int:
    inserted = 0
    for payload in payloads:
        if await insert_single_news(payload, client):
            inserted += 1
    return inserted


async def mark_telegram_sent(news_id: int, client: httpx.AsyncClient) -> None:
    """Update telegram_sent = True for a given news_alerts row."""
    patch_url = f"{settings.supabase_rest_url}?id=eq.{news_id}"
    resp = await client.patch(
        patch_url,
        headers=HEADERS_PATCH,
        json={"telegram_sent": True},
        timeout=20.0,
    )
    resp.raise_for_status()
    logger.info("[TG] Флаг telegram_sent обновлён для news_alerts.id=%s", news_id)
