"""Storage helpers for the ``news_alerts`` table.

Здесь собран весь доступ к новостной таблице: вставка статей из пайплайна,
дедупликация по ``article_url``, флаг отправки в Telegram и выборка активных
блокировок для торгового движка.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from app.config import settings
from app.logging_setup import get_logger
from app.storage.supabase import Rows, get_supabase

logger = get_logger("tradebot.storage.news_alerts")

TABLE = settings.supabase_table

# Уникальный индекс news_alerts_article_url_key делает вставку идемпотентной:
# Google- и RSS-фетчеры ходят каждые 5 минут и пересекаются по ссылкам, а
# проверка дублей на стороне приложения подвержена гонке между ними.
ON_CONFLICT = "article_url"


async def get_duplicate_urls(urls: List[str]) -> Set[str]:
    """Return the subset of URLs that already exist in the database (one request).

    Ошибку наружу не глушим: пустой ответ пайплайн трактует как «всё новое»
    и переоценивает весь батч через LLM, а затем перевставляет его. Отказ БД
    не должен превращаться в лишние вызовы Gemini и мусор в таблице.
    """
    if not urls:
        return set()

    supabase = await get_supabase()
    resp = await (
        supabase.table(TABLE).select("article_url").in_("article_url", urls).execute()
    )
    return {row["article_url"] for row in resp.data if row.get("article_url")}


async def insert_news_batch(payloads: List[Dict[str, Any]]) -> int:
    """Upsert a batch of articles; falls back to one-by-one insert on failure.

    Notifies Telegram for newly inserted blocked records.
    """
    if not payloads:
        return 0

    supabase = await get_supabase()
    try:
        resp = await (
            supabase.table(TABLE)
            .upsert(payloads, on_conflict=ON_CONFLICT, ignore_duplicates=True)
            .execute()
        )
    except Exception as exc:  # noqa: BLE001 — падаем в поштучный режим
        # 42P10 здесь означает, что миграция 001 ещё не применена и уникального
        # индекса на article_url нет: ON CONFLICT не к чему привязаться.
        logger.error(
            "[DB] Ошибка пакетной вставки: %s. Переход на поштучный режим. "
            "Если это 42P10 — примените migrations/001_news_alerts_unique_url.sql.",
            exc,
        )
        return await _insert_one_by_one(payloads)

    # При ignore_duplicates PostgREST возвращает только реально вставленные
    # строки, поэтому их количество и есть число новых новостей.
    inserted = resp.data or []
    _assign_ids_from_response(payloads, inserted)

    inserted_urls = {row.get("article_url") for row in inserted}
    for payload in payloads:
        if payload.get("article_url") in inserted_urls and payload.get("is_blocked"):
            await _maybe_notify_telegram(payload)

    logger.info("[DB] Успешно сохранено пакетом: %s новостей.", len(inserted))
    return len(inserted)


async def insert_single_news(payload: Dict[str, Any]) -> bool:
    """Insert one row; used as a fallback when batch insert fails."""
    supabase = await get_supabase()
    try:
        url = payload.get("article_url")
        if url:
            check = await (
                supabase.table(TABLE)
                .select("id")
                .eq("article_url", url)
                .limit(1)
                .execute()
            )
            if check.data:
                logger.info("[DB] Пропущен дубликат (фолбек): %s...", url[:80])
                # The existing record is responsible for its own notification.
                return False

        resp = await supabase.table(TABLE).insert(payload).execute()
        if resp.data:
            payload["id"] = resp.data[0].get("id")

        # Telegram notification for newly inserted blocked records only.
        if payload.get("is_blocked"):
            await _maybe_notify_telegram(payload)

        logger.info(
            "[DB] Сохранено (фолбек): %s...",
            (payload.get("article_title") or "")[:60],
        )
        return True
    except Exception as exc:  # noqa: BLE001 — одна статья не валит батч
        logger.error("[DB] Ошибка при вставке одиночной новости: %s", exc)
        return False


async def _insert_one_by_one(payloads: List[Dict[str, Any]]) -> int:
    inserted = 0
    for payload in payloads:
        if await insert_single_news(payload):
            inserted += 1
    return inserted


def _assign_ids_from_response(
    payloads: List[Dict[str, Any]],
    inserted: Rows,
) -> None:
    """Match inserted DB rows back to payloads by URL and attach the generated id."""
    url_to_payload = {p["article_url"]: p for p in payloads}
    for record in inserted:
        payload = url_to_payload.get(record.get("article_url"))
        if payload is not None and "id" in record:
            payload["id"] = record["id"]


async def _maybe_notify_telegram(payload: Dict[str, Any]) -> None:
    """Notify Telegram for blocked records that were just inserted."""
    if not payload.get("is_blocked"):
        return

    try:
        # Import here to avoid a circular import at module load time.
        from app.telegram_bot import get_telegram_bot

        bot = get_telegram_bot()
        await bot.notify_blocked_news(payload)
    except Exception as exc:  # noqa: BLE001 — уведомление не ломает вставку
        logger.error("[Telegram] Не удалось отправить уведомление: %s", exc)


async def mark_telegram_sent(news_id: int) -> bool:
    """Set telegram_sent = True for a given news_alerts row.

    Возвращает False, если строка не нашлась, вместо исключения: вызывающий
    код в telegram_bot уже отправил сообщение, и ронять его из-за неудачного
    флага смысла нет — но и молча считать запись успешной нельзя, иначе
    алерт продублируется на следующем проходе.
    """
    supabase = await get_supabase()
    resp = await (
        supabase.table(TABLE)
        .update({"telegram_sent": True})
        .eq("id", news_id)
        .execute()
    )
    if not resp.data:
        logger.error(
            "[TG] Флаг telegram_sent не обновлён: news_alerts.id=%s не найден", news_id
        )
        return False
    logger.info("[TG] Флаг telegram_sent обновлён для news_alerts.id=%s", news_id)
    return True


async def get_active_blocks(*, now: Optional[datetime] = None) -> Rows:
    """Алерты, которые прямо сейчас блокируют торговлю.

    Активен алерт с ``is_blocked = true`` и ещё не истёкшим ``block_until``.
    Просроченные записи считаем снятыми (AUTO_EXPIRED) и не учитываем.
    """
    moment = (now or datetime.now(timezone.utc)).isoformat()
    supabase = await get_supabase()
    resp = await (
        supabase.table(TABLE)
        .select("id,article_title,block_until,block_source,is_blocked,extended_until")
        .eq("is_blocked", True)
        .gt("block_until", moment)
        .order("block_until", desc=True)
        .limit(50)
        .execute()
    )
    return resp.data
