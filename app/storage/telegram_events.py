"""Storage helpers for the ``telegram_events`` table (notification audit trail)."""

from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from app.logging_setup import get_logger
from app.storage.supabase import insert_rows

logger = get_logger("tradebot.storage.telegram_events")

TABLE = "telegram_events"


async def record_telegram_event(
    event_type: str,
    chat_id: int,
    client: httpx.AsyncClient,
    message_text: Optional[str] = None,
    trade_uuid: Optional[str] = None,
    is_success: Optional[bool] = None,
    error_text: Optional[str] = None,
    response_json: Optional[Dict[str, Any]] = None,
    callback_data: Optional[str] = None,
    callback_message_id: Optional[int] = None,
    related_news_alert_id: Optional[int] = None,
) -> None:
    """Insert one telegram_events row.

    Never raises: a Supabase write failure here must not break the caller,
    it only gets reported to the stdlib logger.
    """
    payload: Dict[str, Any] = {"event_type": event_type, "chat_id": chat_id}
    optional_fields = {
        "message_text": message_text,
        "trade_uuid": trade_uuid,
        "is_success": is_success,
        "error_text": error_text,
        "response_json": response_json,
        "callback_data": callback_data,
        "callback_message_id": callback_message_id,
        "related_news_alert_id": related_news_alert_id,
    }
    payload.update({k: v for k, v in optional_fields.items() if v is not None})

    try:
        await insert_rows(TABLE, payload, client)
    except Exception as exc:
        logger.error("[DB] Не удалось записать событие в telegram_events: %s", exc)
