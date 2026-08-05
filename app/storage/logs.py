"""Storage helpers for the ``logs`` table (structured diagnostic journal)."""

from __future__ import annotations

from typing import Any, Dict, Optional

from app.logging_setup import get_logger
from app.storage.supabase import get_supabase

logger = get_logger("tradebot.storage.logs")

TABLE = "logs"


async def log_event(
    level: str,
    component: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
    trade_uuid: Optional[str] = None,
) -> None:
    """Insert one structured log row.

    Never raises: a Supabase write failure here must not break the caller,
    it only gets reported to the stdlib logger.
    """
    payload: Dict[str, Any] = {
        "level": level,
        "component": component,
        "message": message,
    }
    if details is not None:
        payload["details"] = details
    if trade_uuid is not None:
        payload["trade_uuid"] = trade_uuid

    try:
        supabase = await get_supabase()
        await supabase.table(TABLE).insert(payload).execute()
    except Exception as exc:  # noqa: BLE001 — журнал не ломает бизнес-путь
        logger.error("[DB] Не удалось записать событие в logs: %s", exc)
