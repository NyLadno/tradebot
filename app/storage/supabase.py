"""Жизненный цикл клиента Supabase.

Весь доступ к БД идёт через официальный ``supabase-py``: его async-клиент сам
собирает URL, ставит заголовки ``apikey``/``Authorization`` и разбирает ответы
PostgREST. Здесь остаётся только синглтон клиента — сами запросы живут в
модулях по таблицам (``app/storage/<table>.py``).
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Optional

from postgrest.exceptions import APIError
from supabase import AsyncClient, create_async_client

from app.config import settings
from app.logging_setup import get_logger

logger = get_logger("tradebot.supabase")

# Строки PostgREST — то, что возвращает ``APIResponse.data``.
Row = Dict[str, Any]
Rows = List[Row]

__all__ = ["APIError", "AsyncClient", "Row", "Rows", "close_supabase", "get_supabase"]

_client: Optional[AsyncClient] = None
_lock: Optional[asyncio.Lock] = None


def _ensure_lock() -> asyncio.Lock:
    """Ленивый Lock: вне работающего event loop его создавать нельзя."""
    global _lock
    if _lock is None:
        _lock = asyncio.Lock()
    return _lock


async def get_supabase() -> AsyncClient:
    """Вернуть общий async-клиент Supabase, создав его при первом обращении.

    Конфиг проверяется здесь, а не на импорте модуля: приложение должно
    импортироваться и без ``.env`` (тесты, ``--help``, статический анализ).
    """
    global _client
    if _client is not None:
        return _client

    async with _ensure_lock():
        # Пока ждали лок, клиент мог создать другой вызов.
        if _client is None:
            settings.validate_supabase()
            _client = await create_async_client(
                settings.supabase_url, settings.supabase_key
            )
            logger.info("[DB] Клиент Supabase создан: %s", settings.supabase_url)
    return _client


async def close_supabase() -> None:
    """Закрыть HTTP-сессию клиента и сбросить синглтон."""
    global _client
    if _client is None:
        return
    try:
        await _client.postgrest.aclose()
    except Exception as exc:  # noqa: BLE001 — на выключении глушим всё
        logger.warning("[DB] Ошибка при закрытии клиента Supabase: %s", exc)
    _client = None
