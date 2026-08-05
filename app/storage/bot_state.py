"""Storage helpers for the ``bot_state`` singleton table (id=1)."""

from __future__ import annotations

from typing import Any, Dict

from app.storage.supabase import Row, get_supabase

TABLE = "bot_state"


async def get_bot_state() -> Row:
    """Return the singleton bot_state row (id=1, already seeded).

    No insert function is provided: id has PRIMARY KEY + CHECK(id=1), so a
    second insert would fail with a conflict.
    """
    supabase = await get_supabase()
    resp = await supabase.table(TABLE).select("*").eq("id", 1).limit(1).execute()
    return resp.data[0] if resp.data else {}


async def update_bot_state(patch: Dict[str, Any]) -> Row:
    """PATCH the singleton bot_state row.

    Бросает, если строка не изменилась: PostgREST на UPDATE без совпадений
    отвечает успехом с пустым телом, и раньше это выглядело как удачная
    запись. Для торгового состояния тихий no-op опаснее исключения —
    движок обязан узнать, что его решение не доехало до БД.
    """
    supabase = await get_supabase()
    resp = await supabase.table(TABLE).update(patch).eq("id", 1).execute()
    if not resp.data:
        raise RuntimeError(
            f"{TABLE}: обновление не затронуло ни одной строки (нет строки id=1?)"
        )
    return resp.data[0]
