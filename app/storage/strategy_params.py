"""Storage helpers for the ``strategy_params`` singleton table (id=1)."""

from __future__ import annotations

from typing import Any, Dict

from app.storage.supabase import Row, get_supabase

TABLE = "strategy_params"


async def get_strategy_params() -> Row:
    """Return the singleton strategy_params row (id=1, already seeded).

    No insert function is provided: id has PRIMARY KEY + CHECK(id=1), so a
    second insert would fail with a conflict.
    """
    supabase = await get_supabase()
    resp = await supabase.table(TABLE).select("*").eq("id", 1).limit(1).execute()
    return resp.data[0] if resp.data else {}


async def update_strategy_params(patch: Dict[str, Any]) -> Row:
    """PATCH the singleton strategy_params row.

    Callers should include ``updated_by`` and ``update_comment`` in ``patch``
    for the deploy audit trail described in the DB architecture doc.

    Бросает, если строка не изменилась — см. ``bot_state.update_bot_state``.
    """
    supabase = await get_supabase()
    resp = await supabase.table(TABLE).update(patch).eq("id", 1).execute()
    if not resp.data:
        raise RuntimeError(
            f"{TABLE}: обновление не затронуло ни одной строки (нет строки id=1?)"
        )
    return resp.data[0]
