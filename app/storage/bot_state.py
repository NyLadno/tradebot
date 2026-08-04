"""Storage helpers for the ``bot_state`` singleton table (id=1)."""

from __future__ import annotations

from typing import Any, Dict

import httpx

from app.storage.supabase import select_rows, update_rows

TABLE = "bot_state"


async def get_bot_state(client: httpx.AsyncClient) -> Dict[str, Any]:
    """Return the singleton bot_state row (id=1, already seeded).

    No insert function is provided: id has PRIMARY KEY + CHECK(id=1), so a
    second insert would fail with a conflict.
    """
    rows = await select_rows(TABLE, client, filters={"id": "eq.1"}, limit=1)
    return rows[0] if rows else {}


async def update_bot_state(patch: Dict[str, Any], client: httpx.AsyncClient) -> None:
    """PATCH the singleton bot_state row."""
    await update_rows(TABLE, {"id": "eq.1"}, patch, client)
