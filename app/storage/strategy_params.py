"""Storage helpers for the ``strategy_params`` singleton table (id=1)."""

from __future__ import annotations

from typing import Any, Dict

import httpx

from app.storage.supabase import select_rows, update_rows

TABLE = "strategy_params"


async def get_strategy_params(client: httpx.AsyncClient) -> Dict[str, Any]:
    """Return the singleton strategy_params row (id=1, already seeded).

    No insert function is provided: id has PRIMARY KEY + CHECK(id=1), so a
    second insert would fail with a conflict.
    """
    rows = await select_rows(TABLE, client, filters={"id": "eq.1"}, limit=1)
    return rows[0] if rows else {}


async def update_strategy_params(
    patch: Dict[str, Any], client: httpx.AsyncClient
) -> None:
    """PATCH the singleton strategy_params row.

    Callers should include ``updated_by`` and ``update_comment`` in ``patch``
    for the deploy audit trail described in the DB architecture doc.
    """
    await update_rows(TABLE, {"id": "eq.1"}, patch, client)
