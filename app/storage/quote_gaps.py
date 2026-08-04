"""Storage helpers for the ``quote_gaps`` table (BKS quote-feed outages)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx

from app.storage.supabase import insert_rows, select_rows, update_rows

TABLE = "quote_gaps"


async def open_quote_gap(
    started_at: str,
    client: httpx.AsyncClient,
    affected_symbols: Optional[str] = None,
) -> Dict[str, Any]:
    """Insert an unresolved gap row."""
    payload: Dict[str, Any] = {"started_at": started_at}
    if affected_symbols is not None:
        payload["affected_symbols"] = affected_symbols

    rows = await insert_rows(TABLE, payload, client)
    return rows[0] if rows else {}


async def close_quote_gap(
    gap_id: int, ended_at: str, duration_min: int, client: httpx.AsyncClient
) -> None:
    """Mark a gap as resolved with its computed duration."""
    patch = {"ended_at": ended_at, "duration_min": duration_min, "resolved": True}
    await update_rows(TABLE, {"id": f"eq.{gap_id}"}, patch, client)


async def get_unresolved_gaps(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Return all gaps not yet marked resolved."""
    return await select_rows(TABLE, client, filters={"resolved": "eq.false"})
