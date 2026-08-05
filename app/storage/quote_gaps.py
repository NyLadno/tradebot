"""Storage helpers for the ``quote_gaps`` table (BKS quote-feed outages)."""

from __future__ import annotations

from typing import Any, Dict, Optional

from app.storage.supabase import Row, Rows, get_supabase

TABLE = "quote_gaps"


async def open_quote_gap(
    started_at: str,
    affected_symbols: Optional[str] = None,
) -> Row:
    """Insert an unresolved gap row."""
    payload: Dict[str, Any] = {"started_at": started_at}
    if affected_symbols is not None:
        payload["affected_symbols"] = affected_symbols

    supabase = await get_supabase()
    resp = await supabase.table(TABLE).insert(payload).execute()
    return resp.data[0] if resp.data else {}


async def close_quote_gap(gap_id: int, ended_at: str, duration_min: int) -> Row:
    """Mark a gap as resolved with its computed duration.

    Бросает, если запись не нашлась — иначе разрыв навсегда остался бы
    открытым, а движок считал бы его закрытым.
    """
    patch = {"ended_at": ended_at, "duration_min": duration_min, "resolved": True}
    supabase = await get_supabase()
    resp = await supabase.table(TABLE).update(patch).eq("id", gap_id).execute()
    if not resp.data:
        raise RuntimeError(f"{TABLE}: разрыв #{gap_id} не найден")
    return resp.data[0]


async def get_unresolved_gaps() -> Rows:
    """Return all gaps not yet marked resolved."""
    supabase = await get_supabase()
    resp = await supabase.table(TABLE).select("*").eq("resolved", False).execute()
    return resp.data
