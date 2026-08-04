"""Storage helpers for the ``heartbeat`` table (periodic health snapshots)."""

from __future__ import annotations

from typing import Any, Dict, Optional

import httpx

from app.storage.supabase import insert_rows

TABLE = "heartbeat"


async def record_heartbeat(
    client: httpx.AsyncClient,
    status: Optional[str] = None,
    last_quote_age_sec: Optional[int] = None,
    open_trade_count: Optional[int] = None,
    virtual_equity: Optional[float] = None,
    today_pnl_rub: Optional[float] = None,
    memory_usage_mb: Optional[float] = None,
    cpu_usage_pct: Optional[float] = None,
    db_size_mb: Optional[float] = None,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Insert one heartbeat row; ``timestamp`` is left to the DB default (now()).

    Only fields the caller actually has data for are sent; there is no
    quote feed or trading state in this repo yet, so most numeric fields
    are typically omitted (stored as NULL) rather than fabricated.
    """
    payload: Dict[str, Any] = {}
    optional_fields = {
        "status": status,
        "last_quote_age_sec": last_quote_age_sec,
        "open_trade_count": open_trade_count,
        "virtual_equity": virtual_equity,
        "today_pnl_rub": today_pnl_rub,
        "memory_usage_mb": memory_usage_mb,
        "cpu_usage_pct": cpu_usage_pct,
        "db_size_mb": db_size_mb,
        "details": details,
    }
    payload.update({k: v for k, v in optional_fields.items() if v is not None})

    rows = await insert_rows(TABLE, payload, client)
    return rows[0] if rows else {}
