"""Storage helpers for the ``trades`` table (paper-trading P&L ledger)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx

from app.storage.supabase import insert_rows, select_rows, update_rows

TABLE = "trades"


async def open_trade(
    *,
    direction: str,
    entry_time: str,
    leg1_entry_price: float,
    leg2_entry_price: float,
    spread_entry: float,
    zscore_entry: float,
    client: httpx.AsyncClient,
    mode: str = "PAPER",
    leg1_qty: Optional[float] = None,
    leg2_qty: Optional[float] = None,
    position_size_rub: Optional[float] = None,
    spread_mean_entry: Optional[float] = None,
    spread_std_entry: Optional[float] = None,
) -> Dict[str, Any]:
    """Insert a new OPEN trade row.

    trade_uuid/status/leg1_symbol/leg2_symbol are left to their DB defaults
    (gen_random_uuid(), 'OPEN', 'TATN', 'TATNP').
    """
    payload: Dict[str, Any] = {
        "mode": mode,
        "direction": direction,
        "entry_time": entry_time,
        "leg1_entry_price": leg1_entry_price,
        "leg2_entry_price": leg2_entry_price,
        "spread_entry": spread_entry,
        "zscore_entry": zscore_entry,
    }
    optional_fields = {
        "leg1_qty": leg1_qty,
        "leg2_qty": leg2_qty,
        "position_size_rub": position_size_rub,
        "spread_mean_entry": spread_mean_entry,
        "spread_std_entry": spread_std_entry,
    }
    payload.update({k: v for k, v in optional_fields.items() if v is not None})

    rows = await insert_rows(TABLE, payload, client)
    return rows[0] if rows else {}


async def get_open_trades(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Return all trades currently in OPEN status."""
    return await select_rows(TABLE, client, filters={"status": "eq.OPEN"})


async def get_trades_closed_since(
    since_iso: str, client: httpx.AsyncClient
) -> List[Dict[str, Any]]:
    """Return trades closed at or after ``since_iso`` (for daily P&L rollups)."""
    return await select_rows(
        TABLE,
        client,
        select="trade_uuid,exit_time,net_pnl_rub,exit_reason,mode",
        filters={"status": "eq.CLOSED", "exit_time": f"gte.{since_iso}"},
        order="exit_time.desc",
    )


async def get_trade_by_uuid(
    trade_uuid: str, client: httpx.AsyncClient
) -> Optional[Dict[str, Any]]:
    """Return a single trade by its trade_uuid, or None."""
    rows = await select_rows(
        TABLE, client, filters={"trade_uuid": f"eq.{trade_uuid}"}, limit=1
    )
    return rows[0] if rows else None


async def close_trade(
    trade_uuid: str, patch: Dict[str, Any], client: httpx.AsyncClient
) -> None:
    """PATCH a trade by trade_uuid.

    Forces status='CLOSED' unless the caller's patch already overrides it.
    The caller supplies exit_time/leg{1,2}_exit_price/spread_exit/
    zscore_exit/net_pnl_rub/exit_reason/etc. as a plain dict, since those
    fields depend on strategy logic not yet implemented in this repo.
    """
    full_patch = {"status": "CLOSED", **patch}
    await update_rows(TABLE, {"trade_uuid": f"eq.{trade_uuid}"}, full_patch, client)
