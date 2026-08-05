"""Storage helpers for the ``trades`` table (paper-trading P&L ledger)."""

from __future__ import annotations

from typing import Any, Dict, Optional

from app.storage.supabase import Row, Rows, get_supabase

TABLE = "trades"


async def open_trade(
    *,
    direction: str,
    entry_time: str,
    leg1_entry_price: float,
    leg2_entry_price: float,
    spread_entry: float,
    zscore_entry: float,
    mode: str = "PAPER",
    leg1_qty: Optional[float] = None,
    leg2_qty: Optional[float] = None,
    position_size_rub: Optional[float] = None,
    spread_mean_entry: Optional[float] = None,
    spread_std_entry: Optional[float] = None,
) -> Row:
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

    supabase = await get_supabase()
    resp = await supabase.table(TABLE).insert(payload).execute()
    return resp.data[0] if resp.data else {}


async def get_open_trades() -> Rows:
    """Return all trades currently in OPEN status."""
    supabase = await get_supabase()
    resp = await supabase.table(TABLE).select("*").eq("status", "OPEN").execute()
    return resp.data


async def get_trades_closed_since(since_iso: str) -> Rows:
    """Return trades closed at or after ``since_iso`` (for daily P&L rollups)."""
    supabase = await get_supabase()
    resp = await (
        supabase.table(TABLE)
        .select("trade_uuid,exit_time,net_pnl_rub,exit_reason,mode")
        .eq("status", "CLOSED")
        .gte("exit_time", since_iso)
        .order("exit_time", desc=True)
        .execute()
    )
    return resp.data


async def get_trade_by_uuid(trade_uuid: str) -> Optional[Row]:
    """Return a single trade by its trade_uuid, or None."""
    supabase = await get_supabase()
    resp = await (
        supabase.table(TABLE)
        .select("*")
        .eq("trade_uuid", trade_uuid)
        .limit(1)
        .execute()
    )
    return resp.data[0] if resp.data else None


async def close_trade(trade_uuid: str, patch: Dict[str, Any]) -> Row:
    """PATCH a trade by trade_uuid.

    Forces status='CLOSED' unless the caller's patch already overrides it.
    The caller supplies exit_time/leg{1,2}_exit_price/spread_exit/
    zscore_exit/net_pnl_rub/exit_reason/etc. as a plain dict, since those
    fields depend on strategy logic not yet implemented in this repo.

    Бросает, если сделка не нашлась: иначе движок счёл бы позицию закрытой,
    а в БД она осталась бы OPEN — и следующий тик снова попытался бы её
    закрыть, уже без реальной позиции на счёте.
    """
    full_patch = {"status": "CLOSED", **patch}
    supabase = await get_supabase()
    resp = await (
        supabase.table(TABLE)
        .update(full_patch)
        .eq("trade_uuid", trade_uuid)
        .execute()
    )
    if not resp.data:
        raise RuntimeError(
            f"{TABLE}: сделка {trade_uuid} не найдена — закрытие не записано"
        )
    return resp.data[0]
