"""Storage helpers for the ``candles`` table (minute OHLCV bars)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.storage.supabase import Row, Rows, get_supabase

TABLE = "candles"

# Уникальный индекс candles_symbol_timestamp_key — вставка баров идемпотентна.
ON_CONFLICT = "symbol,timestamp"


async def insert_candle(
    *,
    symbol: str,
    timestamp: str,
    open_price: float,
    high_price: float,
    low_price: float,
    close_price: float,
    volume: Optional[int] = None,
    source: str = "BKS",
) -> Row:
    """Insert one OHLCV bar; returns the inserted row."""
    payload: Dict[str, Any] = {
        "symbol": symbol,
        "timestamp": timestamp,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "source": source,
    }
    if volume is not None:
        payload["volume"] = volume

    supabase = await get_supabase()
    resp = await supabase.table(TABLE).upsert(payload, on_conflict=ON_CONFLICT).execute()
    return resp.data[0] if resp.data else {}


def dedupe_candles(candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Keep one row per (symbol, timestamp), preferring the last occurrence.

    Postgres refuses an ON CONFLICT statement whose own payload hits the same
    key twice ("cannot affect row a second time"), so the batch must be unique
    before it reaches the database. The last occurrence wins because a bar that
    arrives later is the more complete version of the same minute.
    """
    unique: Dict[tuple, Dict[str, Any]] = {}
    for row in candles:
        unique[(row.get("symbol"), row.get("timestamp"))] = row
    return list(unique.values())


async def insert_candles_batch(candles: List[Dict[str, Any]]) -> Rows:
    """Batch-upsert bars already matching the DB column names.

    Uses ON CONFLICT (symbol, timestamp) so WebSocket reconnects and REST
    gap-backfills can re-send overlapping bars without creating duplicates —
    duplicate bars would corrupt the rolling z-score window.
    """
    if not candles:
        return []
    supabase = await get_supabase()
    resp = await (
        supabase.table(TABLE)
        .upsert(dedupe_candles(candles), on_conflict=ON_CONFLICT)
        .execute()
    )
    return resp.data


async def get_latest_candles(symbol: str, limit: int) -> Rows:
    """Return the most recent ``limit`` bars for ``symbol``, newest first."""
    supabase = await get_supabase()
    resp = await (
        supabase.table(TABLE)
        .select("*")
        .eq("symbol", symbol)
        .order("timestamp", desc=True)
        .limit(limit)
        .execute()
    )
    return resp.data
