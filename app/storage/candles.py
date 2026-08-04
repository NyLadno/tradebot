"""Storage helpers for the ``candles`` table (minute OHLCV bars)."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import httpx

from app.storage.supabase import select_rows, upsert_rows

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
    client: httpx.AsyncClient,
    volume: Optional[int] = None,
    source: str = "BKS",
) -> Dict[str, Any]:
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

    rows = await upsert_rows(TABLE, payload, client, on_conflict=ON_CONFLICT)
    return rows[0] if rows else {}


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


async def insert_candles_batch(
    candles: List[Dict[str, Any]], client: httpx.AsyncClient
) -> List[Dict[str, Any]]:
    """Batch-upsert bars already matching the DB column names.

    Uses ON CONFLICT (symbol, timestamp) so WebSocket reconnects and REST
    gap-backfills can re-send overlapping bars without creating duplicates —
    duplicate bars would corrupt the rolling z-score window.
    """
    if not candles:
        return []
    return await upsert_rows(
        TABLE, dedupe_candles(candles), client, on_conflict=ON_CONFLICT
    )


async def get_latest_candles(
    symbol: str, limit: int, client: httpx.AsyncClient
) -> List[Dict[str, Any]]:
    """Return the most recent ``limit`` bars for ``symbol``, newest first."""
    return await select_rows(
        TABLE,
        client,
        filters={"symbol": f"eq.{symbol}"},
        order="timestamp.desc",
        limit=limit,
    )
