"""Загрузчик исторических свечей с MOEX ISS API.

Перенос ``fetch_candles`` из ``backtest_pairs_v2.py``. Сохранён рабочий
паттерн пагинации ISS: ``start += len(rows)``, выход при ``len(rows) < 500``.

Отличия от оригинала:

* асинхронно на общем ``httpx.AsyncClient`` вместо блокирующего ``requests``;
* ретраи через ``app.retry.fetch_with_retry`` (экспоненциальный backoff,
  уже умеет 5xx/429/таймауты) вместо ``time.sleep(3)`` в цикле, который
  блокировал бы event loop;
* generic по борду и интервалу — не привязан к TQBR и минуткам.

Источник используется для оффлайн-исследования (бэктест, walk-forward,
проверка коинтеграции). Боевой контур берёт данные у БКС: ISS отдаёт
данные с задержкой и это не тот фид, по которому мы реально исполняемся.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import httpx

from app.logging_setup import get_logger
from app.retry import fetch_with_retry

logger = get_logger("tradebot.market.moex")

ISS_CANDLES_URL = (
    "https://iss.moex.com/iss/engines/stock/markets/shares/"
    "boards/{board}/securities/{ticker}/candles.json"
)

# ISS отдаёт максимум 500 свечей за запрос — это и есть признак последней страницы.
PAGE_SIZE = 500
# Пауза между страницами, чтобы не получить 429 от ISS.
PAGE_DELAY_SEC = 0.2
# Предохранитель от бесконечного цикла, если ISS вдруг начнёт возвращать одно и то же.
MAX_PAGES = 400
# Размер куска при разбиении длинного периода (только для минуток).
CHUNK_DAYS = 90

# Интервалы ISS: 1=минута, 10=10 минут, 60=час, 24=день, 7=неделя, 31=месяц.
INTERVAL_MINUTE = 1
INTERVAL_HOUR = 60
INTERVAL_DAY = 24


def _chunk_ranges(date_from: str, date_to: str, interval: int) -> List[tuple]:
    """Разбить период на куски, каждый из которых заведомо влезает в MAX_PAGES.

    Минутных баров примерно 17 000 в месяц (≈34 страницы), поэтому кусок
    в 90 дней даёт около 100 страниц — с большим запасом. Без разбиения
    длинный запрос упирался в потолок страниц и **молча** возвращал
    обрезанный период, из-за чего в середине истории появлялась дыра.
    """
    if interval != INTERVAL_MINUTE:
        return [(date_from, date_to)]

    start = _parse_bound(date_from)
    end = _parse_bound(date_to)
    if start >= end:
        return [(date_from, date_to)]

    chunks: List[tuple] = []
    cursor = start
    while cursor < end:
        stop = min(cursor + timedelta(days=CHUNK_DAYS), end)
        chunks.append((cursor.strftime("%Y-%m-%d"), stop.strftime("%Y-%m-%d")))
        cursor = stop
    return chunks


def _parse_bound(value: str) -> datetime:
    """Разобрать границу периода (``YYYY-MM-DD`` или ISO 8601)."""
    text = str(value).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError(f"Не удалось разобрать дату: {value!r}")


async def fetch_candles(
    ticker: str,
    date_from: str,
    date_to: str,
    *,
    interval: int = INTERVAL_MINUTE,
    board: str = "TQBR",
    client: Optional[httpx.AsyncClient] = None,
) -> List[Dict[str, Any]]:
    """Скачать свечи инструмента за период.

    Длинные периоды автоматически режутся на куски: ISS отдаёт данные
    страницами по 500, и один запрос на два года минуток упёрся бы
    в защитный потолок страниц, вернув неполный ряд.

    Args:
        ticker: тикер, например ``TATN``.
        date_from / date_to: границы периода, ``YYYY-MM-DD`` или ISO 8601.
        interval: код интервала ISS (1 — минута).
        board: режим торгов (``TQBR`` для акций).
        client: переиспользуемый HTTP-клиент; при отсутствии создаётся временный.

    Returns:
        Список словарей с ключами ``begin, open, close, high, low, volume``,
        отсортированный по времени. Пустой список, если данных нет.
    """
    if client is None:
        async with httpx.AsyncClient(timeout=30.0) as temp_client:
            return await fetch_candles(
                ticker, date_from, date_to,
                interval=interval, board=board, client=temp_client,
            )

    chunks = _chunk_ranges(date_from, date_to, interval)
    if len(chunks) > 1:
        collected: List[Dict[str, Any]] = []
        for chunk_from, chunk_to in chunks:
            collected.extend(
                await _fetch_range(
                    ticker, chunk_from, chunk_to,
                    interval=interval, board=board, client=client,
                )
            )
        normalized = _normalize(collected)
        logger.info(
            "[MOEX] %s (%s, interval=%s): всего %s свечей за %s → %s (%s кусков)",
            ticker, board, interval, len(normalized), date_from, date_to, len(chunks),
        )
        return normalized

    return await _fetch_range(
        ticker, date_from, date_to, interval=interval, board=board, client=client
    )


async def _fetch_range(
    ticker: str,
    date_from: str,
    date_to: str,
    *,
    interval: int,
    board: str,
    client: httpx.AsyncClient,
) -> List[Dict[str, Any]]:
    """Скачать один непрерывный период, листая страницы ISS."""
    url = ISS_CANDLES_URL.format(board=board, ticker=ticker)
    rows: List[Dict[str, Any]] = []
    start = 0

    for page in range(MAX_PAGES):
        params = {
            "from": date_from,
            "till": date_to,
            "interval": interval,
            "start": start,
        }
        try:
            response = await fetch_with_retry(client, "GET", url, params=params)
            payload = response.json()
        except Exception as exc:  # noqa: BLE001 — исследование не должно падать целиком
            logger.error(
                "[MOEX] %s: страница со start=%s не загрузилась: %s", ticker, start, exc
            )
            break

        candles = payload.get("candles") or {}
        data = candles.get("data") or []
        columns = candles.get("columns") or []
        if not data:
            break

        rows.extend(dict(zip(columns, row)) for row in data)
        start += len(data)

        if len(data) < PAGE_SIZE:
            break
        await asyncio.sleep(PAGE_DELAY_SEC)
    else:
        logger.warning(
            "[MOEX] %s: достигнут предел в %s страниц — данные могут быть неполными",
            ticker, MAX_PAGES,
        )

    normalized = _normalize(rows)
    logger.info(
        "[MOEX] %s (%s, interval=%s): загружено %s свечей за %s → %s",
        ticker, board, interval, len(normalized), date_from, date_to,
    )
    return normalized


def _normalize(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Привести строки ISS к единому виду и отсортировать по времени."""
    out: List[Dict[str, Any]] = []
    for row in rows:
        begin = _parse_iss_datetime(row.get("begin"))
        if begin is None:
            continue
        try:
            out.append(
                {
                    "begin": begin,
                    "open": float(row["open"]),
                    "close": float(row["close"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "volume": float(row.get("volume") or 0),
                }
            )
        except (KeyError, TypeError, ValueError):
            continue

    out.sort(key=lambda item: item["begin"])
    # Одна и та же свеча может прийти на стыке страниц.
    deduped: List[Dict[str, Any]] = []
    seen = set()
    for row in out:
        if row["begin"] in seen:
            continue
        seen.add(row["begin"])
        deduped.append(row)
    return deduped


def _parse_iss_datetime(raw: Any) -> Optional[datetime]:
    """ISS отдаёт время как ``2026-06-02 10:00:00`` без таймзоны (МСК)."""
    if isinstance(raw, datetime):
        return raw
    if not raw:
        return None
    text = str(raw).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None
