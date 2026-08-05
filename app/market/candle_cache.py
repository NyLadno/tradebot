"""CSV-кэш свечей: «проверить кэш → скачать недостающее → сохранить».

Перенос ``load_candles`` из ``backtest_pairs_v2.py`` с одним важным
исправлением: оригинал возвращал закэшированный файл при **любом** запросе,
даже если в файле лежал совсем другой диапазон дат. Здесь проверяется
фактическое покрытие запрошенного периода, и недостающие хвосты догружаются.
"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.logging_setup import get_logger
from app.market.moex_client import INTERVAL_MINUTE, fetch_candles

logger = get_logger("tradebot.market.cache")

DEFAULT_CACHE_DIR = "candles_cache"
FIELDS = ["begin", "open", "close", "high", "low", "volume"]

# Запас на нерабочие дни: если запрошенный период начинается в субботу,
# первая свеча всё равно будет в понедельник — это не повод считать кэш неполным.
COVERAGE_TOLERANCE = timedelta(days=4)

# Ниже этого числа баров месяц считается непокрытым. В торговом месяце
# минуток порядка 17 000; даже с учётом праздников и коротких дней
# сотня баров означает, что месяца в кэше фактически нет.
MIN_BARS_PER_MONTH = 100


def cache_path(ticker: str, interval: int, board: str, cache_dir: str) -> Path:
    """Путь к файлу кэша. Интервал и борд в имени, чтобы не смешивать данные."""
    return Path(cache_dir) / f"{ticker}_{board}_i{interval}.csv"


def save_cache(path: Path, rows: List[Dict[str, Any]]) -> None:
    """Записать свечи в CSV."""
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, "begin": row["begin"].isoformat()})
    logger.info("[CACHE] %s: сохранено %s свечей", path.name, len(rows))


def load_cache(path: Path) -> List[Dict[str, Any]]:
    """Прочитать свечи из CSV. Битый файл считаем отсутствующим."""
    if not path.exists():
        return []
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            rows = []
            for raw in csv.DictReader(handle):
                rows.append(
                    {
                        "begin": datetime.fromisoformat(raw["begin"]),
                        "open": float(raw["open"]),
                        "close": float(raw["close"]),
                        "high": float(raw["high"]),
                        "low": float(raw["low"]),
                        "volume": float(raw.get("volume") or 0),
                    }
                )
        rows.sort(key=lambda item: item["begin"])
        return rows
    except Exception as exc:  # noqa: BLE001
        logger.warning("[CACHE] %s повреждён (%s) — перекачиваю", path.name, exc)
        return []


def _parse_bound(value: str) -> datetime:
    """Разобрать границу периода (``YYYY-MM-DD`` или ISO 8601)."""
    text = str(value).strip().replace("T", " ")
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    raise ValueError(f"Не удалось разобрать дату: {value!r}")


def missing_ranges(
    rows: List[Dict[str, Any]], date_from: str, date_to: str
) -> List[Tuple[str, str]]:
    """Какие куски запрошенного периода отсутствуют в кэше.

    Проверяются и края, и **середина**: помесячно ищутся пропущенные
    интервалы. Проверки одних только краёв недостаточно — обрыв загрузки
    посреди периода оставляет дыру, которая при следующем запросе выглядит
    как полное покрытие (первый и последний бар на месте) и уже никогда
    не чинится. Месяц — компромисс: дневная сверка породила бы сотни
    лишних запросов к ISS из-за выходных и праздников.
    """
    start = _parse_bound(date_from)
    end = _parse_bound(date_to)
    if not rows:
        return [(date_from, date_to)]

    # Сколько баров есть в каждом календарном месяце.
    per_month: Dict[Tuple[int, int], int] = {}
    for row in rows:
        key = (row["begin"].year, row["begin"].month)
        per_month[key] = per_month.get(key, 0) + 1

    # Собираем подряд идущие пустые месяцы в непрерывные диапазоны.
    gaps: List[Tuple[str, str]] = []
    run_start: Optional[datetime] = None
    cursor = datetime(start.year, start.month, 1)
    last_month = datetime(end.year, end.month, 1)

    while cursor <= last_month:
        empty = per_month.get((cursor.year, cursor.month), 0) < MIN_BARS_PER_MONTH
        if empty and run_start is None:
            run_start = cursor
        elif not empty and run_start is not None:
            gaps.append((run_start.strftime("%Y-%m-%d"), cursor.strftime("%Y-%m-%d")))
            run_start = None
        cursor = _next_month(cursor)

    if run_start is not None:
        gaps.append((run_start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))

    # Края уточняем по фактическим границам ряда, а не по месяцам.
    first, last = rows[0]["begin"], rows[-1]["begin"]
    if not gaps:
        if first - start > COVERAGE_TOLERANCE:
            gaps.append((start.strftime("%Y-%m-%d"), first.strftime("%Y-%m-%d")))
        if end - last > COVERAGE_TOLERANCE:
            gaps.append((last.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
    return gaps


def _next_month(moment: datetime) -> datetime:
    return (
        datetime(moment.year + 1, 1, 1)
        if moment.month == 12
        else datetime(moment.year, moment.month + 1, 1)
    )


def clip(
    rows: List[Dict[str, Any]], date_from: str, date_to: str
) -> List[Dict[str, Any]]:
    """Обрезать ряд по запрошенному периоду."""
    start = _parse_bound(date_from)
    end = _parse_bound(date_to)
    if end.hour == 0 and end.minute == 0:
        end = end + timedelta(days=1)  # включаем последний день целиком
    return [row for row in rows if start <= row["begin"] < end]


async def load_candles(
    ticker: str,
    date_from: str,
    date_to: str,
    *,
    interval: int = INTERVAL_MINUTE,
    board: str = "TQBR",
    cache_dir: str = DEFAULT_CACHE_DIR,
    client: Optional[httpx.AsyncClient] = None,
    refresh: bool = False,
) -> List[Dict[str, Any]]:
    """Свечи за период: из кэша, догружая недостающее с MOEX ISS.

    Args:
        refresh: игнорировать кэш и скачать период заново.
    """
    path = cache_path(ticker, interval, board, cache_dir)
    cached = [] if refresh else load_cache(path)

    gaps = missing_ranges(cached, date_from, date_to)
    if not gaps:
        result = clip(cached, date_from, date_to)
        logger.info(
            "[CACHE] %s: %s свечей взято из кэша (%s → %s)",
            ticker, len(result), date_from, date_to,
        )
        return result

    merged: Dict[datetime, Dict[str, Any]] = {row["begin"]: row for row in cached}
    for gap_from, gap_to in gaps:
        logger.info("[CACHE] %s: догружаю %s → %s", ticker, gap_from, gap_to)
        fetched = await fetch_candles(
            ticker, gap_from, gap_to, interval=interval, board=board, client=client
        )
        for row in fetched:
            merged[row["begin"]] = row

    rows = [merged[key] for key in sorted(merged)]
    if rows:
        save_cache(path, rows)
    return clip(rows, date_from, date_to)


async def load_pair(
    ticker_a: str,
    ticker_b: str,
    date_from: str,
    date_to: str,
    *,
    interval: int = INTERVAL_MINUTE,
    board: str = "TQBR",
    cache_dir: str = DEFAULT_CACHE_DIR,
    client: Optional[httpx.AsyncClient] = None,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Загрузить обе ноги пары за один вызов."""
    bars_a = await load_candles(
        ticker_a, date_from, date_to,
        interval=interval, board=board, cache_dir=cache_dir, client=client,
    )
    bars_b = await load_candles(
        ticker_b, date_from, date_to,
        interval=interval, board=board, cache_dir=cache_dir, client=client,
    )
    return bars_a, bars_b
