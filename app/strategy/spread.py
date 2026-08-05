"""Расчёт спреда и z-score для парной торговли.

Перенос механики из ``prepare_pair`` бэктеста:
``log(price_a / price_b)`` → скользящие mean/std → z-score.
Логика generic — не завязана на TATN/TATNP.

Чистый Python без pandas: этот модуль импортируется боевым циклом,
и тянуть в него тяжёлые зависимости незачем.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

# Совпадение с pandas: Series.std() по умолчанию использует ddof=1.
# Иначе боевые z-score разойдутся с бэктестом.
_DDOF = 1

# Минимальный std, при котором z-score имеет смысл.
# На вырожденном (почти постоянном) спреде остаточная ошибка float даёт
# std порядка 1e-14, и деление на неё рождает z из чистого шума. Реальный
# log-спред коинтегрированной пары имеет std порядка 1e-3, так что порог
# 1e-9 отсекает только вырожденные случаи.
MIN_STD = 1e-9


def log_spread(price_a: float, price_b: float) -> Optional[float]:
    """ln(price_a / price_b) — логарифмический спред пары."""
    if price_a <= 0 or price_b <= 0:
        return None
    return math.log(price_a / price_b)


def align_closes(
    bars_a: Sequence[Dict[str, object]],
    bars_b: Sequence[Dict[str, object]],
    *,
    time_key: str = "timestamp",
    price_key: str = "close",
) -> Tuple[List[datetime], List[float], List[float]]:
    """Inner-join двух рядов баров по времени.

    Прямой аналог ``df_a.join(df_b, how="inner")`` из бэктеста: в расчёт
    попадают только те минуты, где есть цена по обеим ногам. Это важно —
    рассинхрон рядов даёт ложный z-score.

    Возвращает (времена, цены A, цены B), отсортированные по возрастанию.
    """
    index_b = {row[time_key]: row[price_key] for row in bars_b}
    times: List[datetime] = []
    prices_a: List[float] = []
    prices_b: List[float] = []

    for row in sorted(bars_a, key=lambda item: item[time_key]):  # type: ignore[arg-type,return-value]
        stamp = row[time_key]
        if stamp not in index_b:
            continue
        try:
            price_a = float(row[price_key])  # type: ignore[arg-type]
            price_b = float(index_b[stamp])  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
        if price_a <= 0 or price_b <= 0:
            continue
        times.append(stamp)  # type: ignore[arg-type]
        prices_a.append(price_a)
        prices_b.append(price_b)

    return times, prices_a, prices_b


def spread_series(
    prices_a: Sequence[float], prices_b: Sequence[float]
) -> List[float]:
    """Ряд log-спреда по двум выровненным рядам цен."""
    series: List[float] = []
    for price_a, price_b in zip(prices_a, prices_b):
        value = log_spread(price_a, price_b)
        if value is not None:
            series.append(value)
    return series


def rolling_stats(
    spread: Sequence[float], window: int
) -> Tuple[Optional[float], Optional[float]]:
    """Среднее и стандартное отклонение по последним ``window`` значениям.

    Возвращает (None, None), пока данных меньше окна — ровно как
    ``rolling(window)`` в pandas, который до заполнения окна даёт NaN.
    """
    if window <= 1 or len(spread) < window:
        return None, None

    tail = list(spread[-window:])
    mean = sum(tail) / window
    variance = sum((value - mean) ** 2 for value in tail) / (window - _DDOF)
    return mean, math.sqrt(variance)


def zscore(
    spread_now: float, mean: Optional[float], std: Optional[float]
) -> Optional[float]:
    """(spread - mean) / std; None, если статистика недоступна или std вырожден."""
    if mean is None or std is None or std < MIN_STD:
        return None
    return (spread_now - mean) / std


def current_zscore(
    prices_a: Sequence[float], prices_b: Sequence[float], window: int
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Свернуть весь расчёт в один вызов.

    Возвращает (spread, mean, std, zscore) для последней точки рядов.
    Любой элемент может быть None, если данных не хватает — движок в этом
    случае просто не принимает решений.
    """
    series = spread_series(prices_a, prices_b)
    if not series:
        return None, None, None, None

    spread_now = series[-1]
    mean, std = rolling_stats(series, window)
    return spread_now, mean, std, zscore(spread_now, mean, std)
