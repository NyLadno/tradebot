"""Событийный бэктест парной стратегии.

Переписан относительно ``simulate_pairs`` из ``backtest_pairs_v2.py``.
Что изменено и почему:

1. **Проскальзывание.** Оригинал входил и выходил по цене закрытия, то есть
   по середине спреда. В реальности покупаешь по ask и продаёшь по bid, и на
   паре это четыре пересечения спреда за сделку. Здесь есть ``slippage_bps``.
2. **Границы сессии.** Захардкоженное ``hour >= 17 and minute >= 10``
   не только привязано к конкретному расписанию, но и содержит ошибку:
   условие ложно в 18:05, потому что ``minute >= 10`` не выполняется.
   Здесь сессии определяются по разрывам в самих данных, а вход запрещается
   за ``session_end_buffer_min`` до закрытия.
3. **Время удержания** везде считается через ``total_seconds()``.
   ``.days`` округляет вниз до суток: позиция, открытая в 18:00 и закрытая
   в 11:00 следующего дня, давала ``hold_days = 0`` — то есть ни таймаут,
   ни плата за перенос не срабатывали.
4. **Плата за перенос** начисляется за фактические ночи (переходы через
   полночь), а не за целые сутки удержания.
5. **Параметры** — не константы модуля, а ``BacktestConfig`` с теми же
   полями, что в таблице ``strategy_params``, чтобы бэктест и боевой цикл
   считали одно и то же.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

# Маркер: β переоценивается по скользящему окну вместо фиксированного числа.
ROLLING_BETA = "rolling"

LONG_A = "LONG_A_SHORT_B"
SHORT_A = "SHORT_A_LONG_B"

EXIT_TP = "TP"
EXIT_STOP = "STOP"
EXIT_TIMEOUT = "TIMEOUT"
EXIT_EOD = "EOD"

# Разрыв между барами, после которого считаем, что началась новая сессия.
SESSION_BREAK = timedelta(minutes=30)


@dataclass
class BacktestConfig:
    """Параметры прогона. Имена совпадают с колонками ``strategy_params``."""

    spread_window: int = 2500
    entry_zscore: float = 2.5
    exit_zscore: float = 0.0
    stop_zscore: float = 4.5
    max_hold_days: int = 15
    min_hold_min: int = 120
    commission_pct: float = 0.0003
    position_size: float = 50000.0
    overnight_pct: float = 0.0001
    session_end_buffer_min: int = 90

    # Хедж-коэффициент: сколько единиц стоимости ноги B хеджируют единицу A.
    # 1.0 = поведение исходного скрипта (спред ln(A/B), ноги равного объёма).
    hedge_beta: Any = 1.0

    # Одностороннее проскальзывание в базисных пунктах (1 bp = 0.01%).
    # 0 воспроизводит поведение исходного скрипта.
    slippage_bps: float = 0.0
    # Закрывать ли позицию в конце последней сессии периода.
    close_at_end: bool = True

    # В какие фазы дня разрешён ВХОД (имена из app.research.slippage).
    # None = разрешено везде. Выходы фильтр не ограничивает: держать
    # позицию только потому, что сейчас неудобная фаза, опаснее, чем выйти.
    allowed_entry_phases: Optional[frozenset] = None

    # Разное проскальзывание по фазам дня: {фаза: bps}. Если фаза не указана,
    # берётся slippage_bps. Нужно, чтобы моделировать более широкий спред
    # утренней сессии, а не мазать средним по всему дню.
    slippage_by_phase: Optional[Dict[str, float]] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Position:
    direction: str
    entry_time: datetime
    entry_price_a: float
    entry_price_b: float
    qty_a: float
    qty_b: float
    zscore_entry: float
    spread_entry: float
    peak_pnl: float = 0.0
    max_drawdown: float = 0.0


def prepare_pair(
    bars_a: Sequence[Dict[str, Any]],
    bars_b: Sequence[Dict[str, Any]],
    window: int,
    *,
    time_key: str = "begin",
    price_key: str = "close",
    hedge_beta: Any = 1.0,
) -> pd.DataFrame:
    """Выровнять ноги по времени и посчитать спред, mean/std и z-score.

    Спред считается как ``ln(price_a) - β·ln(price_b)``. При ``β = 1`` это
    в точности ``log(price_a / price_b)`` из исходного скрипта.

    Зачем β: соотношение 1:1 неявно предполагает, что ноги движутся один
    к одному. МНК по TATN/TATNP этого не подтверждает, и при β = 1 остаток
    содержит направленную составляющую — позиция перестаёт быть рыночно
    нейтральной, а тест на стационарность спреда проваливается.
    """
    frame_a = pd.DataFrame(
        [{"time": r[time_key], "price_a": float(r[price_key])} for r in bars_a]
    )
    frame_b = pd.DataFrame(
        [{"time": r[time_key], "price_b": float(r[price_key])} for r in bars_b]
    )
    if frame_a.empty or frame_b.empty:
        return pd.DataFrame()

    frame = frame_a.merge(frame_b, on="time", how="inner").sort_values("time")
    frame = frame[(frame["price_a"] > 0) & (frame["price_b"] > 0)].reset_index(drop=True)
    if frame.empty:
        return frame

    log_a = np.log(frame["price_a"])
    log_b = np.log(frame["price_b"])
    if hedge_beta == ROLLING_BETA:
        # β переоценивается на каждом баре по скользящему окну — без
        # заглядывания вперёд, в отличие от МНК по всей выборке.
        beta_series = (
            log_a.rolling(window).cov(log_b) / log_b.rolling(window).var()
        )
        frame["hedge_beta"] = beta_series
        frame["spread"] = log_a - beta_series * log_b
        # Статистику остатка считаем ТЕКУЩИМ β по всему окну.
        # Наивное rolling(window) по самому ряду spread было бы ошибкой:
        # он собран из остатков с разными β, и отклонение отражало бы
        # дрейф β, а не расхождение цен.
        mean_a = log_a.rolling(window).mean()
        mean_b = log_b.rolling(window).mean()
        var_a = log_a.rolling(window).var()
        var_b = log_b.rolling(window).var()
        cov_ab = log_a.rolling(window).cov(log_b)
        frame["spread_mean"] = mean_a - beta_series * mean_b
        residual_var = var_a - 2.0 * beta_series * cov_ab + beta_series**2 * var_b
        frame["spread_std"] = np.sqrt(residual_var.clip(lower=0.0))
        frame["zscore"] = (frame["spread"] - frame["spread_mean"]) / frame["spread_std"]
        frame["session_end"] = _session_ends(frame["time"])
        return frame

    frame["hedge_beta"] = float(hedge_beta)
    frame["spread"] = log_a - float(hedge_beta) * log_b
    frame["spread_mean"] = frame["spread"].rolling(window).mean()
    frame["spread_std"] = frame["spread"].rolling(window).std()
    frame["zscore"] = (frame["spread"] - frame["spread_mean"]) / frame["spread_std"]
    frame["session_end"] = _session_ends(frame["time"])
    return frame


def _session_ends(times: pd.Series) -> pd.Series:
    """Для каждого бара — время последнего бара его торговой сессии.

    Сессии определяются по разрывам в данных: биржа не торгует ночью,
    поэтому между последним баром дня и первым баром следующего всегда
    большой промежуток. Это устойчивее, чем зашивать расписание в код.
    """
    stamps = pd.to_datetime(times).reset_index(drop=True)
    if stamps.empty:
        return pd.Series(dtype="datetime64[ns]")

    gaps = stamps.diff() > SESSION_BREAK
    session_id = gaps.cumsum()
    ends = stamps.groupby(session_id).transform("max")
    return ends


def _apply_slippage(price: float, *, is_buy: bool, slippage_bps: float) -> float:
    """Покупаем дороже, продаём дешевле — на половину спреда в каждую сторону."""
    if slippage_bps <= 0:
        return price
    factor = 1.0 + (slippage_bps / 10_000.0) * (1 if is_buy else -1)
    return price * factor


def _phase_of(moment: datetime) -> str:
    """Фаза торгового дня для бара (время из ISS наивное, в МСК)."""
    from app.research.slippage import classify_time

    return classify_time(moment)


def _slippage_for(moment: datetime, config: BacktestConfig) -> float:
    """Проскальзывание, применимое в данный момент дня."""
    if not config.slippage_by_phase:
        return config.slippage_bps
    return config.slippage_by_phase.get(_phase_of(moment), config.slippage_bps)


def leg_notionals(
    config: BacktestConfig, bar_beta: Optional[float] = None
) -> Tuple[float, float]:
    """Номиналы ног при заданном β.

    Чтобы P&L отслеживал спред ``ln(A) - β·ln(B)``, нужно
    ``notional_b = β · notional_a``. Общий задействованный капитал
    удерживаем равным ``2 · position_size`` — столько же, сколько при
    равных ногах, иначе прогоны с разным β нельзя сравнивать по рублям.
    """
    raw = bar_beta if bar_beta is not None and pd.notna(bar_beta) else config.hedge_beta
    if raw == ROLLING_BETA:
        raw = 1.0
    beta = max(1e-6, float(raw))
    notional_a = 2.0 * config.position_size / (1.0 + beta)
    return notional_a, beta * notional_a


def _overnight_nights(entry: datetime, exit_: datetime) -> int:
    """Число переходов через полночь между входом и выходом."""
    return max(0, (exit_.date() - entry.date()).days)


def simulate(
    frame: pd.DataFrame, config: Optional[BacktestConfig] = None
) -> List[Dict[str, Any]]:
    """Прогнать стратегию по подготовленному DataFrame.

    Возвращает список сделок в формате, который понимает
    ``app.research.metrics.summarize_trades``.
    """
    config = config or BacktestConfig()
    if frame.empty:
        return []

    trades: List[Dict[str, Any]] = []
    position: Optional[Position] = None
    min_hold = timedelta(minutes=config.min_hold_min)
    max_hold = timedelta(days=config.max_hold_days)
    buffer_ = timedelta(minutes=config.session_end_buffer_min)

    rows = frame.to_dict("records")
    last_index = len(rows) - 1

    for index, row in enumerate(rows):
        zscore = row["zscore"]
        if pd.isna(zscore):
            continue

        now = row["time"]
        price_a = row["price_a"]
        price_b = row["price_b"]

        if position is not None:
            _track_drawdown(position, price_a, price_b)

            # Через total_seconds(), а не .days.
            held = now - position.entry_time
            reason: Optional[str] = None

            if abs(zscore) >= config.stop_zscore:
                reason = EXIT_STOP
            elif held.total_seconds() >= max_hold.total_seconds():
                reason = EXIT_TIMEOUT
            elif position.direction == LONG_A and zscore >= -config.exit_zscore:
                reason = EXIT_TP
            elif position.direction == SHORT_A and zscore <= config.exit_zscore:
                reason = EXIT_TP

            # Кулдаун уважают TP и TIMEOUT, но не STOP.
            if reason in (EXIT_TP, EXIT_TIMEOUT) and held < min_hold:
                reason = None

            if reason is None and config.close_at_end and index == last_index:
                reason = EXIT_EOD

            if reason:
                trades.append(_close(position, row, reason, config))
                position = None
            continue

        # Вход запрещён на закрытии сессии: выходить будет нечем.
        session_end = row.get("session_end")
        if isinstance(session_end, (pd.Timestamp, datetime)):
            if now >= (session_end - buffer_):
                continue
        if index >= last_index:
            continue

        if zscore >= config.entry_zscore:
            direction = SHORT_A
        elif zscore <= -config.entry_zscore:
            direction = LONG_A
        else:
            continue

        if config.allowed_entry_phases is not None:
            if _phase_of(now) not in config.allowed_entry_phases:
                continue

        # Вход по следующему бару — сигнал получен только на закрытии текущего.
        next_row = rows[index + 1]
        if (next_row["time"] - now) > SESSION_BREAK:
            # Следующий бар уже в другой сессии: гэп на открытии исполнить нельзя.
            continue

        position = _open(direction, next_row, zscore, config)

    return trades


def _open(
    direction: str, row: Dict[str, Any], zscore: float, config: BacktestConfig
) -> Position:
    long_a = direction == LONG_A
    bps = _slippage_for(row["time"], config)
    price_a = _apply_slippage(row["price_a"], is_buy=long_a, slippage_bps=bps)
    price_b = _apply_slippage(row["price_b"], is_buy=not long_a, slippage_bps=bps)
    bar_beta = row.get("hedge_beta")
    notional_a, notional_b = leg_notionals(config, bar_beta)
    return Position(
        direction=direction,
        entry_time=row["time"],
        entry_price_a=price_a,
        entry_price_b=price_b,
        qty_a=math.floor(notional_a / price_a),
        qty_b=math.floor(notional_b / price_b),
        zscore_entry=float(zscore),
        spread_entry=float(row["spread"]),
    )


def _track_drawdown(position: Position, price_a: float, price_b: float) -> None:
    sign = 1.0 if position.direction == LONG_A else -1.0
    gross = (
        sign * position.qty_a * (price_a - position.entry_price_a)
        - sign * position.qty_b * (price_b - position.entry_price_b)
    )
    position.peak_pnl = max(position.peak_pnl, gross)
    position.max_drawdown = max(position.max_drawdown, position.peak_pnl - gross)


def _close(
    position: Position, row: Dict[str, Any], reason: str, config: BacktestConfig
) -> Dict[str, Any]:
    long_a = position.direction == LONG_A
    # Закрытие разворачивает стороны: лонг продаём, шорт откупаем.
    bps = _slippage_for(row["time"], config)
    exit_a = _apply_slippage(row["price_a"], is_buy=not long_a, slippage_bps=bps)
    exit_b = _apply_slippage(row["price_b"], is_buy=long_a, slippage_bps=bps)

    sign = 1.0 if long_a else -1.0
    pnl_a = sign * position.qty_a * (exit_a - position.entry_price_a)
    pnl_b = -sign * position.qty_b * (exit_b - position.entry_price_b)
    gross = pnl_a + pnl_b

    commission = config.commission_pct * (
        position.qty_a * position.entry_price_a
        + position.qty_b * position.entry_price_b
        + position.qty_a * exit_a
        + position.qty_b * exit_b
    )

    held = row["time"] - position.entry_time
    held_sec = max(0.0, held.total_seconds())
    nights = _overnight_nights(position.entry_time, row["time"])
    overnight = nights * config.overnight_pct * config.position_size

    return {
        "direction": position.direction,
        "entry_phase": _phase_of(position.entry_time),
        "entry_time": position.entry_time,
        "exit_time": row["time"],
        "entry_price_a": round(position.entry_price_a, 4),
        "entry_price_b": round(position.entry_price_b, 4),
        "exit_price_a": round(exit_a, 4),
        "exit_price_b": round(exit_b, 4),
        "qty_a": position.qty_a,
        "qty_b": position.qty_b,
        "zscore_entry": round(position.zscore_entry, 3),
        "zscore_exit": round(float(row["zscore"]), 3),
        "spread_entry": round(position.spread_entry, 6),
        "spread_exit": round(float(row["spread"]), 6),
        "pnl_leg_a": round(pnl_a, 2),
        "pnl_leg_b": round(pnl_b, 2),
        "gross_pnl_rub": round(gross, 2),
        "commission_rub": round(commission, 2),
        "overnight_fees_rub": round(overnight, 2),
        "net_pnl_rub": round(gross - commission - overnight, 2),
        "exit_reason": reason,
        "hold_time_min": int(held_sec // 60),
        "hold_days": int(held_sec // 86400),
        "overnight_nights": nights,
        "max_drawdown_rub": round(position.max_drawdown, 2),
    }


def run_backtest(
    bars_a: Sequence[Dict[str, Any]],
    bars_b: Sequence[Dict[str, Any]],
    config: Optional[BacktestConfig] = None,
) -> Tuple[List[Dict[str, Any]], pd.DataFrame]:
    """Подготовить данные и прогнать стратегию.

    Returns:
        (сделки, подготовленный DataFrame со спредом и z-score).
    """
    config = config or BacktestConfig()
    frame = prepare_pair(
        bars_a, bars_b, config.spread_window, hedge_beta=config.hedge_beta
    )
    if frame.empty:
        return [], frame
    return simulate(frame, config), frame
