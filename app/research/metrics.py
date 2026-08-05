"""Метрики результативности сделок.

Перенос блока «Результаты» из ``backtest_pairs_v2.py``: win rate,
profit factor, средний выигрыш/проигрыш, максимальная просадка через
``cumsum()`` / ``cummax()``.

Набор универсальный — годится и для бэктестных сделок, и для боевых
строк из таблицы ``trades`` (там колонка называется ``net_pnl_rub``).
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Union

import pandas as pd

# Возможные имена колонки с результатом сделки, в порядке приоритета.
_PNL_COLUMNS = ("net_pnl_rub", "pnl_rub", "pnl")
_TIME_COLUMNS = ("exit_time", "entry_time", "begin")


def to_frame(trades: Union[Iterable[Dict[str, Any]], pd.DataFrame]) -> pd.DataFrame:
    """Привести список сделок к DataFrame."""
    if isinstance(trades, pd.DataFrame):
        return trades.copy()
    return pd.DataFrame(list(trades))


def pnl_column(frame: pd.DataFrame) -> Optional[str]:
    """Найти колонку с P&L."""
    for name in _PNL_COLUMNS:
        if name in frame.columns:
            return name
    return None


def _sort_column(frame: pd.DataFrame) -> Optional[str]:
    for name in _TIME_COLUMNS:
        if name in frame.columns:
            return name
    return None


def summarize_trades(
    trades: Union[Iterable[Dict[str, Any]], pd.DataFrame]
) -> Dict[str, Any]:
    """Свод по сделкам.

    Returns:
        total, winners, losers, winrate, profit_factor, avg_win, avg_loss,
        total_pnl, max_drawdown, best, worst.

    Просадка считается по кумулятивной кривой P&L, отсортированной по
    времени сделки: ``(cumsum - cumsum.cummax()).min()``. Значение
    отрицательное или ноль.
    """
    frame = to_frame(trades)
    empty = {
        "total": 0, "winners": 0, "losers": 0, "winrate": 0.0,
        "profit_factor": 0.0, "avg_win": 0.0, "avg_loss": 0.0,
        "total_pnl": 0.0, "max_drawdown": 0.0, "best": 0.0, "worst": 0.0,
    }
    if frame.empty:
        return empty

    column = pnl_column(frame)
    if column is None:
        raise ValueError(
            f"Не найдена колонка с P&L; ожидалась одна из {_PNL_COLUMNS}"
        )

    pnl = pd.to_numeric(frame[column], errors="coerce").dropna()
    if pnl.empty:
        return empty

    wins = pnl[pnl > 0]
    losses = pnl[pnl <= 0]
    total = int(len(pnl))

    loss_sum = abs(losses.sum())
    profit_factor = float(wins.sum() / loss_sum) if loss_sum > 0 else float("inf")

    sort_col = _sort_column(frame)
    ordered = (
        frame.assign(_pnl=pd.to_numeric(frame[column], errors="coerce"))
        .dropna(subset=["_pnl"])
        .sort_values(sort_col)["_pnl"]
        if sort_col
        else pnl
    )
    cumulative = ordered.cumsum()
    max_drawdown = float((cumulative - cumulative.cummax()).min())

    return {
        "total": total,
        "winners": int(len(wins)),
        "losers": int(len(losses)),
        "winrate": round(float(len(wins) / total * 100), 2),
        "profit_factor": round(profit_factor, 3) if profit_factor != float("inf") else float("inf"),
        "avg_win": round(float(wins.mean()), 2) if len(wins) else 0.0,
        "avg_loss": round(float(losses.mean()), 2) if len(losses) else 0.0,
        "total_pnl": round(float(pnl.sum()), 2),
        "max_drawdown": round(max_drawdown, 2),
        "best": round(float(pnl.max()), 2),
        "worst": round(float(pnl.min()), 2),
    }


def breakdown_by(
    trades: Union[Iterable[Dict[str, Any]], pd.DataFrame], column: str
) -> pd.DataFrame:
    """Разбивка P&L по произвольной колонке.

    Заменяет три почти одинаковых ``groupby`` из скрипта (по ``reason``,
    ``pair`` и ``direction``) одной функцией.
    """
    frame = to_frame(trades)
    if frame.empty or column not in frame.columns:
        return pd.DataFrame()

    pnl_col = pnl_column(frame)
    if pnl_col is None:
        return pd.DataFrame()

    frame = frame.assign(_pnl=pd.to_numeric(frame[pnl_col], errors="coerce"))
    result = (
        frame.groupby(column)["_pnl"]
        .agg(["count", "sum", "mean"])
        .round(2)
        .sort_values("sum", ascending=False)
    )
    result.columns = ["сделок", "сумма", "среднее"]
    return result


def equity_curve(
    trades: Union[Iterable[Dict[str, Any]], pd.DataFrame],
    starting_equity: float = 0.0,
) -> pd.Series:
    """Кумулятивная кривая капитала по сделкам."""
    frame = to_frame(trades)
    if frame.empty:
        return pd.Series(dtype=float)

    column = pnl_column(frame)
    if column is None:
        return pd.Series(dtype=float)

    sort_col = _sort_column(frame)
    if sort_col:
        frame = frame.sort_values(sort_col)
    values = pd.to_numeric(frame[column], errors="coerce").fillna(0.0)
    curve = values.cumsum() + starting_equity
    if sort_col:
        curve.index = pd.to_datetime(frame[sort_col], errors="coerce")
    return curve


def format_summary(summary: Dict[str, Any], title: str = "Результаты") -> str:
    """Человекочитаемый отчёт — тот же формат, что печатал скрипт."""
    if not summary or not summary.get("total"):
        return f"{title}: сделок нет"

    pf = summary["profit_factor"]
    pf_text = "∞" if pf == float("inf") else f"{pf:.2f}"
    lines = [
        f"{title}",
        "─" * 46,
        f"Сделок всего:     {summary['total']}",
        f"Прибыльных:       {summary['winners']} ({summary['winrate']:.1f}%)",
        f"Убыточных:        {summary['losers']}",
        f"Profit factor:    {pf_text}",
        f"Средний выигрыш:  {summary['avg_win']:+.0f} руб.",
        f"Средний проигрыш: {summary['avg_loss']:+.0f} руб.",
        f"Итого P&L:        {summary['total_pnl']:+.0f} руб.",
        f"Макс. просадка:   {summary['max_drawdown']:+.0f} руб.",
    ]
    return "\n".join(lines)
