"""Walk-forward валидация параметров стратегии.

Зачем: текущие значения в ``strategy_params`` (окно 2500, вход z=2.5,
стоп z=4.5, max hold 15 дней) подобраны на одном полугодовом отрезке
2025-12 → 2026-06. Подобранные так цифры почти всегда выглядят прекрасно
на том же отрезке и разваливаются на следующем — это классическая
переподгонка.

Walk-forward отвечает на вопрос честно: параметры подбираются на окне
обучения, а метрики снимаются на **следующем** окне, которого оптимизатор
не видел. Если out-of-sample результат систематически хуже in-sample —
цифрам верить нельзя, сколько бы ни обещал единичный бэктест.

CLI::

    python -m app.research.walkforward --from 2024-01-01 --to 2026-06-01
    python -m app.research.walkforward --pair SBER SBERP --train-days 120 --test-days 30
"""

from __future__ import annotations

import argparse
import asyncio
import itertools
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd

from app.logging_setup import get_logger
from app.research import metrics
from app.research.backtest import BacktestConfig, prepare_pair, simulate
from app.research.cointegration import assess_pair, format_assessment

logger = get_logger("tradebot.research.walkforward")

# Сетка по умолчанию. Намеренно включает текущие боевые значения,
# чтобы было видно, выигрывают ли они у альтернатив на честной проверке.
DEFAULT_GRID: Dict[str, List[Any]] = {
    "spread_window": [500, 1000, 2500],
    "entry_zscore": [2.0, 2.5, 3.0],
    "stop_zscore": [4.0, 4.5],
    "max_hold_days": [5, 15],
}


@dataclass
class WalkForwardConfig:
    """Настройка скользящих окон обучения и проверки."""

    train_days: int = 120
    test_days: int = 30
    step_days: int = 30
    grid: Dict[str, List[Any]] = field(default_factory=lambda: dict(DEFAULT_GRID))
    base: BacktestConfig = field(default_factory=BacktestConfig)
    # Минимум сделок на обучении, иначе набор параметров не рассматриваем:
    # profit factor по двум сделкам — это не статистика.
    min_trades_train: int = 5
    # Метрика оптимизации на обучающем окне.
    objective: str = "total_pnl"


def grid_configs(
    grid: Dict[str, List[Any]], base: BacktestConfig
) -> List[BacktestConfig]:
    """Развернуть сетку в список конфигураций."""
    keys = list(grid)
    combos = itertools.product(*(grid[key] for key in keys))
    configs: List[BacktestConfig] = []
    for combo in combos:
        params = dict(zip(keys, combo))
        # Стоп обязан быть шире входа, иначе позиция закрывается сразу.
        if params.get("stop_zscore", base.stop_zscore) <= params.get(
            "entry_zscore", base.entry_zscore
        ):
            continue
        configs.append(BacktestConfig(**{**base.to_dict(), **params}))
    return configs


def _score(summary: Dict[str, Any], objective: str) -> float:
    """Значение целевой метрики для сравнения наборов параметров."""
    value = summary.get(objective, 0.0)
    if value == float("inf"):
        # Profit factor без единого убытка — артефакт малой выборки.
        return 0.0
    return float(value or 0.0)


def _slice(frame: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    return frame[(frame["time"] >= start) & (frame["time"] < end)]


def _windows(
    first: datetime, last: datetime, config: WalkForwardConfig
) -> List[Tuple[datetime, datetime, datetime]]:
    """Границы окон: (начало обучения, начало проверки, конец проверки)."""
    windows = []
    train_start = first
    while True:
        test_start = train_start + timedelta(days=config.train_days)
        test_end = test_start + timedelta(days=config.test_days)
        if test_start >= last:
            break
        windows.append((train_start, test_start, min(test_end, last)))
        train_start += timedelta(days=config.step_days)
    return windows


def run_walkforward(
    bars_a: Sequence[Dict[str, Any]],
    bars_b: Sequence[Dict[str, Any]],
    config: Optional[WalkForwardConfig] = None,
) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
    """Прогнать walk-forward.

    Returns:
        (таблица по окнам, все out-of-sample сделки).

    Важная деталь: спред и z-score считаются один раз на всей истории для
    каждого размера окна, а затем нарезаются по датам. Пересчитывать
    статистику внутри каждого окна нельзя — в начале обучающего отрезка
    скользящее окно было бы пустым, и первые сотни баров выпадали бы.
    """
    config = config or WalkForwardConfig()

    windows_by_size: Dict[int, pd.DataFrame] = {}
    for window_size in sorted(set(config.grid.get("spread_window", [config.base.spread_window]))):
        frame = prepare_pair(bars_a, bars_b, window_size)
        if frame.empty:
            continue
        windows_by_size[window_size] = frame

    if not windows_by_size:
        logger.error("[WF] Нет данных для расчёта спреда")
        return pd.DataFrame(), []

    reference = next(iter(windows_by_size.values()))
    first, last = reference["time"].min(), reference["time"].max()
    bounds = _windows(first, last, config)
    if not bounds:
        logger.error(
            "[WF] Период %s → %s короче одного окна train=%s + test=%s дней",
            first.date(), last.date(), config.train_days, config.test_days,
        )
        return pd.DataFrame(), []

    logger.info(
        "[WF] Окон: %s | сетка: %s комбинаций | train=%s дн, test=%s дн, шаг=%s дн",
        len(bounds), len(grid_configs(config.grid, config.base)),
        config.train_days, config.test_days, config.step_days,
    )

    rows: List[Dict[str, Any]] = []
    oos_trades: List[Dict[str, Any]] = []

    for number, (train_start, test_start, test_end) in enumerate(bounds, 1):
        best_config: Optional[BacktestConfig] = None
        best_summary: Optional[Dict[str, Any]] = None
        best_score = float("-inf")

        for candidate in grid_configs(config.grid, config.base):
            frame = windows_by_size.get(candidate.spread_window)
            if frame is None:
                continue
            train_trades = simulate(_slice(frame, train_start, test_start), candidate)
            if len(train_trades) < config.min_trades_train:
                continue
            summary = metrics.summarize_trades(train_trades)
            score = _score(summary, config.objective)
            if score > best_score:
                best_score, best_config, best_summary = score, candidate, summary

        if best_config is None:
            logger.warning(
                "[WF] Окно %s (%s → %s): ни один набор не дал %s+ сделок на обучении",
                number, train_start.date(), test_start.date(), config.min_trades_train,
            )
            rows.append(
                {
                    "окно": number,
                    "обучение": f"{train_start.date()} → {test_start.date()}",
                    "проверка": f"{test_start.date()} → {test_end.date()}",
                    "статус": "нет сделок на обучении",
                }
            )
            continue

        # Лучший набор применяется к следующему окну БЕЗ переподбора.
        test_frame = _slice(windows_by_size[best_config.spread_window], test_start, test_end)
        test_trades = simulate(test_frame, best_config)
        test_summary = metrics.summarize_trades(test_trades)

        for trade in test_trades:
            oos_trades.append({**trade, "окно": number})

        rows.append(
            {
                "окно": number,
                "обучение": f"{train_start.date()} → {test_start.date()}",
                "проверка": f"{test_start.date()} → {test_end.date()}",
                "статус": "ок",
                "окно_спреда": best_config.spread_window,
                "вход_z": best_config.entry_zscore,
                "стоп_z": best_config.stop_zscore,
                "макс_дней": best_config.max_hold_days,
                "IS_сделок": best_summary["total"],
                "IS_winrate": best_summary["winrate"],
                "IS_pnl": best_summary["total_pnl"],
                "OOS_сделок": test_summary["total"],
                "OOS_winrate": test_summary["winrate"],
                "OOS_pnl": test_summary["total_pnl"],
                "OOS_просадка": test_summary["max_drawdown"],
            }
        )
        logger.info(
            "[WF] Окно %s: подобрано window=%s entry=%s → IS %s сделок / %+.0f ₽, "
            "OOS %s сделок / %+.0f ₽",
            number, best_config.spread_window, best_config.entry_zscore,
            best_summary["total"], best_summary["total_pnl"],
            test_summary["total"], test_summary["total_pnl"],
        )

    return pd.DataFrame(rows), oos_trades


def format_report(table: pd.DataFrame, oos_trades: List[Dict[str, Any]]) -> str:
    """Итоговый отчёт: таблица окон, агрегат OOS и вывод про переподгонку."""
    if table.empty:
        return "Walk-forward: результатов нет"

    lines = ["Walk-forward по окнам", "=" * 78, table.to_string(index=False), ""]

    ok = table[table["статус"] == "ок"] if "статус" in table.columns else table
    if ok.empty:
        return "\n".join(lines + ["Ни одно окно не дало результата."])

    summary = metrics.summarize_trades(oos_trades)
    lines.append(metrics.format_summary(summary, "Совокупный OUT-OF-SAMPLE результат"))
    lines.append("")

    if oos_trades:
        lines.append("По причинам выхода (OOS):")
        lines.append(metrics.breakdown_by(oos_trades, "exit_reason").to_string())
        lines.append("")
        lines.append("По направлению (OOS):")
        lines.append(metrics.breakdown_by(oos_trades, "direction").to_string())
        lines.append("")

    is_pnl = float(ok["IS_pnl"].sum())
    oos_pnl = float(ok["OOS_pnl"].sum())
    profitable_windows = int((ok["OOS_pnl"] > 0).sum())
    total_windows = int(len(ok))

    lines.append("Вывод по переподгонке")
    lines.append("─" * 46)
    lines.append(f"Суммарный P&L in-sample:      {is_pnl:+.0f} руб.")
    lines.append(f"Суммарный P&L out-of-sample:  {oos_pnl:+.0f} руб.")
    lines.append(f"Прибыльных OOS-окон:          {profitable_windows} из {total_windows}")

    if oos_pnl <= 0:
        lines.append(
            "\n⚠️  Out-of-sample убыточен. Параметры описывают прошлое, а не "
            "закономерность. Выводить стратегию в LIVE на этих цифрах нельзя."
        )
    elif is_pnl > 0 and oos_pnl < is_pnl * 0.3:
        lines.append(
            "\n⚠️  OOS-результат меньше трети от in-sample — признак переподгонки. "
            "Параметры стоит упростить (шире окно, меньше степеней свободы)."
        )
    elif profitable_windows < total_windows * 0.5:
        lines.append(
            "\n⚠️  Меньше половины OOS-окон прибыльны: результат неустойчив во времени."
        )
    else:
        lines.append(
            "\n✅ OOS-результат положителен и устойчив по окнам. Это не гарантия "
            "будущей прибыли, но параметры хотя бы не выглядят подогнанными."
        )

    if "окно_спреда" in ok.columns:
        chosen = ok["окно_спреда"].value_counts().to_dict()
        lines.append(f"\nКак часто выбиралось окно спреда: {chosen}")
        entries = ok["вход_z"].value_counts().to_dict()
        lines.append(f"Как часто выбирался порог входа:  {entries}")
        lines.append(
            "Если победитель скачет от окна к окну — устойчивого оптимума нет, "
            "и любое конкретное значение в strategy_params произвольно."
        )

    return "\n".join(lines)


async def _load(
    ticker_a: str, ticker_b: str, date_from: str, date_to: str, interval: int
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    from app.market.candle_cache import load_pair

    return await load_pair(
        ticker_a, ticker_b, date_from, date_to, interval=interval
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Walk-forward валидация параметров парной стратегии"
    )
    parser.add_argument("--pair", nargs=2, default=["TATN", "TATNP"], metavar=("A", "B"))
    parser.add_argument("--from", dest="date_from", default="2024-01-01")
    parser.add_argument("--to", dest="date_to", default="2026-06-01")
    parser.add_argument("--interval", type=int, default=1, help="1=минуты, 60=часы, 24=дни")
    parser.add_argument("--train-days", type=int, default=120)
    parser.add_argument("--test-days", type=int, default=30)
    parser.add_argument("--step-days", type=int, default=30)
    parser.add_argument("--slippage-bps", type=float, default=0.0)
    parser.add_argument("--objective", default="total_pnl", choices=["total_pnl", "profit_factor", "winrate"])
    parser.add_argument("--out", default="walkforward_report.csv")
    parser.add_argument("--trades-out", default="walkforward_oos_trades.csv")
    parser.add_argument("--skip-cointegration", action="store_true")
    args = parser.parse_args(argv)

    ticker_a, ticker_b = args.pair
    bars_a, bars_b = asyncio.run(
        _load(ticker_a, ticker_b, args.date_from, args.date_to, args.interval)
    )
    if not bars_a or not bars_b:
        print(f"Нет данных по {ticker_a}/{ticker_b} за {args.date_from} → {args.date_to}")
        return 1

    print(f"\nЗагружено: {ticker_a} — {len(bars_a)} баров, {ticker_b} — {len(bars_b)} баров\n")

    if not args.skip_cointegration:
        frame = prepare_pair(bars_a, bars_b, window=2)
        if not frame.empty:
            print(
                format_assessment(
                    assess_pair(frame["price_a"].tolist(), frame["price_b"].tolist()),
                    f"Оценка пары {ticker_a}/{ticker_b} ({args.date_from} → {args.date_to})",
                )
            )
            print()

    config = WalkForwardConfig(
        train_days=args.train_days,
        test_days=args.test_days,
        step_days=args.step_days,
        base=BacktestConfig(slippage_bps=args.slippage_bps),
        objective=args.objective,
    )
    table, oos_trades = run_walkforward(bars_a, bars_b, config)
    print()
    print(format_report(table, oos_trades))

    if not table.empty:
        table.to_csv(args.out, index=False)
        print(f"\nТаблица окон сохранена: {args.out}")
    if oos_trades:
        pd.DataFrame(oos_trades).to_csv(args.trades_out, index=False)
        print(f"OOS-сделки сохранены:    {args.trades_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
