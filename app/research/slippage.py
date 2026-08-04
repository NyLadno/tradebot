"""Измерение фактического проскальзывания по сделкам из таблицы ``trades``.

Сравнивает цену исполнения каждой ноги с ``close`` соответствующего
минутного бара из таблицы ``candles`` — то есть с той ценой, которую
наивный бэктест считает ценой сделки. Разница и есть то, чего бэктест v2
не видел вовсе.

Разбивка идёт **по времени входа**, потому что средний спред и спред
в первые минуты после открытия — это разные величины. Если стратегия
кластеризует входы на открытии (а по данным она это делает: заметная
часть сигналов приходится на первые минуты сессии), то реальные издержки
будут ближе к худшему случаю, а не к среднему по дню.

Запуск::

    python -m app.research.slippage                 # по сделкам PAPER из БД
    python -m app.research.slippage --mode LIVE
    python -m app.research.slippage --opening-minutes 15
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

from app.broker.models import parse_bcs_datetime
from app.logging_setup import get_logger

logger = get_logger("tradebot.research.slippage")

try:
    from zoneinfo import ZoneInfo

    MSK = ZoneInfo("Europe/Moscow")
except Exception:  # pragma: no cover
    MSK = timezone(timedelta(hours=3))

# Расписание TQBR (МСК). Утренняя сессия появилась у МосБиржи отдельно
# от основной, поэтому «открытие» — это открытие ОСНОВНОЙ сессии в 10:00,
# сразу после аукциона открытия 09:50–10:00.
EARLY_START = time(6, 50)
AUCTION_START = time(9, 50)
MAIN_START = time(10, 0)
MAIN_END = time(18, 40)
EVENING_START = time(19, 0)
EVENING_END = time(23, 50)

DEFAULT_OPENING_MINUTES = 15

BUCKET_OPENING = "открытие"
BUCKET_MAIN = "основная сессия"
BUCKET_EARLY = "утренняя сессия"
BUCKET_AUCTION = "аукцион открытия"
BUCKET_EVENING = "вечерняя сессия"
BUCKET_UNKNOWN = "вне расписания"

BUCKET_ORDER = [
    BUCKET_OPENING,
    BUCKET_MAIN,
    BUCKET_EVENING,
    BUCKET_EARLY,
    BUCKET_AUCTION,
    BUCKET_UNKNOWN,
]


@dataclass
class FillObservation:
    """Одно исполнение с посчитанным проскальзыванием."""

    trade_uuid: str
    ticker: str
    side: str  # BUY / SELL
    phase: str  # entry / exit
    fill_time: datetime
    fill_price: float
    bar_close: float
    bucket: str
    quantity: float

    @property
    def slippage_bps(self) -> float:
        """Во сколько базисных пунктов обошлось исполнение.

        Положительное значение = мы заплатили дороже эталона: купили выше
        ``close`` или продали ниже. Отрицательное = исполнились лучше
        эталонной цены (бывает — бар закрылся не там, где мы торговали).
        """
        if self.bar_close <= 0:
            return 0.0
        raw = (self.fill_price - self.bar_close) / self.bar_close
        signed = raw if self.side == "BUY" else -raw
        return signed * 10_000.0

    @property
    def cost_rub(self) -> float:
        """Стоимость проскальзывания в рублях по этой ноге."""
        return abs(self.quantity) * self.bar_close * self.slippage_bps / 10_000.0


def classify_time(
    moment: datetime, *, opening_minutes: int = DEFAULT_OPENING_MINUTES
) -> str:
    """Отнести момент исполнения к фазе торгового дня (по МСК)."""
    # Наивное время трактуем как МСК: именно так его отдаёт MOEX ISS.
    local = (moment if moment.tzinfo is None else moment.astimezone(MSK)).time()
    opening_end = (
        datetime.combine(datetime.today(), MAIN_START) + timedelta(minutes=opening_minutes)
    ).time()

    if MAIN_START <= local < opening_end:
        return BUCKET_OPENING
    if opening_end <= local < MAIN_END:
        return BUCKET_MAIN
    if AUCTION_START <= local < MAIN_START:
        return BUCKET_AUCTION
    if EARLY_START <= local < AUCTION_START:
        return BUCKET_EARLY
    if EVENING_START <= local <= EVENING_END:
        return BUCKET_EVENING
    return BUCKET_UNKNOWN


def _floor_minute(moment: datetime) -> datetime:
    return moment.astimezone(timezone.utc).replace(second=0, microsecond=0)


def build_bar_index(candles: Sequence[Dict[str, Any]]) -> Dict[Tuple[str, datetime], float]:
    """Индекс (тикер, минута) → close для быстрого сопоставления."""
    index: Dict[Tuple[str, datetime], float] = {}
    for row in candles:
        stamp = parse_bcs_datetime(row.get("timestamp"))
        if stamp is None:
            continue
        try:
            index[(str(row.get("symbol")), _floor_minute(stamp))] = float(row["close"])
        except (KeyError, TypeError, ValueError):
            continue
    return index


def _lookup_close(
    index: Dict[Tuple[str, datetime], float], ticker: str, moment: datetime
) -> Optional[float]:
    """Найти close бара минуты исполнения, с допуском ±1 минута."""
    minute = _floor_minute(moment)
    for delta in (0, -1, 1):
        value = index.get((ticker, minute + timedelta(minutes=delta)))
        if value is not None:
            return value
    return None


def _legs_of(trade: Dict[str, Any], phase: str) -> List[Tuple[str, str, Any, Any]]:
    """Ноги сделки как (тикер, сторона, цена, количество) для входа или выхода.

    ``direction`` описывает открытие: ``LONG_*`` — leg1 купили, leg2 продали.
    На выходе стороны разворачиваются.
    """
    direction = str(trade.get("direction") or "")
    long_leg1 = direction.startswith("LONG_")
    entry = phase == "entry"

    leg1_side = "BUY" if (long_leg1 == entry) else "SELL"
    leg2_side = "SELL" if leg1_side == "BUY" else "BUY"
    price_key = "entry_price" if entry else "exit_price"

    return [
        (
            str(trade.get("leg1_symbol") or ""),
            leg1_side,
            trade.get(f"leg1_{price_key}"),
            trade.get("leg1_qty"),
        ),
        (
            str(trade.get("leg2_symbol") or ""),
            leg2_side,
            trade.get(f"leg2_{price_key}"),
            trade.get("leg2_qty"),
        ),
    ]


def measure(
    trades: Sequence[Dict[str, Any]],
    candles: Sequence[Dict[str, Any]],
    *,
    opening_minutes: int = DEFAULT_OPENING_MINUTES,
) -> Tuple[List[FillObservation], Dict[str, int]]:
    """Посчитать проскальзывание по всем исполнениям.

    Returns:
        (наблюдения, счётчики пропусков).
    """
    index = build_bar_index(candles)
    observations: List[FillObservation] = []
    skipped = {"нет бара": 0, "нет цены": 0}

    for trade in trades:
        for phase, time_key in (("entry", "entry_time"), ("exit", "exit_time")):
            moment = parse_bcs_datetime(trade.get(time_key))
            if moment is None:
                continue
            for ticker, side, price, quantity in _legs_of(trade, phase):
                if price is None or not ticker:
                    skipped["нет цены"] += 1
                    continue
                bar_close = _lookup_close(index, ticker, moment)
                if bar_close is None:
                    skipped["нет бара"] += 1
                    continue
                try:
                    fill_price = float(price)
                    qty = float(quantity or 0)
                except (TypeError, ValueError):
                    skipped["нет цены"] += 1
                    continue

                observations.append(
                    FillObservation(
                        trade_uuid=str(trade.get("trade_uuid") or ""),
                        ticker=ticker,
                        side=side,
                        phase=phase,
                        fill_time=moment,
                        fill_price=fill_price,
                        bar_close=bar_close,
                        bucket=classify_time(moment, opening_minutes=opening_minutes),
                        quantity=qty,
                    )
                )

    return observations, skipped


def _percentile(values: List[float], q: float) -> float:
    """Персентиль без numpy — модуль должен работать и без научного стека."""
    if not values:
        return 0.0
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    low = int(position)
    high = min(low + 1, len(ordered) - 1)
    weight = position - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def summarize(
    observations: Sequence[FillObservation], key: str = "bucket"
) -> Dict[str, Dict[str, Any]]:
    """Сводка проскальзывания по группам (фаза дня, тикер, сторона…)."""
    groups: Dict[str, List[FillObservation]] = {}
    for item in observations:
        groups.setdefault(str(getattr(item, key)), []).append(item)

    result: Dict[str, Dict[str, Any]] = {}
    for name, items in groups.items():
        bps = [item.slippage_bps for item in items]
        result[name] = {
            "исполнений": len(items),
            "среднее_bps": round(sum(bps) / len(bps), 3),
            "медиана_bps": round(_percentile(bps, 0.5), 3),
            "p90_bps": round(_percentile(bps, 0.9), 3),
            "максимум_bps": round(max(bps), 3),
            "сумма_руб": round(sum(item.cost_rub for item in items), 2),
        }
    return result


def format_report(
    observations: Sequence[FillObservation],
    skipped: Dict[str, int],
    *,
    opening_minutes: int = DEFAULT_OPENING_MINUTES,
) -> str:
    """Отчёт: разбивка по фазе дня, тикеру и стороне + вывод про открытие."""
    if not observations:
        return (
            "Нет данных для измерения: в таблице trades нет сделок с ценами "
            "исполнения, либо в candles нет соответствующих баров.\n"
            "Запустите бот в режиме PAPER и повторите после нескольких сделок."
        )

    lines = [
        f"Фактическое проскальзывание: {len(observations)} исполнений",
        f"(окно «открытия» — первые {opening_minutes} мин основной сессии)",
        "=" * 78,
        "",
        "Положительное значение = исполнились ХУЖЕ close бара (заплатили спред).",
        "Именно эту величину надо подставлять в --slippage-bps бэктеста.",
        "",
    ]

    def table(title: str, data: Dict[str, Dict[str, Any]], order: Optional[List[str]] = None):
        lines.append(title)
        lines.append("─" * 78)
        lines.append(
            f"{'группа':<20} {'шт':>5} {'среднее':>9} {'медиана':>9} "
            f"{'p90':>8} {'макс':>8} {'руб':>10}"
        )
        keys = [k for k in (order or sorted(data)) if k in data]
        for name in keys:
            row = data[name]
            lines.append(
                f"{name:<20} {row['исполнений']:>5} {row['среднее_bps']:>9.2f} "
                f"{row['медиана_bps']:>9.2f} {row['p90_bps']:>8.2f} "
                f"{row['максимум_bps']:>8.2f} {row['сумма_руб']:>10.2f}"
            )
        lines.append("")

    by_bucket = summarize(observations, "bucket")
    table("По фазе торгового дня", by_bucket, BUCKET_ORDER)
    table("По инструменту", summarize(observations, "ticker"))
    table("По стороне", summarize(observations, "side"))
    table("Вход / выход", summarize(observations, "phase"))

    if any(skipped.values()):
        lines.append(f"Пропущено исполнений: {skipped}")
        lines.append("")

    lines.append("Вывод")
    lines.append("─" * 78)

    opening = by_bucket.get(BUCKET_OPENING)
    rest = by_bucket.get(BUCKET_MAIN)
    all_bps = [item.slippage_bps for item in observations]
    overall = sum(all_bps) / len(all_bps)
    lines.append(f"Среднее по всем исполнениям: {overall:.2f} bps на ногу.")
    lines.append(
        f"Круг из четырёх исполнений обходится примерно в "
        f"{overall * 4 / 10_000 * 50_000:.0f} ₽ при позиции 50 000 ₽."
    )
    lines.append("")

    if not opening or not rest:
        lines.append(
            "Для сравнения открытия с остальным днём данных пока не хватает: "
            f"на открытии {opening['исполнений'] if opening else 0} исполнений, "
            f"в основной сессии {rest['исполнений'] if rest else 0}. "
            "Нужен ещё период работы в PAPER."
        )
        return "\n".join(lines)

    delta = opening["среднее_bps"] - rest["среднее_bps"]
    share = opening["исполнений"] / len(observations) * 100

    lines.append(
        f"Открытие: {opening['среднее_bps']:.2f} bps (медиана {opening['медиана_bps']:.2f}, "
        f"p90 {opening['p90_bps']:.2f}), {opening['исполнений']} исполнений = {share:.0f}% всех."
    )
    lines.append(
        f"Остальная сессия: {rest['среднее_bps']:.2f} bps "
        f"(медиана {rest['медиана_bps']:.2f}, p90 {rest['p90_bps']:.2f})."
    )
    lines.append("")

    if delta > 0.3:
        lines.append(
            f"⚠️  На открытии дороже на {delta:.2f} bps. Опасение про кластеризацию "
            "подтверждается: стратегия входит там, где спред шире."
        )
        lines.append(
            "   Варианты: (1) запретить входы в первые "
            f"{opening_minutes} минут основной сессии; (2) оставить как есть, но "
            f"закладывать в оценку не среднее, а {opening['среднее_bps']:.2f} bps — "
            "реальные издержки будут ближе к худшему случаю."
        )
        if share > 25:
            lines.append(
                f"   Доля входов на открытии — {share:.0f}%, это много: "
                "запрет первых минут заметно изменит и число сделок, и результат. "
                "Прогоните бэктест с обоими вариантами прежде чем решать."
            )
    elif delta < -0.3:
        lines.append(
            f"На открытии, наоборот, дешевле на {abs(delta):.2f} bps — "
            "опасение про кластеризацию не подтвердилось."
        )
    else:
        lines.append(
            f"Разница между открытием и остальным днём в пределах шума ({delta:+.2f} bps) — "
            "избегать первых минут смысла нет, среднее можно использовать как оценку."
        )

    return "\n".join(lines)


async def load_from_db(
    mode: str = "PAPER", *, limit_candles: int = 20000
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Забрать сделки и бары из Supabase."""
    from app.http_client import close_http_client, get_http_client
    from app.storage.supabase import select_rows

    client = get_http_client()
    try:
        filters = {"mode": f"eq.{mode}"} if mode != "ALL" else None
        trades = await select_rows(
            "trades", client, filters=filters, order="entry_time.asc"
        )
        candles = await select_rows(
            "candles", client, order="timestamp.desc", limit=limit_candles
        )
        return trades, candles
    finally:
        await close_http_client()


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Измерить фактическое проскальзывание по сделкам бота"
    )
    parser.add_argument("--mode", default="PAPER", choices=["PAPER", "DEMO", "LIVE", "ALL"])
    parser.add_argument(
        "--opening-minutes",
        type=int,
        default=DEFAULT_OPENING_MINUTES,
        help="Длина окна «открытия» в минутах основной сессии (по умолчанию 15)",
    )
    parser.add_argument("--csv", help="Куда сохранить наблюдения по исполнениям")
    args = parser.parse_args(argv)

    trades, candles = asyncio.run(load_from_db(args.mode))
    print(f"Сделок в режиме {args.mode}: {len(trades)}, баров в выборке: {len(candles)}\n")

    observations, skipped = measure(
        trades, candles, opening_minutes=args.opening_minutes
    )
    print(format_report(observations, skipped, opening_minutes=args.opening_minutes))

    if args.csv and observations:
        import csv as csv_mod

        with open(args.csv, "w", newline="", encoding="utf-8") as handle:
            writer = csv_mod.writer(handle)
            writer.writerow(
                ["trade_uuid", "ticker", "side", "phase", "fill_time",
                 "fill_price", "bar_close", "bucket", "quantity", "slippage_bps", "cost_rub"]
            )
            for item in observations:
                writer.writerow(
                    [item.trade_uuid, item.ticker, item.side, item.phase,
                     item.fill_time.isoformat(), item.fill_price, item.bar_close,
                     item.bucket, item.quantity, round(item.slippage_bps, 4),
                     round(item.cost_rub, 2)]
                )
        print(f"\nНаблюдения сохранены: {args.csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
