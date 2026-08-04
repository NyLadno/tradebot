"""Слой исполнения: одинаковый интерфейс для PAPER и LIVE.

Движок стратегии ничего не знает про заявки БКС — он просит адаптер
«открыть пару» или «закрыть пару» и получает фактические исполнения.
Это позволяет гонять ту же логику в бумажном режиме без единой заявки
на реальный счёт (у БКС нет sandbox — ошибка в LIVE стоит денег).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Protocol

from app.broker.models import Instrument, Quote, Side
from app.logging_setup import get_logger
from app.strategy import sizing

logger = get_logger("tradebot.strategy.execution")


class ExecutionError(RuntimeError):
    """Пару исполнить не удалось (обе ноги остались неоткрытыми)."""


class LeggingRiskError(RuntimeError):
    """Исполнилась только одна нога — критическая ситуация для парной сделки."""


@dataclass(frozen=True)
class LegOrder:
    """Намерение по одной ноге."""

    ticker: str
    side: Side
    quantity: int
    reference_price: float

    @property
    def is_buy(self) -> bool:
        return self.side is Side.BUY


@dataclass(frozen=True)
class Fill:
    """Фактическое исполнение одной ноги."""

    ticker: str
    side: Side
    quantity: int
    price: float
    commission: float
    time: datetime
    is_simulated: bool = False
    client_order_id: Optional[str] = None

    @property
    def signed_quantity(self) -> int:
        """Количество со знаком: покупка +, продажа −."""
        return self.quantity if self.side is Side.BUY else -self.quantity

    @property
    def notional(self) -> float:
        return sizing.notional(self.quantity, self.price)


@dataclass
class PairFill:
    """Результат исполнения обеих ног."""

    leg1: Fill
    leg2: Fill

    @property
    def total_commission(self) -> float:
        return self.leg1.commission + self.leg2.commission

    @property
    def time(self) -> datetime:
        return max(self.leg1.time, self.leg2.time)


class ExecutionAdapter(Protocol):
    """Контракт, который движок ожидает от режима исполнения."""

    mode: str

    async def execute_pair(self, leg1: LegOrder, leg2: LegOrder) -> PairFill:
        """Открыть обе ноги. Бросает ExecutionError / LeggingRiskError."""
        ...

    async def close_pair(self, leg1: LegOrder, leg2: LegOrder) -> PairFill:
        """Закрыть обе ноги (стороны уже развёрнуты вызывающим кодом)."""
        ...


class QuoteBook:
    """Последние котировки по инструментам — общий источник цен для PAPER."""

    def __init__(self) -> None:
        self._quotes: Dict[str, Quote] = {}

    def update(self, quote: Quote) -> None:
        self._quotes[quote.symbol] = quote

    def get(self, ticker: str) -> Optional[Quote]:
        return self._quotes.get(ticker)

    def is_tradable(self, ticker: str) -> bool:
        quote = self._quotes.get(ticker)
        return quote is not None and quote.is_tradable

    def all_tradable(self, tickers: List[str]) -> bool:
        return all(self.is_tradable(ticker) for ticker in tickers)

    def snapshot(self) -> Dict[str, Dict[str, object]]:
        return {
            ticker: {
                "bid": quote.bid,
                "offer": quote.offer,
                "last": quote.last,
                "trading_status": quote.trading_status,
                "time": quote.time.isoformat() if quote.time else None,
            }
            for ticker, quote in self._quotes.items()
        }


class PaperExecutor:
    """Бумажное исполнение по противоположной стороне стакана.

    Покупка исполняется по ``offer``, продажа по ``bid`` — то есть мы
    платим спред, как в реальности. Это принципиально честнее наивного
    бэктестного «вход по цене закрытия следующей свечи», который
    систематически завышает результат на половину спреда с каждой ноги.

    Если котировки нет, откатываемся на опорную цену (последний close)
    со штрафом в один шаг цены и помечаем это в логах — такие исполнения
    в статистике доверия не заслуживают.
    """

    mode = "PAPER"

    def __init__(
        self,
        quote_book: QuoteBook,
        instruments: Dict[str, Instrument],
        *,
        commission_pct: float = 0.0003,
    ) -> None:
        self._quotes = quote_book
        self._instruments = instruments
        self.commission_pct = commission_pct
        self.degraded_fills = 0

    def _fill_price(self, order: LegOrder) -> tuple[float, bool]:
        """Цена исполнения и признак «котировки не было»."""
        quote = self._quotes.get(order.ticker)
        if quote is not None:
            side_price = quote.offer if order.is_buy else quote.bid
            if side_price and side_price > 0:
                return float(side_price), False

        instrument = self._instruments.get(order.ticker)
        step = instrument.minimum_step if instrument else 0.0
        penalty = step if step > 0 else order.reference_price * 0.0005
        price = order.reference_price + (penalty if order.is_buy else -penalty)
        return max(price, 0.0), True

    def _fill(self, order: LegOrder) -> Fill:
        price, degraded = self._fill_price(order)
        if degraded:
            self.degraded_fills += 1
            logger.warning(
                "[PAPER] Нет котировки по %s — исполняю по опорной цене %.4f "
                "со штрафом; результат сделки менее достоверен",
                order.ticker,
                order.reference_price,
            )
        if price <= 0:
            raise ExecutionError(f"Не удалось определить цену исполнения для {order.ticker}")

        return Fill(
            ticker=order.ticker,
            side=order.side,
            quantity=order.quantity,
            price=price,
            commission=sizing.estimate_commission(order.quantity, price, self.commission_pct),
            time=datetime.now(timezone.utc),
            is_simulated=True,
        )

    async def execute_pair(self, leg1: LegOrder, leg2: LegOrder) -> PairFill:
        if leg1.quantity <= 0 or leg2.quantity <= 0:
            raise ExecutionError(
                f"Нулевой объём ноги: {leg1.ticker}={leg1.quantity}, {leg2.ticker}={leg2.quantity}"
            )
        fills = PairFill(self._fill(leg1), self._fill(leg2))
        logger.info(
            "[PAPER] Пара исполнена: %s %s x%s @%.4f | %s %s x%s @%.4f | комиссия %.2f",
            leg1.side.name, leg1.ticker, fills.leg1.quantity, fills.leg1.price,
            leg2.side.name, leg2.ticker, fills.leg2.quantity, fills.leg2.price,
            fills.total_commission,
        )
        return fills

    async def close_pair(self, leg1: LegOrder, leg2: LegOrder) -> PairFill:
        return await self.execute_pair(leg1, leg2)


def build_close_orders(
    trade: Dict[str, object],
    *,
    leg1_price: float,
    leg2_price: float,
) -> tuple[LegOrder, LegOrder]:
    """Собрать заявки на закрытие: стороны, обратные открытию.

    ``direction`` в таблице trades описывает открытие
    (LONG_TATN_SHORT_TATNP — купили leg1, продали leg2), поэтому
    на закрытии стороны разворачиваются.
    """
    direction = str(trade.get("direction") or "")
    leg1_symbol = str(trade.get("leg1_symbol") or "")
    leg2_symbol = str(trade.get("leg2_symbol") or "")
    leg1_qty = int(float(trade.get("leg1_qty") or 0))
    leg2_qty = int(float(trade.get("leg2_qty") or 0))

    opened_leg1_long = direction.startswith("LONG_")
    close_leg1_side = Side.SELL if opened_leg1_long else Side.BUY
    close_leg2_side = Side.BUY if opened_leg1_long else Side.SELL

    return (
        LegOrder(leg1_symbol, close_leg1_side, leg1_qty, leg1_price),
        LegOrder(leg2_symbol, close_leg2_side, leg2_qty, leg2_price),
    )
