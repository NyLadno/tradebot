"""Типы данных БКС Trade API.

Значения перечислений взяты напрямую из документации
(https://cdn.bcs.ru/static/bcs/files/trade-api-docs.pdf).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, Dict, List, Optional, Tuple


class Side(IntEnum):
    """Направление сделки."""

    BUY = 1
    SELL = 2


class OrderType(IntEnum):
    """Тип заявки."""

    MARKET = 1
    LIMIT = 2


class OrderStatus(IntEnum):
    """Состояние заявки (поле data.orderStatus)."""

    NEW = 0
    PARTIALLY_FILLED = 1
    FILLED = 2
    CANCELED = 4
    REPLACED = 5
    PENDING_CANCEL = 6
    REJECTED = 8
    PENDING_REPLACE = 9
    PENDING_NEW = 10

    @property
    def is_terminal(self) -> bool:
        """True для статусов, после которых заявка больше не изменится."""
        return self in (
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.REPLACED,
        )

    @property
    def is_dead(self) -> bool:
        """True, если заявка завершилась без полного исполнения."""
        return self in (OrderStatus.CANCELED, OrderStatus.REJECTED)


class RejectReason(IntEnum):
    """Причина отклонения (data.rejectReason)."""

    TOO_LATE_TO_CANCEL = 0
    UNKNOWN_ORDER = 1
    BROKER_OPTION = 2
    ALREADY_IN_PENDING_STATE = 3


class TradingStatus(IntEnum):
    """securityTradingStatus из потока котировок."""

    TRADING_HALL = 2  # сессия заморожена
    READY_TO_TRADE = 17  # сессия открыта — единственный статус, в котором торгуем
    NOT_AVAILABLE = 18  # сессия закрыта
    CLOSING = 100
    OPENING = 101
    AUCTION = 102
    CLOSING_AUCTION = 103


class DataType(IntEnum):
    """dataType в подписке market-data WebSocket."""

    ORDER_BOOK = 0
    CANDLES = 1
    LAST_TRADES = 2
    QUOTES = 3


class SubscribeAction(IntEnum):
    """subscribeType / subscriberType в подписке."""

    SUBSCRIBE = 0
    UNSUBSCRIBE = 1


class StreamErrorCode(IntEnum):
    """Коды ошибок внутри WebSocket-сообщений."""

    UNDEFINED = 0
    NO_DATA = 1  # нет данных от источника (FixAdapter down)
    NOT_FOUND = 2  # инструмент не найден
    INVALID_JSON = 3


def parse_bcs_datetime(raw: Any) -> Optional[datetime]:
    """Разобрать ISO 8601 из ответа БКС в timezone-aware datetime (UTC).

    БКС отдаёт как ``2024-10-30T09:01:00.000Z``, так и варианты без ``Z``.
    Наивные значения трактуем как UTC — так же, как их отдаёт candles-chart.
    """
    if raw is None:
        return None
    if isinstance(raw, datetime):
        dt = raw
    else:
        text = str(raw).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_bcs_datetime(dt: datetime) -> str:
    """Сериализовать datetime в формат, который принимает candles-chart."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True)
class Bar:
    """Одна OHLCV-свеча."""

    symbol: str
    time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: Optional[int] = None

    @classmethod
    def from_rest(cls, symbol: str, raw: Dict[str, Any]) -> Optional["Bar"]:
        """Собрать бар из элемента ``bars[]`` ответа /candles-chart."""
        time = parse_bcs_datetime(raw.get("time"))
        return cls._build(symbol, time, raw)

    @classmethod
    def from_stream(cls, symbol: str, raw: Dict[str, Any]) -> Optional["Bar"]:
        """Собрать бар из контейнера ``candleStick`` WebSocket-сообщения."""
        time = parse_bcs_datetime(raw.get("dateTimeUtc") or raw.get("time"))
        return cls._build(symbol, time, raw)

    @classmethod
    def _build(
        cls, symbol: str, time: Optional[datetime], raw: Dict[str, Any]
    ) -> Optional["Bar"]:
        if time is None:
            return None
        try:
            close = float(raw["close"])
            open_ = float(raw.get("open", close))
            high = float(raw.get("high", close))
            low = float(raw.get("low", close))
        except (KeyError, TypeError, ValueError):
            return None
        volume = raw.get("volume")
        try:
            volume_int = int(volume) if volume is not None else None
        except (TypeError, ValueError):
            volume_int = None
        return cls(
            symbol=symbol,
            time=time,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume_int,
        )

    def to_db_row(self, source: str = "BKS") -> Dict[str, Any]:
        """Строка для таблицы ``candles`` (имена колонок совпадают со схемой)."""
        row: Dict[str, Any] = {
            "symbol": self.symbol,
            "timestamp": self.time.isoformat(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "source": source,
        }
        if self.volume is not None:
            row["volume"] = self.volume
        return row


@dataclass(frozen=True)
class Quote:
    """Снимок котировки из потока dataType=3."""

    symbol: str
    time: Optional[datetime]
    bid: Optional[float]
    offer: Optional[float]
    last: Optional[float]
    close: Optional[float]
    trading_status: Optional[int]

    @property
    def is_tradable(self) -> bool:
        """True только при securityTradingStatus == 17 (Ready to trade)."""
        return self.trading_status == TradingStatus.READY_TO_TRADE

    @property
    def mid(self) -> Optional[float]:
        """Середина спреда, если есть обе стороны."""
        if self.bid is None or self.offer is None:
            return None
        return (self.bid + self.offer) / 2.0

    @classmethod
    def from_stream(cls, raw: Dict[str, Any]) -> Optional["Quote"]:
        symbol = raw.get("ticker")
        if not symbol:
            return None

        def _num(key: str) -> Optional[float]:
            value = raw.get(key)
            try:
                return float(value) if value is not None else None
            except (TypeError, ValueError):
                return None

        status = raw.get("securityTradingStatus")
        try:
            status_int = int(status) if status is not None else None
        except (TypeError, ValueError):
            status_int = None

        return cls(
            symbol=str(symbol),
            time=parse_bcs_datetime(raw.get("dateTime")),
            bid=_num("bid"),
            offer=_num("offer"),
            last=_num("last"),
            close=_num("close"),
            trading_status=status_int,
        )


@dataclass(frozen=True)
class OrderBook:
    """Стакан из потока dataType=0."""

    symbol: str
    time: Optional[datetime]
    bids: List[Tuple[float, float]] = field(default_factory=list)
    asks: List[Tuple[float, float]] = field(default_factory=list)

    @property
    def best_bid(self) -> Optional[float]:
        return self.bids[0][0] if self.bids else None

    @property
    def best_ask(self) -> Optional[float]:
        return self.asks[0][0] if self.asks else None

    @classmethod
    def from_stream(cls, raw: Dict[str, Any]) -> Optional["OrderBook"]:
        symbol = raw.get("ticker")
        if not symbol:
            return None

        def _levels(key: str) -> List[Tuple[float, float]]:
            out: List[Tuple[float, float]] = []
            for level in raw.get(key) or []:
                try:
                    out.append((float(level["price"]), float(level["quantity"])))
                except (KeyError, TypeError, ValueError):
                    continue
            return out

        return cls(
            symbol=str(symbol),
            time=parse_bcs_datetime(raw.get("dateTime")),
            bids=_levels("bids"),
            asks=_levels("asks"),
        )


@dataclass(frozen=True)
class Instrument:
    """Инструмент из /instruments/by-tickers.

    Из всего огромного ответа нам нужны только торговые характеристики.
    """

    ticker: str
    lot_size: int
    minimum_step: float
    scale: int
    is_can_short: bool
    is_can_margin: bool
    is_blocked: bool
    primary_board: Optional[str]
    class_codes: List[str] = field(default_factory=list)

    @classmethod
    def from_response(cls, raw: Dict[str, Any]) -> Optional["Instrument"]:
        ticker = raw.get("ticker")
        if not ticker:
            return None
        try:
            lot_size = int(float(raw.get("lotSize") or 1))
        except (TypeError, ValueError):
            lot_size = 1
        try:
            minimum_step = float(raw.get("minimumStep") or 0.0)
        except (TypeError, ValueError):
            minimum_step = 0.0
        try:
            scale = int(raw.get("scale") or 2)
        except (TypeError, ValueError):
            scale = 2
        boards = [
            board.get("classCode")
            for board in (raw.get("boards") or [])
            if isinstance(board, dict) and board.get("classCode")
        ]
        return cls(
            ticker=str(ticker),
            lot_size=max(1, lot_size),
            minimum_step=minimum_step,
            scale=scale,
            is_can_short=bool(raw.get("isCanShort")),
            is_can_margin=bool(raw.get("isCanMargin")),
            is_blocked=bool(raw.get("isBlocked")),
            primary_board=raw.get("primaryBoard"),
            class_codes=boards,
        )


@dataclass
class OrderState:
    """Разобранный ответ GET /orders/{id} или сообщение из потока заявок."""

    client_order_id: str
    original_client_order_id: Optional[str] = None
    status: Optional[OrderStatus] = None
    ticker: Optional[str] = None
    side: Optional[Side] = None
    order_type: Optional[OrderType] = None
    order_quantity: float = 0.0
    executed_quantity: float = 0.0
    remained_quantity: float = 0.0
    average_price: Optional[float] = None
    price: Optional[float] = None
    commission: float = 0.0
    execution_value: Optional[float] = None
    transaction_time: Optional[datetime] = None
    reject_reason: Optional[int] = None
    order_id: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.status is not None and self.status.is_terminal

    @property
    def fill_price(self) -> Optional[float]:
        """Средняя цена исполнения, с откатом на цену заявки."""
        if self.average_price:
            return self.average_price
        return self.price

    @classmethod
    def from_response(cls, raw: Dict[str, Any]) -> "OrderState":
        """Разобрать ответ БКС; ``data`` может быть null при ошибке."""
        data = raw.get("data") or {}

        def _num(key: str, default: float = 0.0) -> float:
            value = data.get(key)
            try:
                return float(value) if value is not None else default
            except (TypeError, ValueError):
                return default

        def _opt_num(key: str) -> Optional[float]:
            value = data.get(key)
            try:
                return float(value) if value is not None else None
            except (TypeError, ValueError):
                return None

        def _enum(enum_cls, key: str):
            value = data.get(key)
            if value is None:
                return None
            try:
                return enum_cls(int(value))
            except (TypeError, ValueError):
                return None

        return cls(
            client_order_id=str(raw.get("clientOrderId") or ""),
            original_client_order_id=raw.get("originalClientOrderId"),
            status=_enum(OrderStatus, "orderStatus"),
            ticker=data.get("ticker"),
            side=_enum(Side, "side"),
            order_type=_enum(OrderType, "orderType"),
            order_quantity=_num("orderQuantity"),
            executed_quantity=_num("executedQuantity"),
            remained_quantity=_num("remainedQuantity"),
            average_price=_opt_num("averagePrice"),
            price=_opt_num("price"),
            commission=_num("commission"),
            execution_value=_opt_num("executionValue"),
            transaction_time=parse_bcs_datetime(data.get("transactionTime")),
            reject_reason=(
                int(data["rejectReason"]) if data.get("rejectReason") is not None else None
            ),
            order_id=data.get("orderId"),
            raw=raw,
        )
