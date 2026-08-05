"""WebSocket-потоки БКС Trade API.

Два потока:

* ``BcsMarketDataStream`` — market-data ws: свечи (dataType=1),
  котировки (dataType=3), стакан (dataType=0).
* ``BcsOrderStream`` — статусы и исполнения заявок.

Важная особенность из документации (раздел «Особенности работы с информацией
о заявках через стриминг»): если открыть поток **после** создания заявки,
данные по ней в поток не попадут. Поэтому ``BcsOrderStream`` поднимается
до первой заявки, а REST-опрос статуса остаётся подстраховкой.

Ещё одно: документация непоследовательна в имени поля подписки —
в разделах «Стакан» и «Котировки» это ``subscribeType``, в разделах
«Последняя свеча» и «Обезличенные сделки» — ``subscriberType``. Отправляем
оба ключа с одинаковым значением: лишнее поле безопаснее пропущенного.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional, Sequence

import websockets

from app.broker.auth import BcsTokenManager
from app.broker.models import (
    Bar,
    DataType,
    OrderBook,
    OrderState,
    Quote,
    SubscribeAction,
)
from app.config import settings
from app.logging_setup import get_logger

logger = get_logger("tradebot.bcs.ws")

MARKET_DATA_WS = "/trade-api-market-data-connector/api/v1/market-data/ws"
ORDER_TRANSACTION_WS = "/trade-api-bff-operations/api/v1/orders/transaction/ws"
ORDER_EXECUTION_WS = "/trade-api-bff-operations/api/v1/orders/execution/ws"


async def _connect(url: str, token: str):
    """Открыть соединение, переживая переименование параметра в websockets 14."""
    kwargs: Dict[str, Any] = {
        "ping_interval": settings.bcs_ws_ping_interval,
        "ping_timeout": settings.bcs_ws_ping_interval,
        "close_timeout": 5,
    }
    headers = {"Authorization": f"Bearer {token}"}
    try:
        return await websockets.connect(url, additional_headers=headers, **kwargs)
    except TypeError:
        return await websockets.connect(url, extra_headers=headers, **kwargs)


class _BaseStream:
    """Общая механика: подключение, реконнект с backoff, цикл чтения."""

    name = "ws"

    def __init__(self, token_manager: BcsTokenManager, url: str) -> None:
        self._tokens = token_manager
        self._url = url
        self._ws: Any = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self.connected = False
        self.last_message_at: Optional[datetime] = None
        self.connect_attempts = 0

    def start(self, http_client) -> asyncio.Task:
        """Запустить фоновую задачу чтения потока."""
        self._running = True
        self._task = asyncio.create_task(self.run(http_client), name=f"bcs-{self.name}")
        return self._task

    async def stop(self) -> None:
        """Остановить поток и дождаться завершения задачи."""
        self._running = False
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:  # noqa: BLE001 — закрытие не должно ронять shutdown
                pass
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._task = None
        self.connected = False

    async def run(self, http_client) -> None:
        """Вечный цикл: подключиться, подписаться, читать, при обрыве — заново."""
        backoff = settings.bcs_ws_reconnect_min
        while self._running:
            try:
                token = await self._tokens.get_access_token(http_client)
                self.connect_attempts += 1
                async with await _connect(self._url, token) as ws:
                    self._ws = ws
                    self.connected = True
                    backoff = settings.bcs_ws_reconnect_min
                    logger.info("[WS] %s подключён: %s", self.name, self._url)
                    await self._on_connected()
                    async for raw in ws:
                        self.last_message_at = datetime.now(timezone.utc)
                        await self._handle_raw(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — поток не должен умирать
                if not self._running:
                    break
                logger.warning(
                    "[WS] %s оборван (%s), переподключение через %.0f сек",
                    self.name,
                    exc,
                    backoff,
                )
            finally:
                self.connected = False
                self._ws = None

            if not self._running:
                break
            await asyncio.sleep(backoff)
            backoff = min(settings.bcs_ws_reconnect_max, backoff * 2)

    async def _send(self, payload: Dict[str, Any]) -> None:
        if self._ws is None:
            raise RuntimeError(f"{self.name}: соединение не установлено")
        await self._ws.send(json.dumps(payload))

    async def _on_connected(self) -> None:
        """Хук: восстановить подписки после (пере)подключения."""

    async def _handle_raw(self, raw: Any) -> None:
        try:
            message = json.loads(raw)
        except (TypeError, ValueError):
            logger.warning("[WS] %s: не-JSON сообщение: %s", self.name, str(raw)[:200])
            return
        if not isinstance(message, dict):
            return
        try:
            await self._handle_message(message)
        except Exception as exc:  # noqa: BLE001
            logger.error("[WS] %s: ошибка обработки сообщения: %s", self.name, exc)

    async def _handle_message(self, message: Dict[str, Any]) -> None:
        raise NotImplementedError


class BcsMarketDataStream(_BaseStream):
    """Поток свечей / котировок / стакана по инструментам."""

    name = "market-data"

    def __init__(
        self,
        token_manager: BcsTokenManager,
        *,
        on_candle: Optional[Callable[[Bar], Awaitable[None]]] = None,
        on_quote: Optional[Callable[[Quote], Awaitable[None]]] = None,
        on_orderbook: Optional[Callable[[OrderBook], Awaitable[None]]] = None,
        ws_base: Optional[str] = None,
    ) -> None:
        base = (ws_base or settings.bcs_ws_base).rstrip("/")
        super().__init__(token_manager, f"{base}{MARKET_DATA_WS}")
        self._on_candle = on_candle
        self._on_quote = on_quote
        self._on_orderbook = on_orderbook
        self._subscriptions: List[Dict[str, Any]] = []
        self._known_tickers: set[str] = set()
        self._logged_samples: set[str] = set()
        self.last_event_at: Dict[str, datetime] = {}

    # --- подписки ------------------------------------------------------

    @staticmethod
    def _subscription(
        data_type: DataType,
        tickers: Sequence[str],
        class_code: str,
        **extra: Any,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            # Документация называет это поле по-разному в разных разделах —
            # отправляем оба варианта.
            "subscribeType": int(SubscribeAction.SUBSCRIBE),
            "subscriberType": int(SubscribeAction.SUBSCRIBE),
            "dataType": int(data_type),
            "instruments": [
                {"ticker": ticker, "classCode": class_code} for ticker in tickers
            ],
        }
        payload.update(extra)
        return payload

    async def subscribe_candles(
        self,
        tickers: Sequence[str],
        *,
        timeframe: str = "M1",
        class_code: Optional[str] = None,
    ) -> None:
        """Подписаться на последнюю свечу выбранного таймфрейма."""
        payload = self._subscription(
            DataType.CANDLES,
            tickers,
            class_code or settings.bcs_class_code,
            timeFrame=timeframe,
        )
        await self._register(payload, tickers)

    async def subscribe_quotes(
        self, tickers: Sequence[str], *, class_code: Optional[str] = None
    ) -> None:
        """Подписаться на котировки (bid/offer/last и статус сессии)."""
        payload = self._subscription(
            DataType.QUOTES, tickers, class_code or settings.bcs_class_code
        )
        await self._register(payload, tickers)

    async def subscribe_orderbook(
        self,
        tickers: Sequence[str],
        *,
        depth: int = 20,
        class_code: Optional[str] = None,
    ) -> None:
        """Подписаться на стакан заданной глубины (1..20)."""
        payload = self._subscription(
            DataType.ORDER_BOOK,
            tickers,
            class_code or settings.bcs_class_code,
            depth=max(1, min(20, depth)),
        )
        await self._register(payload, tickers)

    async def _register(self, payload: Dict[str, Any], tickers: Sequence[str]) -> None:
        self._subscriptions.append(payload)
        self._known_tickers.update(tickers)
        if self.connected:
            await self._send(payload)

    async def _on_connected(self) -> None:
        for payload in self._subscriptions:
            await self._send(payload)
        if self._subscriptions:
            logger.info(
                "[WS] market-data: восстановлено подписок: %s", len(self._subscriptions)
            )

    # --- разбор сообщений ----------------------------------------------

    def _resolve_ticker(self, message: Dict[str, Any]) -> Optional[str]:
        """Достать тикер из сообщения.

        В разделе «Последняя свеча» документация перепутала описания
        ``code`` и ``exchange`` внутри ``instrument``, поэтому берём то из
        двух значений, которое совпадает с подписанными тикерами.
        """
        instrument = message.get("instrument") or {}
        candidates = [
            message.get("ticker"),
            instrument.get("code"),
            instrument.get("ticker"),
            instrument.get("exchange"),
        ]
        for candidate in candidates:
            if candidate and str(candidate) in self._known_tickers:
                return str(candidate)
        for candidate in candidates:
            if candidate:
                return str(candidate)
        return None

    def _log_sample_once(self, kind: str, message: Dict[str, Any]) -> None:
        """Один раз залогировать сырое сообщение каждого типа.

        Нужно, чтобы на живом подключении сверить фактические имена полей
        с документацией, в которой есть расхождения.
        """
        if kind in self._logged_samples:
            return
        self._logged_samples.add(kind)
        logger.info("[WS] Первое сообщение типа %s: %s", kind, json.dumps(message)[:1000])

    async def _handle_message(self, message: Dict[str, Any]) -> None:
        error = message.get("error")
        if isinstance(error, dict) and (error.get("message") or error.get("code")):
            logger.error(
                "[WS] market-data вернул ошибку: code=%s message=%s",
                error.get("code"),
                error.get("message"),
            )
            return

        response_type = str(message.get("responseType") or "")
        ticker = self._resolve_ticker(message)

        candle_raw = message.get("candleStick")
        if isinstance(candle_raw, dict) and ticker:
            self._log_sample_once("candle", message)
            bar = Bar.from_stream(ticker, candle_raw)
            if bar is not None:
                self.last_event_at[ticker] = datetime.now(timezone.utc)
                if self._on_candle is not None:
                    await self._on_candle(bar)
            return

        if response_type.lower().startswith("quote"):
            self._log_sample_once("quotes", message)
            quote = Quote.from_stream(message)
            if quote is not None:
                self.last_event_at[quote.symbol] = datetime.now(timezone.utc)
                if self._on_quote is not None:
                    await self._on_quote(quote)
            return

        if response_type.startswith("OrderBook"):
            self._log_sample_once("orderbook", message)
            book = OrderBook.from_stream(message)
            if book is not None:
                self.last_event_at[book.symbol] = datetime.now(timezone.utc)
                if self._on_orderbook is not None:
                    await self._on_orderbook(book)
            return

        if message.get("subscribeResponse"):
            logger.info("[WS] Подписка подтверждена: %s", json.dumps(message)[:300])

    # --- состояние для мониторинга -------------------------------------

    def seconds_since_event(self, ticker: str) -> Optional[float]:
        """Сколько секунд назад приходили данные по инструменту."""
        stamp = self.last_event_at.get(ticker)
        if stamp is None:
            return None
        return (datetime.now(timezone.utc) - stamp).total_seconds()


class BcsOrderStream(_BaseStream):
    """Поток статусов заявок (transaction/ws) или исполнений (execution/ws)."""

    def __init__(
        self,
        token_manager: BcsTokenManager,
        *,
        on_order: Callable[[OrderState], Awaitable[None]],
        kind: str = "transaction",
        ws_base: Optional[str] = None,
    ) -> None:
        base = (ws_base or settings.bcs_ws_base).rstrip("/")
        path = ORDER_EXECUTION_WS if kind == "execution" else ORDER_TRANSACTION_WS
        super().__init__(token_manager, f"{base}{path}")
        self.name = f"orders-{kind}"
        self._on_order = on_order

    async def _handle_message(self, message: Dict[str, Any]) -> None:
        if not message.get("data") and not message.get("clientOrderId"):
            return
        state = OrderState.from_response(message)
        logger.info(
            "[WS] %s: заявка %s статус=%s исполнено=%s/%s",
            self.name,
            state.client_order_id or state.original_client_order_id,
            state.status.name if state.status else "?",
            state.executed_quantity,
            state.order_quantity,
        )
        await self._on_order(state)
