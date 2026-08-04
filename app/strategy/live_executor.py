"""Исполнение реальных заявок в БКС.

Отдельный модуль, а не часть ``execution.py``, чтобы бумажный режим
физически не зависел от кода, отправляющего заявки на реальный счёт.

Главный риск парной сделки — **legging risk**: одна нога исполнилась,
вторая нет. Тогда вместо рыночно-нейтральной позиции остаётся голая
направленная, и убыток ничем не ограничен. Политика здесь жёсткая:
исполненную ногу немедленно раскрываем по рынку, пишем CRITICAL,
уведомляем и поднимаем аварийный флаг. Никаких попыток «доисполнить».

У БКС нет sandbox: всё, что делает этот модуль, происходит с реальными
деньгами. Поэтому он включается только при ``BCS_TRADING_MODE=LIVE``
и после предстартовых проверок (шорт, маржа, доступность инструментов).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.broker.errors import BcsError
from app.broker.models import Instrument, OrderState, OrderStatus, OrderType, Side
from app.broker.rest import BcsRestClient, new_client_order_id
from app.broker.ws import BcsOrderStream
from app.config import settings
from app.logging_setup import get_logger
from app.storage.logs import log_event
from app.strategy import sizing
from app.strategy.execution import (
    ExecutionError,
    Fill,
    LeggingRiskError,
    LegOrder,
    PairFill,
    QuoteBook,
)

logger = get_logger("tradebot.strategy.live")

# На сколько шагов цены «продавливаем» лимитную заявку, чтобы она
# исполнилась почти как рыночная, но с ограничением сверху/снизу.
DEFAULT_SLIPPAGE_STEPS = 3


class PreflightError(RuntimeError):
    """Счёт не готов к LIVE-торговле парной стратегией."""


class LiveExecutor:
    """Отправляет реальные заявки в БКС и следит за их исполнением."""

    mode = "LIVE"

    def __init__(
        self,
        rest: BcsRestClient,
        quote_book: QuoteBook,
        instruments: Dict[str, Instrument],
        http_client: httpx.AsyncClient,
        *,
        commission_pct: float = 0.0003,
        notifier: Optional[Any] = None,
        slippage_steps: int = DEFAULT_SLIPPAGE_STEPS,
    ) -> None:
        self._rest = rest
        self._quotes = quote_book
        self._instruments = instruments
        self._http = http_client
        self.commission_pct = commission_pct
        self._notifier = notifier
        self._slippage_steps = slippage_steps

        # Состояния заявок, пришедшие из потока исполнений.
        self._order_states: Dict[str, OrderState] = {}
        self._streams: List[BcsOrderStream] = []
        self.emergency_reason: Optional[str] = None
        # Первое LIVE-исполнение сверяем с портфелем: документация
        # неоднозначна в том, штуки или лоты ожидает orderQuantity.
        self._quantity_semantics_checked = False

    # ------------------------------------------------------------------
    # Запуск
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Поднять потоки заявок ДО первой заявки.

        Документация прямо предупреждает: если открыть стрим после создания
        заявки, данные по ней в стрим не попадут. REST-опрос остаётся
        подстраховкой, но основной канал — поток.
        """
        for kind in ("execution", "transaction"):
            stream = BcsOrderStream(
                self._rest._write_manager(),
                on_order=self._on_order_update,
                kind=kind,
            )
            stream.start(self._http)
            self._streams.append(stream)

        for _ in range(100):
            if all(stream.connected for stream in self._streams):
                break
            await asyncio.sleep(0.1)

        connected = [stream.name for stream in self._streams if stream.connected]
        logger.info("[LIVE] Потоки заявок подключены: %s", ", ".join(connected) or "нет")

    async def stop(self) -> None:
        for stream in self._streams:
            await stream.stop()
        self._streams = []

    async def _on_order_update(self, state: OrderState) -> None:
        key = state.original_client_order_id or state.client_order_id
        if key:
            self._order_states[key] = state

    # ------------------------------------------------------------------
    # Предстартовые проверки
    # ------------------------------------------------------------------

    async def preflight(self, leg1: str, leg2: str) -> Dict[str, Any]:
        """Проверить, что счёт вообще способен исполнить эту стратегию.

        Парная торговля требует шорта одной ноги, а значит маржинальной
        торговли. Выяснять это в момент открытия позиции поздно: первая
        нога уже исполнится.
        """
        report: Dict[str, Any] = {"ok": True, "problems": [], "checks": {}}

        def fail(message: str) -> None:
            report["ok"] = False
            report["problems"].append(message)

        for symbol in (leg1, leg2):
            instrument = self._instruments.get(symbol)
            if instrument is None:
                fail(f"инструмент {symbol} не найден в справочнике БКС")
                continue
            report["checks"][symbol] = {
                "lot_size": instrument.lot_size,
                "minimum_step": instrument.minimum_step,
                "is_can_short": instrument.is_can_short,
                "is_can_margin": instrument.is_can_margin,
                "is_blocked": instrument.is_blocked,
            }
            if instrument.is_blocked:
                fail(f"{symbol}: класс заблокирован")
            if not instrument.is_can_short:
                fail(
                    f"{symbol}: шорт недоступен — парная стратегия в LIVE неисполнима"
                )

        try:
            discounts = await self._rest.get_discounts()
            by_ticker = {
                str(row.get("ticker")): row for row in discounts if isinstance(row, dict)
            }
            for symbol in (leg1, leg2):
                row = by_ticker.get(symbol)
                if row is None:
                    report["problems"].append(
                        f"{symbol}: нет ставки дисконта (маржа может быть недоступна)"
                    )
                    continue
                report["checks"].setdefault(symbol, {})["discount_short"] = row.get(
                    "discountShort"
                )
                report["checks"][symbol]["discount_long"] = row.get("discountLong")
        except BcsError as exc:
            report["problems"].append(f"не удалось прочитать дисконты: {exc}")

        try:
            limits = await self._rest.get_limits()
            money = limits.get("moneyLimit") or {}
            report["checks"]["money_limit"] = money
        except BcsError as exc:
            report["problems"].append(f"не удалось прочитать лимиты: {exc}")

        if report["ok"]:
            logger.info("[LIVE] Предстартовые проверки пройдены: %s", report["checks"])
        else:
            logger.error(
                "[LIVE] Предстартовые проверки НЕ пройдены: %s",
                "; ".join(report["problems"]),
            )
        return report

    # ------------------------------------------------------------------
    # Исполнение пары
    # ------------------------------------------------------------------

    async def execute_pair(self, leg1: LegOrder, leg2: LegOrder) -> PairFill:
        """Открыть обе ноги. При частичном исполнении — раскрыть и остановиться."""
        return await self._execute_both(leg1, leg2)

    async def close_pair(self, leg1: LegOrder, leg2: LegOrder) -> PairFill:
        """Закрыть обе ноги.

        На закрытии не сдаёмся при отказе одной ноги: оставить половину
        позиции висеть хуже, чем повторить попытку по рынку.
        """
        return await self._execute_both(leg1, leg2, unwind_on_failure=False)

    async def _execute_both(
        self, leg1: LegOrder, leg2: LegOrder, *, unwind_on_failure: bool = True
    ) -> PairFill:
        if leg1.quantity <= 0 or leg2.quantity <= 0:
            raise ExecutionError(
                f"Нулевой объём ноги: {leg1.ticker}={leg1.quantity}, {leg2.ticker}={leg2.quantity}"
            )

        results = await asyncio.gather(
            self._submit_and_wait(leg1),
            self._submit_and_wait(leg2),
            return_exceptions=True,
        )
        fill1, fill2 = self._unpack(results, (leg1, leg2))

        if fill1 is not None and fill2 is not None:
            await self._verify_quantity_semantics(fill1)
            return self._balance(fill1, fill2, leg1, leg2)

        # Ниже — обработка legging risk.
        filled = [
            (fill, order)
            for fill, order in ((fill1, leg1), (fill2, leg2))
            if fill is not None
        ]
        missing = [order.ticker for fill, order in ((fill1, leg1), (fill2, leg2)) if fill is None]

        if not filled:
            raise ExecutionError(
                f"Ни одна нога не исполнена ({', '.join(missing)}); позиция не открыта"
            )

        message = (
            f"Исполнена только нога {filled[0][1].ticker}, "
            f"а {', '.join(missing)} — нет. Позиция не рыночно-нейтральна."
        )
        logger.critical("[LIVE] %s", message)
        await log_event("CRITICAL", "strategy", message, self._http)

        if not unwind_on_failure:
            raise LeggingRiskError(message)

        await self._unwind(filled[0][0])
        raise LeggingRiskError(message + " Исполненная нога раскрыта по рынку.")

    @staticmethod
    def _unpack(
        results: List[Any], orders: Tuple[LegOrder, LegOrder]
    ) -> Tuple[Optional[Fill], Optional[Fill]]:
        fills: List[Optional[Fill]] = []
        for result, order in zip(results, orders):
            if isinstance(result, Fill):
                fills.append(result)
            else:
                if isinstance(result, BaseException):
                    logger.error("[LIVE] Нога %s не исполнена: %s", order.ticker, result)
                fills.append(None)
        return fills[0], fills[1]

    def _balance(
        self, fill1: Fill, fill2: Fill, leg1: LegOrder, leg2: LegOrder
    ) -> PairFill:
        """Предупредить, если фактические объёмы разошлись с задуманными."""
        if fill1.quantity != leg1.quantity or fill2.quantity != leg2.quantity:
            logger.warning(
                "[LIVE] Частичное исполнение: %s %s из %s, %s %s из %s. "
                "Учитываю фактические объёмы.",
                leg1.ticker, fill1.quantity, leg1.quantity,
                leg2.ticker, fill2.quantity, leg2.quantity,
            )
        return PairFill(fill1, fill2)

    # ------------------------------------------------------------------
    # Одна заявка
    # ------------------------------------------------------------------

    async def _submit_and_wait(self, order: LegOrder) -> Fill:
        """Отправить лимитную заявку и дождаться терминального статуса."""
        client_order_id = new_client_order_id()
        instrument = self._instruments.get(order.ticker)
        price = self._limit_price(order, instrument)

        await self._rest.create_order(
            client_order_id=client_order_id,
            ticker=order.ticker,
            side=order.side,
            order_type=OrderType.LIMIT,
            quantity=order.quantity,
            price=price,
        )

        state = await self._await_terminal(client_order_id)

        if state is None:
            # Статус неизвестен — отменяем и проверяем, что успело исполниться.
            logger.error(
                "[LIVE] %s: заявка %s не дошла до терминального статуса за %.0f с — отменяю",
                order.ticker, client_order_id, settings.bcs_order_timeout_sec,
            )
            await self._safe_cancel(client_order_id)
            state = await self._poll_once(client_order_id)

        if state is None or state.executed_quantity <= 0:
            reason = (
                f"статус={state.status.name}" if state and state.status else "статус неизвестен"
            )
            raise ExecutionError(
                f"{order.ticker}: заявка {client_order_id} не исполнена ({reason})"
            )

        if state.status is OrderStatus.PARTIALLY_FILLED:
            # Остаток нам не нужен: пара балансируется по фактическому объёму.
            await self._safe_cancel(client_order_id)

        fill_price = state.fill_price or price
        commission = state.commission or sizing.estimate_commission(
            int(state.executed_quantity), fill_price, self.commission_pct
        )
        logger.info(
            "[LIVE] %s %s x%s @%.4f (заявка %s, комиссия %.2f)",
            order.side.name, order.ticker, int(state.executed_quantity),
            fill_price, client_order_id, commission,
        )
        return Fill(
            ticker=order.ticker,
            side=order.side,
            quantity=int(state.executed_quantity),
            price=float(fill_price),
            commission=float(commission),
            time=state.transaction_time or datetime.now(timezone.utc),
            is_simulated=False,
            client_order_id=client_order_id,
        )

    def _limit_price(self, order: LegOrder, instrument: Optional[Instrument]) -> float:
        """Цена лимитной заявки от противоположной стороны стакана."""
        quote = self._quotes.get(order.ticker)
        reference = order.reference_price
        if quote is not None:
            side_price = quote.offer if order.is_buy else quote.bid
            if side_price and side_price > 0:
                reference = float(side_price)
        return sizing.protective_limit_price(
            reference,
            is_buy=order.is_buy,
            instrument=instrument,
            slippage_steps=self._slippage_steps,
        )

    async def _await_terminal(self, client_order_id: str) -> Optional[OrderState]:
        """Ждать терминального статуса: сперва из потока, затем опросом."""
        deadline = asyncio.get_running_loop().time() + settings.bcs_order_timeout_sec

        while asyncio.get_running_loop().time() < deadline:
            streamed = self._order_states.get(client_order_id)
            if streamed is not None and streamed.is_terminal:
                return streamed
            if streamed is not None and streamed.status is OrderStatus.PARTIALLY_FILLED:
                # Частичное исполнение терминальным не считается, но если
                # заявка так и висит, отдадим то, что есть, по таймауту.
                pass

            polled = await self._poll_once(client_order_id)
            if polled is not None and polled.is_terminal:
                return polled

            await asyncio.sleep(settings.bcs_order_poll_sec)

        # Таймаут: возвращаем частичное исполнение, если оно есть.
        last = self._order_states.get(client_order_id) or await self._poll_once(
            client_order_id
        )
        if last is not None and last.executed_quantity > 0:
            return last
        return None

    async def _poll_once(self, client_order_id: str) -> Optional[OrderState]:
        try:
            state = await self._rest.get_order(client_order_id)
        except BcsError as exc:
            logger.debug("[LIVE] Опрос заявки %s: %s", client_order_id, exc)
            return None
        self._order_states[client_order_id] = state
        return state

    async def _safe_cancel(self, client_order_id: str) -> None:
        try:
            await self._rest.cancel_order(client_order_id)
        except BcsError as exc:
            logger.warning("[LIVE] Отмена заявки %s не удалась: %s", client_order_id, exc)

    # ------------------------------------------------------------------
    # Аварийное раскрытие
    # ------------------------------------------------------------------

    async def _unwind(self, fill: Fill) -> None:
        """Немедленно закрыть исполненную ногу рыночной заявкой.

        Единственное место, где мы шлём рыночную заявку: скорость здесь
        важнее цены, потому что голая направленная позиция ничем не покрыта.
        """
        opposite = Side.SELL if fill.side is Side.BUY else Side.BUY
        client_order_id = new_client_order_id()
        logger.critical(
            "[LIVE] Раскрываю ногу %s: %s x%s по рынку",
            fill.ticker, opposite.name, fill.quantity,
        )
        try:
            await self._rest.create_order(
                client_order_id=client_order_id,
                ticker=fill.ticker,
                side=opposite,
                order_type=OrderType.MARKET,
                quantity=fill.quantity,
            )
            state = await self._await_terminal(client_order_id)
            ok = state is not None and state.executed_quantity > 0
        except BcsError as exc:
            ok = False
            logger.critical("[LIVE] Раскрытие ноги %s ПРОВАЛИЛОСЬ: %s", fill.ticker, exc)

        if ok:
            message = f"Нога {fill.ticker} x{fill.quantity} раскрыта по рынку"
            logger.critical("[LIVE] %s", message)
            await log_event("CRITICAL", "strategy", message, self._http)
        else:
            message = (
                f"НЕ УДАЛОСЬ раскрыть ногу {fill.ticker} x{fill.quantity}. "
                "Требуется ручное вмешательство в терминале БКС!"
            )
            logger.critical("[LIVE] %s", message)
            await log_event("CRITICAL", "strategy", message, self._http)

        self.emergency_reason = message
        await self._notify(message)

    async def _notify(self, message: str) -> None:
        if self._notifier is None:
            return
        try:
            await self._notifier.notify_error("strategy", message, self._http)
        except Exception as exc:  # noqa: BLE001
            logger.error("[LIVE] Уведомление не отправлено: %s", exc)

    # ------------------------------------------------------------------
    # Штуки или лоты
    # ------------------------------------------------------------------

    async def _verify_quantity_semantics(self, fill: Fill) -> None:
        """Сверить первое исполнение с портфелем.

        Документация БКС описывает ``orderQuantity`` как «количество активов
        в заявке, шт.», но в портфеле есть отдельное поле ``ratioQuantity``
        («количество в лоте»). Для акций TQBR лот обычно равен 1, и разницы
        нет, но полагаться на «обычно» с реальными деньгами нельзя —
        проверяем фактом при первом же исполнении.
        """
        if self._quantity_semantics_checked:
            return
        self._quantity_semantics_checked = True

        try:
            portfolio = await self._rest.get_portfolio()
        except BcsError as exc:
            logger.warning("[LIVE] Портфель недоступен, сверку объёма пропускаю: %s", exc)
            return

        position = next(
            (
                row
                for row in (portfolio.get("positions") or [])
                if isinstance(row, dict) and row.get("ticker") == fill.ticker
            ),
            None,
        )
        if position is None:
            logger.warning(
                "[LIVE] Позиции по %s нет в портфеле сразу после исполнения — "
                "возможна задержка обновления, сверку объёма пропускаю",
                fill.ticker,
            )
            return

        try:
            ratio = int(float(position.get("ratioQuantity") or 1))
            quantity = abs(float(position.get("quantity") or 0))
        except (TypeError, ValueError):
            return

        instrument = self._instruments.get(fill.ticker)
        lot_size = instrument.lot_size if instrument else ratio

        if lot_size > 1 and abs(quantity - fill.quantity * lot_size) < lot_size:
            message = (
                f"orderQuantity для {fill.ticker} трактуется брокером как ЛОТЫ, "
                f"а не штуки: заявка на {fill.quantity} дала {quantity} шт "
                f"при лоте {lot_size}. Размер позиции завышен в {lot_size} раз!"
            )
            logger.critical("[LIVE] %s", message)
            await log_event("CRITICAL", "strategy", message, self._http)
            await self._notify(message)
            self.emergency_reason = message
        else:
            logger.info(
                "[LIVE] Сверка объёма пройдена: заявка %s шт → в портфеле %s шт (лот %s)",
                fill.quantity, quantity, lot_size,
            )
