"""Боевой цикл парного арбитража TATN/TATNP.

Собирает вместе всё остальное: бары из БКС → скользящий z-score →
решение о входе/выходе → исполнение → запись в ``trades`` / ``bot_state``
и уведомление в Telegram.

Отличия от бэктестной ``simulate_pairs``, сделанные намеренно:

* время удержания считается через ``total_seconds()``, а не ``.days`` —
  ``.days`` округляет вниз до суток и ломает и MAX_HOLD_DAYS, и overnight;
* цена входа берётся из фактического исполнения (в PAPER — с той стороны
  стакана, по которой мы реально бы купили/продали), а не «close следующей свечи»;
* конец сессии определяется по расписанию инструмента, а не захардкоженным
  ``hour >= 17 and minute >= 10``;
* параметры читаются из БД на каждой итерации, а не из констант модуля.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx

from app.broker.models import Bar, Instrument, OrderBook, Quote, Side, parse_bcs_datetime
from app.broker.rest import BcsRestClient
from app.broker.ws import BcsMarketDataStream
from app.config import settings
from app.logging_setup import get_logger
from app.storage.bot_state import get_bot_state, update_bot_state
from app.storage.candles import get_latest_candles, insert_candles_batch
from app.storage.logs import log_event
from app.storage.quote_gaps import close_quote_gap, open_quote_gap
from app.storage.trades import close_trade, get_open_trades, open_trade
from app.strategy import news_guard, sizing, spread
from app.strategy.execution import (
    ExecutionAdapter,
    ExecutionError,
    LeggingRiskError,
    LegOrder,
    PairFill,
    PaperExecutor,
    QuoteBook,
    build_close_orders,
)
from app.strategy.params import get_params

logger = get_logger("tradebot.strategy.engine")

try:  # Python 3.9+
    from zoneinfo import ZoneInfo

    MSK = ZoneInfo("Europe/Moscow")
except Exception:  # pragma: no cover — на случай отсутствия tzdata
    MSK = timezone(timedelta(hours=3))

POSITION_NONE = "NONE"
LONG_LEG1 = "LONG_TATN_SHORT_TATNP"
SHORT_LEG1 = "SHORT_TATN_LONG_TATNP"

EXIT_TP = "TP"
EXIT_STOP = "STOP"
EXIT_TIMEOUT = "TIMEOUT"
EXIT_NEWS = "NEWS"
EXIT_MANUAL = "MANUAL"

# Причины, которые важнее кулдауна min_hold_min: убыток и новости ждать не будут.
_URGENT_EXITS = (EXIT_STOP, EXIT_NEWS, EXIT_MANUAL)

# Сколько баров на инструмент держим в памяти. Двукратный запас от
# максимального разумного окна (2500), чтобы выравнивание рядов не страдало
# от разной длины истории по ногам.
MAX_BARS_IN_MEMORY = 6000


class PairsEngine:
    """Движок парной торговли. Один экземпляр на процесс."""

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        *,
        rest: Optional[BcsRestClient] = None,
        executor: Optional[ExecutionAdapter] = None,
        notifier: Optional[Any] = None,
    ) -> None:
        self._http = http_client
        self._rest = rest or BcsRestClient(http_client)
        self._notifier = notifier

        self.leg1 = settings.pair_leg1
        self.leg2 = settings.pair_leg2
        self.symbols = [self.leg1, self.leg2]

        self.quotes = QuoteBook()
        self.instruments: Dict[str, Instrument] = {}
        self._bars: Dict[str, Dict[datetime, Bar]] = {s: {} for s in self.symbols}
        self._unsaved: List[Bar] = []
        self._executor = executor
        self._live: Optional[Any] = None
        self.stream: Optional[BcsMarketDataStream] = None

        self._lock = asyncio.Lock()
        self._schedule_cache: Tuple[Optional[str], Optional[datetime]] = (None, None)
        self._open_trade_peak: Optional[float] = None
        self._open_trade_drawdown: float = 0.0
        self._consecutive_errors = 0
        self._gap_id: Optional[int] = None
        self._gap_started_at: Optional[datetime] = None

        self.started = False
        self.last_tick_at: Optional[datetime] = None
        self.last_error: Optional[str] = None
        self.last_zscore: Optional[float] = None

    # ------------------------------------------------------------------
    # Жизненный цикл
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Прогреть окно, поднять потоки и подписаться на данные."""
        settings.validate_bcs()
        params = await get_params(self._http, force=True)
        window = int(params["spread_window"])

        self.instruments = await self._rest.get_instruments(self.symbols)
        self._log_instruments()

        await self._warmup(window)

        if self._executor is None:
            self._executor = await self._build_executor(params)

        self.stream = BcsMarketDataStream(
            self._rest.tokens,  # тот же менеджер токенов, что у REST-клиента
            on_candle=self._on_candle,
            on_quote=self._on_quote,
            on_orderbook=self._on_orderbook,
        )
        self.stream.start(self._http)
        for _ in range(100):
            if self.stream.connected:
                break
            await asyncio.sleep(0.1)
        await self.stream.subscribe_candles(self.symbols)
        await self.stream.subscribe_quotes(self.symbols)

        self.started = True
        message = (
            f"Движок запущен в режиме {self._executor.mode}: пара {self.leg1}/{self.leg2}, "
            f"окно {window}, вход z={params['entry_zscore']}, стоп z={params['stop_zscore']}"
        )
        logger.info("[STRAT] %s", message)
        await log_event("INFO", "strategy", message, self._http)

    async def _build_executor(self, params: Dict[str, Any]) -> ExecutionAdapter:
        """Выбрать режим исполнения.

        LIVE включается только если он явно запрошен И счёт прошёл
        предстартовые проверки. Любая неудача — откат в PAPER с алертом:
        у БКС нет sandbox, и торговать вслепую реальными деньгами нельзя.
        """
        commission_pct = float(params["commission_pct"])
        paper = PaperExecutor(self.quotes, self.instruments, commission_pct=commission_pct)

        if not settings.is_live_trading:
            return paper

        try:
            from app.strategy.live_executor import LiveExecutor

            live = LiveExecutor(
                self._rest,
                self.quotes,
                self.instruments,
                self._http,
                commission_pct=commission_pct,
                notifier=self._notifier,
            )
            report = await live.preflight(self.leg1, self.leg2)
            if not report["ok"]:
                raise RuntimeError("; ".join(report["problems"]))

            # Потоки заявок обязаны быть подняты ДО первой заявки,
            # иначе её исполнение в стрим не попадёт (см. док., стр. 49).
            await live.start()
            self._live = live
            logger.warning(
                "[STRAT] РЕЖИМ LIVE: заявки будут отправляться на реальный счёт"
            )
            await log_event(
                "WARNING", "strategy", "Движок запущен в режиме LIVE", self._http
            )
            return live
        except Exception as exc:  # noqa: BLE001
            message = (
                f"LIVE запросили, но счёт к нему не готов ({exc}). "
                "Работаю в PAPER."
            )
            logger.error("[STRAT] %s", message)
            await log_event("ERROR", "strategy", message, self._http)
            await self._notify_error(message)
            return paper

    async def stop(self) -> None:
        """Остановить потоки. Открытые позиции НЕ закрываются автоматически."""
        self.started = False
        if self.stream is not None:
            await self.stream.stop()
            self.stream = None
        if self._live is not None:
            await self._live.stop()
            self._live = None
        logger.info("[STRAT] Движок остановлен")

    def _log_instruments(self) -> None:
        for symbol in self.symbols:
            instrument = self.instruments.get(symbol)
            if instrument is None:
                logger.error("[STRAT] Инструмент %s не найден в справочнике БКС", symbol)
                continue
            logger.info(
                "[STRAT] %s: лот=%s шаг цены=%s точность=%s шорт=%s маржа=%s борды=%s",
                instrument.ticker,
                instrument.lot_size,
                instrument.minimum_step,
                instrument.scale,
                instrument.is_can_short,
                instrument.is_can_margin,
                ",".join(instrument.class_codes) or "?",
            )

    async def _warmup(self, window: int) -> None:
        """Заполнить окно: сперва из БД, недостающее — из REST БКС."""
        for symbol in self.symbols:
            try:
                rows = await get_latest_candles(symbol, window, self._http)
            except Exception as exc:  # noqa: BLE001
                logger.error("[STRAT] Не удалось прочитать бары %s из БД: %s", symbol, exc)
                rows = []

            for row in rows:
                stamp = parse_bcs_datetime(row.get("timestamp"))
                if stamp is None:
                    continue
                self._bars[symbol][stamp] = Bar(
                    symbol=symbol,
                    time=stamp,
                    open=float(row.get("open") or 0),
                    high=float(row.get("high") or 0),
                    low=float(row.get("low") or 0),
                    close=float(row.get("close") or 0),
                    volume=row.get("volume"),
                )

            have = len(self._bars[symbol])
            if have >= window:
                logger.info("[STRAT] %s: окно прогрето из БД (%s баров)", symbol, have)
                continue

            logger.info(
                "[STRAT] %s: в БД %s баров из %s — догружаю историю из БКС",
                symbol, have, window,
            )
            try:
                bars = await self._rest.get_recent_candles(symbol, window)
            except Exception as exc:  # noqa: BLE001
                logger.error("[STRAT] Догрузка истории %s не удалась: %s", symbol, exc)
                continue
            for bar in bars:
                self._bars[symbol][bar.time] = bar
                self._unsaved.append(bar)
            logger.info("[STRAT] %s: окно после догрузки — %s баров", symbol, len(self._bars[symbol]))

        await self._persist_bars()

    # ------------------------------------------------------------------
    # Колбэки потока
    # ------------------------------------------------------------------

    async def _on_candle(self, bar: Bar) -> None:
        if bar.symbol not in self._bars:
            return
        known = self._bars[bar.symbol]
        # Свеча текущей минуты приходит многократно и уточняется — перезаписываем.
        known[bar.time] = bar
        self._unsaved.append(bar)
        await self._close_gap_if_open()

    async def _on_quote(self, quote: Quote) -> None:
        self.quotes.update(quote)
        await self._close_gap_if_open()

    async def _on_orderbook(self, book: OrderBook) -> None:
        """Стакан пока не используется в решениях — держим для диагностики."""

    # ------------------------------------------------------------------
    # Основной цикл
    # ------------------------------------------------------------------

    async def tick(self) -> None:
        """Один проход принятия решений. Никогда не бросает наружу."""
        if not self.started:
            return
        if self._lock.locked():
            # Тик по бару и тик по расписанию могут наложиться.
            return

        async with self._lock:
            try:
                await self._tick_inner()
                self._consecutive_errors = 0
                self.last_error = None
            except Exception as exc:  # noqa: BLE001 — цикл не должен умирать
                self._consecutive_errors += 1
                self.last_error = str(exc)
                logger.exception("[STRAT] Ошибка в цикле: %s", exc)
                await log_event(
                    "ERROR",
                    "strategy",
                    f"Ошибка торгового цикла: {exc}",
                    self._http,
                    details={"consecutive_errors": self._consecutive_errors},
                )
                await self._notify_error(f"Ошибка торгового цикла: {exc}")
                if self._consecutive_errors >= settings.strategy_max_consecutive_errors:
                    logger.critical(
                        "[STRAT] %s ошибок подряд — аварийная остановка",
                        self._consecutive_errors,
                    )
                    await self._raise_emergency_stop(
                        f"{self._consecutive_errors} ошибок подряд в торговом цикле"
                    )
            finally:
                self.last_tick_at = datetime.now(timezone.utc)

    async def _tick_inner(self) -> None:
        params = await get_params(self._http)
        state = await get_bot_state(self._http)
        if not state:
            raise RuntimeError("bot_state пуста — нет строки id=1")

        await self._persist_bars()
        await self._check_data_gap(params)

        window = int(params["spread_window"])
        stats = self._compute_stats(window)
        patch: Dict[str, Any] = self._market_patch(stats)

        news_patch = await news_guard.sync_bot_state(self._http, state)
        patch.update(news_patch)
        state.update(news_patch)

        if patch:
            await update_bot_state(patch, self._http)

        spread_now, mean, std, zscore = stats
        self.last_zscore = zscore

        open_trade_row = await self._current_trade(state)

        # LiveExecutor выставляет reason при legging risk или расхождении
        # объёмов — это повод остановиться, даже если флаг ещё не в БД.
        if self._live is not None and self._live.emergency_reason:
            await self._raise_emergency_stop(self._live.emergency_reason)
            self._live.emergency_reason = None
            return

        if state.get("emergency_stop_flag"):
            if open_trade_row:
                logger.critical("[STRAT] emergency_stop_flag — закрываю позицию")
                await self._exit(open_trade_row, params, stats, EXIT_MANUAL)
            return

        if zscore is None:
            logger.debug("[STRAT] z-score недоступен (окно не заполнено)")
            return

        if open_trade_row:
            reason = self._exit_reason(open_trade_row, params, zscore, state)
            if reason:
                await self._exit(open_trade_row, params, stats, reason)
            return

        if not state.get("is_running", True):
            return
        if state.get("is_news_blocked"):
            return
        if not await self._can_enter(params):
            return

        entry_z = float(params["entry_zscore"])
        if zscore >= entry_z:
            await self._enter(SHORT_LEG1, params, stats)
        elif zscore <= -entry_z:
            await self._enter(LONG_LEG1, params, stats)

    # ------------------------------------------------------------------
    # Данные
    # ------------------------------------------------------------------

    def _compute_stats(
        self, window: int
    ) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
        """Выровнять ряды и посчитать (spread, mean, std, z)."""
        bars_a = [
            {"timestamp": bar.time, "close": bar.close}
            for bar in self._bars[self.leg1].values()
        ]
        bars_b = [
            {"timestamp": bar.time, "close": bar.close}
            for bar in self._bars[self.leg2].values()
        ]
        _, prices_a, prices_b = spread.align_closes(bars_a, bars_b)
        return spread.current_zscore(prices_a, prices_b, window)

    def _market_patch(
        self, stats: Tuple[Optional[float], ...]
    ) -> Dict[str, Any]:
        """Поля bot_state, отражающие последнее состояние рынка."""
        spread_now, mean, std, zscore = stats
        patch: Dict[str, Any] = {}
        if spread_now is not None:
            patch["last_spread"] = round(spread_now, 8)
        if mean is not None:
            patch["last_spread_mean"] = round(mean, 8)
        if std is not None:
            patch["last_spread_std"] = round(std, 8)
        if zscore is not None:
            patch["last_zscore"] = round(zscore, 4)

        last_bar = self._last_bar_time()
        if last_bar is not None:
            patch["last_bar_time"] = last_bar.isoformat()

        quote_times = [
            q.time
            for q in (self.quotes.get(s) for s in self.symbols)
            if q is not None and q.time is not None
        ]
        if quote_times:
            patch["last_quote_time"] = max(quote_times).isoformat()
        return patch

    def _last_bar_time(self) -> Optional[datetime]:
        stamps = [max(bars) for bars in self._bars.values() if bars]
        return min(stamps) if len(stamps) == len(self.symbols) else None

    async def _persist_bars(self) -> None:
        """Записать накопленные бары в ``candles`` (upsert, дубли безопасны)."""
        if not self._unsaved:
            return
        batch = [bar.to_db_row() for bar in self._unsaved]
        self._unsaved = []
        try:
            await insert_candles_batch(batch, self._http)
        except Exception as exc:  # noqa: BLE001
            logger.error("[STRAT] Не удалось сохранить %s баров: %s", len(batch), exc)

        self._trim_bars()

    def _trim_bars(self) -> None:
        """Не давать окну расти бесконечно: держим двукратный запас."""
        for bars in self._bars.values():
            if len(bars) > MAX_BARS_IN_MEMORY:
                for stamp in sorted(bars)[: len(bars) - MAX_BARS_IN_MEMORY]:
                    del bars[stamp]

    # ------------------------------------------------------------------
    # Разрывы данных
    # ------------------------------------------------------------------

    async def _check_data_gap(self, params: Dict[str, Any]) -> None:
        """Открыть quote_gaps, если данные не идут дольше допустимого."""
        if self.stream is None:
            return
        threshold_sec = float(params.get("data_gap_alert_min") or 10) * 60.0

        ages = [self.stream.seconds_since_event(symbol) for symbol in self.symbols]
        stale = [age for age in ages if age is None or age > threshold_sec]
        if not stale:
            return

        # Разрыв вне торговой сессии — это норма, а не авария.
        if not any(self.quotes.is_tradable(symbol) for symbol in self.symbols):
            return
        if self._gap_id is not None:
            return

        started_at = datetime.now(timezone.utc)
        try:
            gap = await open_quote_gap(
                started_at.isoformat(), self._http, affected_symbols=",".join(self.symbols)
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("[STRAT] Не удалось зафиксировать разрыв данных: %s", exc)
            return

        self._gap_id = gap.get("id")
        self._gap_started_at = started_at
        message = (
            f"Нет данных по {'/'.join(self.symbols)} дольше "
            f"{params.get('data_gap_alert_min')} мин"
        )
        logger.error("[STRAT] %s", message)
        await log_event("ERROR", "quotes", message, self._http)
        await self._notify_data_gap({"started_at": started_at.isoformat(), "message": message})

    async def _close_gap_if_open(self) -> None:
        """Закрыть разрыв и догрузить пропущенные бары из REST."""
        if self._gap_id is None or self._gap_started_at is None:
            return

        ended_at = datetime.now(timezone.utc)
        duration_min = max(1, int((ended_at - self._gap_started_at).total_seconds() // 60))
        gap_id, started_at = self._gap_id, self._gap_started_at
        self._gap_id = None
        self._gap_started_at = None

        try:
            await close_quote_gap(gap_id, ended_at.isoformat(), duration_min, self._http)
        except Exception as exc:  # noqa: BLE001
            logger.error("[STRAT] Не удалось закрыть разрыв #%s: %s", gap_id, exc)

        logger.info("[STRAT] Разрыв данных #%s закрыт (%s мин), догружаю бары", gap_id, duration_min)
        await self._backfill(started_at - timedelta(minutes=2), ended_at)

    async def _backfill(self, start: datetime, end: datetime) -> None:
        """Догрузить бары за период через REST /candles-chart."""
        for symbol in self.symbols:
            try:
                bars = await self._rest.get_candles(symbol, start, end)
            except Exception as exc:  # noqa: BLE001
                logger.error("[STRAT] Догрузка %s за разрыв не удалась: %s", symbol, exc)
                continue
            for bar in bars:
                self._bars[symbol][bar.time] = bar
                self._unsaved.append(bar)
            logger.info("[STRAT] %s: догружено %s баров за разрыв", symbol, len(bars))
        await self._persist_bars()

    # ------------------------------------------------------------------
    # Гейты входа
    # ------------------------------------------------------------------

    async def _can_enter(self, params: Dict[str, Any]) -> bool:
        """Разрешён ли сейчас вход в новую позицию."""
        if not self.quotes.all_tradable(self.symbols):
            statuses = {
                symbol: (self.quotes.get(symbol).trading_status if self.quotes.get(symbol) else None)
                for symbol in self.symbols
            }
            logger.debug("[STRAT] Сессия не готова к торгам: %s", statuses)
            return False

        for symbol in self.symbols:
            instrument = self.instruments.get(symbol)
            if instrument is not None and instrument.is_blocked:
                logger.warning("[STRAT] Инструмент %s заблокирован — вход запрещён", symbol)
                return False

        session_end = await self._session_end()
        if session_end is not None:
            buffer_min = float(params.get("session_end_buffer_min") or 0)
            deadline = session_end - timedelta(minutes=buffer_min)
            if datetime.now(timezone.utc) >= deadline:
                logger.debug(
                    "[STRAT] До конца сессии меньше %s мин — вход запрещён", buffer_min
                )
                return False
        return True

    async def _session_end(self) -> Optional[datetime]:
        """Время окончания последней открытой сессии сегодня (кэш на день)."""
        today = datetime.now(MSK).date().isoformat()
        cached_day, cached_end = self._schedule_cache
        if cached_day == today:
            return cached_end

        try:
            schedule = await self._rest.get_daily_schedule(self.leg1)
        except Exception as exc:  # noqa: BLE001
            logger.warning("[STRAT] Расписание сессий недоступно: %s", exc)
            self._schedule_cache = (today, None)
            return None

        if schedule.get("isWorkingDay") is False:
            self._schedule_cache = (today, None)
            return None

        ends: List[datetime] = []
        for item in schedule.get("dailySchedule") or []:
            if str(item.get("tradingSessionStatus") or "").upper() != "OPEN":
                continue
            end = self._parse_schedule_moment(item.get("endDate"))
            if end is not None:
                ends.append(end)

        session_end = max(ends) if ends else None
        self._schedule_cache = (today, session_end)
        if session_end is not None:
            logger.info("[STRAT] Конец торговой сессии сегодня: %s", session_end.isoformat())
        return session_end

    @staticmethod
    def _parse_schedule_moment(raw: Any) -> Optional[datetime]:
        """Разобрать startDate/endDate расписания.

        Документация обещает ISO 8601, но в примере приводит только время
        (``09:01:00``), поэтому поддерживаем оба варианта.
        """
        if not raw:
            return None
        parsed = parse_bcs_datetime(raw)
        if parsed is not None:
            return parsed
        try:
            hour, minute, *rest = str(raw).strip().split(":")
            second = int(rest[0]) if rest else 0
            today_msk = datetime.now(MSK)
            return today_msk.replace(
                hour=int(hour), minute=int(minute), second=second, microsecond=0
            ).astimezone(timezone.utc)
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Вход
    # ------------------------------------------------------------------

    async def _enter(
        self,
        direction: str,
        params: Dict[str, Any],
        stats: Tuple[Optional[float], ...],
    ) -> None:
        spread_now, mean, std, zscore = stats
        position_size = float(params["position_size"])

        price1 = self._reference_price(self.leg1)
        price2 = self._reference_price(self.leg2)
        if price1 is None or price2 is None:
            logger.warning("[STRAT] Нет опорных цен для входа — пропускаю")
            return

        long_leg1 = direction == LONG_LEG1
        qty1 = sizing.compute_leg_quantity(
            price1, position_size, self._lot(self.leg1)
        )
        qty2 = sizing.compute_leg_quantity(
            price2, position_size, self._lot(self.leg2)
        )
        if qty1 <= 0 or qty2 <= 0:
            logger.warning(
                "[STRAT] Размер позиции %.0f ₽ не покрывает лот (%s=%s, %s=%s)",
                position_size, self.leg1, qty1, self.leg2, qty2,
            )
            return

        short_symbol = self.leg2 if long_leg1 else self.leg1
        if not self._short_allowed(short_symbol):
            return

        leg1 = LegOrder(self.leg1, Side.BUY if long_leg1 else Side.SELL, qty1, price1)
        leg2 = LegOrder(self.leg2, Side.SELL if long_leg1 else Side.BUY, qty2, price2)

        try:
            fills = await self._executor.execute_pair(leg1, leg2)
        except LeggingRiskError as exc:
            await self._raise_emergency_stop(f"Не удалось исполнить обе ноги: {exc}")
            return
        except ExecutionError as exc:
            logger.error("[STRAT] Вход не состоялся: %s", exc)
            await log_event("ERROR", "strategy", f"Вход не состоялся: {exc}", self._http)
            return

        entry_time = fills.time
        try:
            trade = await open_trade(
                direction=direction,
                entry_time=entry_time.isoformat(),
                leg1_entry_price=fills.leg1.price,
                leg2_entry_price=fills.leg2.price,
                spread_entry=round(spread_now, 8) if spread_now is not None else 0.0,
                zscore_entry=round(zscore, 4) if zscore is not None else 0.0,
                client=self._http,
                mode=self._executor.mode,
                leg1_qty=fills.leg1.quantity,
                leg2_qty=fills.leg2.quantity,
                position_size_rub=position_size,
                spread_mean_entry=round(mean, 8) if mean is not None else None,
                spread_std_entry=round(std, 8) if std is not None else None,
            )
        except Exception as exc:  # noqa: BLE001
            logger.critical(
                "[STRAT] Пара исполнена, но запись в trades не удалась: %s. "
                "Позиция открыта вне учёта!", exc,
            )
            await self._raise_emergency_stop(
                f"Сделка исполнена, но не записана в trades: {exc}"
            )
            return

        self._open_trade_peak = 0.0
        self._open_trade_drawdown = 0.0

        await update_bot_state(
            {
                "current_position": direction,
                "current_trade_uuid": trade.get("trade_uuid"),
            },
            self._http,
        )

        entry_commission = fills.total_commission
        message = (
            f"Вход {direction}: {self.leg1} x{fills.leg1.quantity} @{fills.leg1.price:.4f}, "
            f"{self.leg2} x{fills.leg2.quantity} @{fills.leg2.price:.4f}, "
            f"z={zscore:.2f}, комиссия {entry_commission:.2f} ₽"
        )
        logger.info("[STRAT] %s", message)
        await log_event(
            "INFO", "strategy", message, self._http, trade_uuid=trade.get("trade_uuid")
        )
        await self._notify_entry(trade, fills, zscore)

    def _reference_price(self, symbol: str) -> Optional[float]:
        """Опорная цена: последняя котировка, иначе close последнего бара."""
        quote = self.quotes.get(symbol)
        if quote is not None:
            for candidate in (quote.last, quote.mid, quote.close):
                if candidate and candidate > 0:
                    return float(candidate)
        bars = self._bars.get(symbol) or {}
        if bars:
            return bars[max(bars)].close
        return None

    def _lot(self, symbol: str) -> int:
        instrument = self.instruments.get(symbol)
        return instrument.lot_size if instrument else 1

    def _short_allowed(self, symbol: str) -> bool:
        """В LIVE шорт второй ноги обязателен — без него стратегия неисполнима."""
        if self._executor is not None and self._executor.mode != "LIVE":
            return True
        instrument = self.instruments.get(symbol)
        if instrument is None or instrument.is_can_short:
            return True
        logger.error(
            "[STRAT] Шорт по %s недоступен на счёте — вход в LIVE невозможен", symbol
        )
        return False

    # ------------------------------------------------------------------
    # Выход
    # ------------------------------------------------------------------

    def _exit_reason(
        self,
        trade: Dict[str, Any],
        params: Dict[str, Any],
        zscore: float,
        state: Dict[str, Any],
    ) -> Optional[str]:
        """Причина закрытия позиции или None.

        Порядок приоритетов: новости → стоп → таймаут → тейк.
        Кулдаун ``min_hold_min`` уважают только TP и TIMEOUT.
        """
        entry_time = parse_bcs_datetime(trade.get("entry_time"))
        if entry_time is None:
            logger.error("[STRAT] У сделки %s нет entry_time", trade.get("trade_uuid"))
            return None

        held_sec = (datetime.now(timezone.utc) - entry_time).total_seconds()
        self._track_drawdown(trade, zscore)

        reason: Optional[str] = None
        if state.get("is_news_blocked"):
            reason = EXIT_NEWS
        elif abs(zscore) >= float(params["stop_zscore"]):
            reason = EXIT_STOP
        elif held_sec >= float(params["max_hold_days"]) * 86400.0:
            reason = EXIT_TIMEOUT
        else:
            exit_z = float(params["exit_zscore"])
            direction = str(trade.get("direction") or "")
            if direction == LONG_LEG1 and zscore >= -exit_z:
                reason = EXIT_TP
            elif direction == SHORT_LEG1 and zscore <= exit_z:
                reason = EXIT_TP

        if reason is None:
            return None

        if reason not in _URGENT_EXITS:
            min_hold_sec = float(params["min_hold_min"]) * 60.0
            if held_sec < min_hold_sec:
                logger.debug(
                    "[STRAT] Причина %s есть, но удержано %.0f мин из %.0f — жду",
                    reason, held_sec / 60.0, min_hold_sec / 60.0,
                )
                return None
        return reason

    def _track_drawdown(self, trade: Dict[str, Any], zscore: float) -> None:
        """Отслеживать максимальную просадку открытой позиции в рублях."""
        price1 = self._reference_price(self.leg1)
        price2 = self._reference_price(self.leg2)
        if price1 is None or price2 is None:
            return
        gross = self._gross_pnl(trade, price1, price2)
        if self._open_trade_peak is None or gross > self._open_trade_peak:
            self._open_trade_peak = gross
        drawdown = (self._open_trade_peak or 0.0) - gross
        if drawdown > self._open_trade_drawdown:
            self._open_trade_drawdown = drawdown

    @staticmethod
    def _gross_pnl(trade: Dict[str, Any], price1: float, price2: float) -> float:
        """P&L по обеим ногам без комиссий, по текущим ценам."""
        direction = str(trade.get("direction") or "")
        long_leg1 = direction == LONG_LEG1
        qty1 = float(trade.get("leg1_qty") or 0)
        qty2 = float(trade.get("leg2_qty") or 0)
        entry1 = float(trade.get("leg1_entry_price") or 0)
        entry2 = float(trade.get("leg2_entry_price") or 0)

        sign1 = 1.0 if long_leg1 else -1.0
        sign2 = -sign1
        return sign1 * qty1 * (price1 - entry1) + sign2 * qty2 * (price2 - entry2)

    async def _exit(
        self,
        trade: Dict[str, Any],
        params: Dict[str, Any],
        stats: Tuple[Optional[float], ...],
        reason: str,
    ) -> None:
        spread_now, _mean, _std, zscore = stats

        price1 = self._reference_price(self.leg1)
        price2 = self._reference_price(self.leg2)
        if price1 is None or price2 is None:
            logger.error("[STRAT] Нет цен для закрытия позиции — повторю на следующем тике")
            return

        leg1, leg2 = build_close_orders(trade, leg1_price=price1, leg2_price=price2)
        try:
            fills = await self._executor.close_pair(leg1, leg2)
        except LeggingRiskError as exc:
            await self._raise_emergency_stop(f"Не удалось закрыть обе ноги: {exc}")
            return
        except ExecutionError as exc:
            logger.error("[STRAT] Закрытие не состоялось: %s", exc)
            await log_event("ERROR", "strategy", f"Закрытие не состоялось: {exc}", self._http)
            return

        patch = self._build_close_patch(trade, params, fills, spread_now, zscore, reason)
        trade_uuid = str(trade.get("trade_uuid"))

        try:
            await close_trade(trade_uuid, patch, self._http)
        except Exception as exc:  # noqa: BLE001
            logger.critical(
                "[STRAT] Позиция закрыта, но запись в trades не удалась: %s", exc
            )
            await self._raise_emergency_stop(f"Закрытие не записано в trades: {exc}")
            return

        await self._update_state_after_exit(patch)

        message = (
            f"Выход {reason}: {trade.get('direction')} → "
            f"{self.leg1} @{fills.leg1.price:.4f}, {self.leg2} @{fills.leg2.price:.4f}, "
            f"z={zscore:.2f}, P&L {patch['net_pnl_rub']:+.2f} ₽ "
            f"(удержано {patch['hold_time_min']} мин)"
        )
        logger.info("[STRAT] %s", message)
        await log_event("INFO", "strategy", message, self._http, trade_uuid=trade_uuid)
        await self._notify_exit({**trade, **patch}, fills, zscore)

        self._open_trade_peak = None
        self._open_trade_drawdown = 0.0

    def _build_close_patch(
        self,
        trade: Dict[str, Any],
        params: Dict[str, Any],
        fills: PairFill,
        spread_now: Optional[float],
        zscore: Optional[float],
        reason: str,
    ) -> Dict[str, Any]:
        """Собрать патч закрытия со всеми расчётами P&L."""
        entry_time = parse_bcs_datetime(trade.get("entry_time")) or fills.time
        exit_time = fills.time
        held = exit_time - entry_time

        # Через total_seconds(), а не .days: .days округляет вниз до суток
        # и превращает 23 часа удержания в ноль дней.
        held_sec = max(0.0, held.total_seconds())
        hold_time_min = int(held_sec // 60)
        hold_days = int(held_sec // 86400)

        direction = str(trade.get("direction") or "")
        long_leg1 = direction == LONG_LEG1
        qty1 = float(trade.get("leg1_qty") or 0)
        qty2 = float(trade.get("leg2_qty") or 0)
        entry1 = float(trade.get("leg1_entry_price") or 0)
        entry2 = float(trade.get("leg2_entry_price") or 0)

        sign1 = 1.0 if long_leg1 else -1.0
        pnl_leg1 = sign1 * qty1 * (fills.leg1.price - entry1)
        pnl_leg2 = -sign1 * qty2 * (fills.leg2.price - entry2)
        gross = pnl_leg1 + pnl_leg2

        # Комиссия за все четыре сделки: две на входе, две на выходе.
        commission_pct = float(params["commission_pct"])
        entry_commission = (
            sizing.estimate_commission(int(qty1), entry1, commission_pct)
            + sizing.estimate_commission(int(qty2), entry2, commission_pct)
        )
        commission = entry_commission + fills.total_commission

        nights = self._overnight_nights(entry_time, exit_time)
        overnight = nights * float(params["overnight_pct"]) * float(
            trade.get("position_size_rub") or params["position_size"]
        )

        net = gross - commission - overnight

        return {
            "exit_time": exit_time.isoformat(),
            "leg1_exit_price": round(fills.leg1.price, 6),
            "leg2_exit_price": round(fills.leg2.price, 6),
            "spread_exit": round(spread_now, 8) if spread_now is not None else None,
            "zscore_exit": round(zscore, 4) if zscore is not None else None,
            "pnl_leg1_rub": round(pnl_leg1, 2),
            "pnl_leg2_rub": round(pnl_leg2, 2),
            "gross_pnl_rub": round(gross, 2),
            "commission_rub": round(commission, 2),
            "overnight_fees_rub": round(overnight, 2),
            "net_pnl_rub": round(net, 2),
            "exit_reason": reason,
            "hold_time_min": hold_time_min,
            "hold_days": hold_days,
            "max_drawdown_rub": round(self._open_trade_drawdown, 2),
        }

    @staticmethod
    def _overnight_nights(entry_time: datetime, exit_time: datetime) -> int:
        """Число фактически пережитых ночей (переходов через полночь MSK).

        Бэктест считал это как ``.days`` разницы, что даёт ноль для позиции,
        открытой в 18:00 и закрытой в 11:00 следующего дня, хотя перенос
        через ночь был и деньги за него списываются.
        """
        entry_date = entry_time.astimezone(MSK).date()
        exit_date = exit_time.astimezone(MSK).date()
        return max(0, (exit_date - entry_date).days)

    async def _update_state_after_exit(self, patch: Dict[str, Any]) -> None:
        """Обновить агрегаты в bot_state после закрытия сделки."""
        state = await get_bot_state(self._http)
        net = float(patch.get("net_pnl_rub") or 0.0)
        total_trades = int(state.get("total_trades_count") or 0) + 1
        total_pnl = float(state.get("total_net_pnl_rub") or 0.0) + net
        balance = float(state.get("virtual_balance") or 0.0) + net

        await update_bot_state(
            {
                "current_position": POSITION_NONE,
                "current_trade_uuid": None,
                "total_trades_count": total_trades,
                "total_net_pnl_rub": round(total_pnl, 2),
                "virtual_balance": round(balance, 2),
            },
            self._http,
        )

    async def _current_trade(self, state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Открытая сделка из БД (источник истины — таблица trades)."""
        try:
            open_trades = await get_open_trades(self._http)
        except Exception as exc:  # noqa: BLE001
            logger.error("[STRAT] Не удалось прочитать открытые сделки: %s", exc)
            return None

        if not open_trades:
            if state.get("current_position") not in (None, POSITION_NONE):
                logger.warning(
                    "[STRAT] bot_state говорит о позиции %s, но открытых сделок нет — сбрасываю",
                    state.get("current_position"),
                )
                await update_bot_state(
                    {"current_position": POSITION_NONE, "current_trade_uuid": None},
                    self._http,
                )
            return None

        if len(open_trades) > 1:
            logger.error(
                "[STRAT] Открытых сделок больше одной (%s) — беру самую раннюю",
                len(open_trades),
            )
            open_trades.sort(key=lambda row: str(row.get("entry_time") or ""))
        return open_trades[0]

    # ------------------------------------------------------------------
    # Аварии и уведомления
    # ------------------------------------------------------------------

    async def _raise_emergency_stop(self, reason: str) -> None:
        """Выставить аварийный флаг: новые входы прекращаются немедленно."""
        logger.critical("[STRAT] АВАРИЙНАЯ ОСТАНОВКА: %s", reason)
        try:
            await update_bot_state({"emergency_stop_flag": True}, self._http)
        except Exception as exc:  # noqa: BLE001
            logger.critical("[STRAT] Не удалось выставить emergency_stop_flag: %s", exc)
        await log_event("CRITICAL", "strategy", f"Аварийная остановка: {reason}", self._http)
        await self._notify_error(f"АВАРИЙНАЯ ОСТАНОВКА: {reason}")

    async def _notify_entry(self, trade: Dict[str, Any], fills: PairFill, zscore: float) -> None:
        if self._notifier is None:
            return
        try:
            await self._notifier.notify_trade_entry(
                {**trade, "zscore_entry": zscore, "mode": self._executor.mode},
                self._http,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error("[STRAT] Уведомление о входе не отправлено: %s", exc)

    async def _notify_exit(self, trade: Dict[str, Any], fills: PairFill, zscore: float) -> None:
        if self._notifier is None:
            return
        try:
            await self._notifier.notify_trade_exit(trade, self._http)
        except Exception as exc:  # noqa: BLE001
            logger.error("[STRAT] Уведомление о выходе не отправлено: %s", exc)

    async def _notify_data_gap(self, gap: Dict[str, Any]) -> None:
        if self._notifier is None:
            return
        try:
            await self._notifier.notify_data_gap(gap, self._http)
        except Exception as exc:  # noqa: BLE001
            logger.error("[STRAT] Уведомление о разрыве не отправлено: %s", exc)

    async def _notify_error(self, message: str) -> None:
        if self._notifier is None:
            return
        try:
            await self._notifier.notify_error("strategy", message, self._http)
        except Exception as exc:  # noqa: BLE001
            logger.error("[STRAT] Уведомление об ошибке не отправлено: %s", exc)

    # ------------------------------------------------------------------
    # Состояние для эндпоинтов
    # ------------------------------------------------------------------

    def status(self) -> Dict[str, Any]:
        """Снимок состояния движка для /health и /strategy/state."""
        stream = self.stream
        return {
            "started": self.started,
            "mode": self._executor.mode if self._executor else None,
            "pair": f"{self.leg1}/{self.leg2}",
            "ws_connected": bool(stream and stream.connected),
            "ws_connect_attempts": stream.connect_attempts if stream else 0,
            "bars_in_window": {s: len(b) for s, b in self._bars.items()},
            "last_bar_time": (
                self._last_bar_time().isoformat() if self._last_bar_time() else None
            ),
            "last_zscore": self.last_zscore,
            "last_tick_at": self.last_tick_at.isoformat() if self.last_tick_at else None,
            "last_error": self.last_error,
            "consecutive_errors": self._consecutive_errors,
            "data_gap_open": self._gap_id is not None,
            "quotes": self.quotes.snapshot(),
            "instruments": {
                symbol: {
                    "lot_size": inst.lot_size,
                    "minimum_step": inst.minimum_step,
                    "is_can_short": inst.is_can_short,
                    "is_blocked": inst.is_blocked,
                }
                for symbol, inst in self.instruments.items()
            },
        }
