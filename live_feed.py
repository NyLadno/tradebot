"""
Живой источник баров TATN/TATNP для paper trading — котировки БКС → минутные бары
→ PairsStrategyV2. Второй кусок из трёх (см. strategy_pairs_v2.py): дальше нужна
только запись событий в Supabase и Telegram-алерты (следующие задачи).

Исправляет главную проблему черновика bcs_client.py (scripts/bcs_client.py в
исследовательском репо): там refresh_token передавался через sys.argv — небезопасно
для боевого токена. Здесь токен только из переменной окружения BCS_REFRESH_TOKEN.

Требует в .env (по аналогии с существующим tradebot_env_template.txt):
    BCS_REFRESH_TOKEN=...
    BCS_TOKEN_URL=https://int.mybroker.global.bcs/trade-api-keycloak/realms/tradeapi/protocol/openid-connect/token
    BCS_QUOTES_URL=https://int.mybroker.global.bcs/http/market-data/quotes/

ВАЖНО (не проверено на реальном подключении — нет живого BKS-токена в этой среде):
  - Формат ответа /quotes/ здесь предполагается по черновику bcs_client.py
    (поля last/lastPrice/close, ticker/symbol/instrument) — тот черновик сам
    отмечал, что формат не подтверждён на реальном ответе API. Первый живой
    запуск нужно свериться с реальным JSON и поправить парсинг при расхождении.
  - Логика обновления access_token по 401 не тестировалась на реальном сервере —
    механизм истечения токена (сколько живёт access_token) неизвестен, взято
    типовое OAuth-поведение (повторный запрос refresh_token при 401).

Юнит-тесты (test_live_feed.py) гоняются на синтетических тиках, без сети —
это единственная часть, которую можно проверить без реального доступа к БКС.
"""

import os
import time
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

from strategy_pairs_v2 import PairsStrategyV2

logger = logging.getLogger("tradebot.live_feed")
if not logger.hasHandlers():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

MOSCOW_TZ = ZoneInfo("Europe/Moscow")

DEFAULT_TOKEN_URL = "https://int.mybroker.global.bcs/trade-api-keycloak/realms/tradeapi/protocol/openid-connect/token"
DEFAULT_QUOTES_URL = "https://int.mybroker.global.bcs/http/market-data/quotes/"


class BCSQuoteClient:
    """
    Получение котировок из БКС. Токен — только из окружения/явного параметра,
    никогда не через argv (это и было небезопасным местом в черновике).
    """

    def __init__(self, refresh_token: str, tickers, token_url: str = None, quotes_url: str = None):
        if not refresh_token:
            raise ValueError("refresh_token пуст — передавай через BCS_REFRESH_TOKEN, не хардкодь")
        self.refresh_token = refresh_token
        self.token_url = token_url or os.getenv("BCS_TOKEN_URL", DEFAULT_TOKEN_URL)
        self.quotes_url = quotes_url or os.getenv("BCS_QUOTES_URL", DEFAULT_QUOTES_URL)
        self.tickers = list(tickers)
        self._ticker_param = ",".join(f"TQBR:{t}" for t in self.tickers)
        self._access_token = None

    def _refresh_access_token(self) -> str:
        resp = requests.post(
            self.token_url,
            data={
                "grant_type": "refresh_token",
                "client_id": "trade-api-read",
                "refresh_token": self.refresh_token,
            },
            timeout=10,
        )
        resp.raise_for_status()
        self._access_token = resp.json()["access_token"]
        return self._access_token

    def get_quotes(self) -> dict:
        """Возвращает {"TATN": price, "TATNP": price} (может быть неполным, если БКС
        не ответил по одному из тикеров — вызывающий код должен это учитывать)."""
        if self._access_token is None:
            self._refresh_access_token()

        for attempt in range(2):  # одна попытка + один рефреш токена на 401
            resp = requests.get(
                self.quotes_url,
                params={"tickers": self._ticker_param},
                headers={"Authorization": f"Bearer {self._access_token}"},
                timeout=10,
            )
            if resp.status_code == 401 and attempt == 0:
                logger.warning("access_token истёк (401) — обновляю")
                self._refresh_access_token()
                continue
            resp.raise_for_status()
            return self._parse_quotes(resp.json())
        return {}

    def _parse_quotes(self, data) -> dict:
        prices = {}
        items = data if isinstance(data, list) else data.get("quotes", data.get("data", []))
        for item in items:
            ticker = item.get("ticker") or item.get("symbol") or item.get("instrument")
            price = item.get("last") or item.get("lastPrice") or item.get("close")
            if ticker and price is not None:
                short = ticker.split(":")[-1]
                if short in self.tickers:
                    prices[short] = float(price)
        return prices


class MinuteBarAggregator:
    """
    Копит котировки (по одному опросу за раз) в OHLC-бары по минутам для набора
    тикеров. При переходе на новую минуту (по времени опроса) финализирует бары
    предыдущей минуты и возвращает их. Не пишет и не читает файлы — вся память
    в объекте, как того требует ТЗ (не CSV).
    """

    def __init__(self, tickers):
        self.tickers = list(tickers)
        self._minute = None
        self._bars = {t: None for t in self.tickers}

    def add_tick(self, ts: datetime, prices: dict):
        """
        prices: {ticker: price} — срез с одного опроса, может не содержать все тикеры.
        Возвращает список закрытых баров вида (ticker, minute_ts, open, high, low, close),
        если произошёл переход на новую минуту, иначе [].
        """
        minute = ts.replace(second=0, microsecond=0)
        closed = []

        if self._minute is None:
            self._minute = minute
        elif minute > self._minute:
            for t in self.tickers:
                bar = self._bars[t]
                if bar is not None:
                    closed.append((t, self._minute, bar["open"], bar["high"], bar["low"], bar["close"]))
            self._minute = minute
            self._bars = {t: None for t in self.tickers}

        for t, price in prices.items():
            if t not in self.tickers:
                continue
            bar = self._bars.get(t)
            if bar is None:
                self._bars[t] = {"open": price, "high": price, "low": price, "close": price}
            else:
                bar["high"] = max(bar["high"], price)
                bar["low"] = min(bar["low"], price)
                bar["close"] = price

        return closed

    def flush(self):
        """Принудительно закрыть текущий незакрытый бар (например, при остановке бота)."""
        closed = []
        if self._minute is not None:
            for t in self.tickers:
                bar = self._bars[t]
                if bar is not None:
                    closed.append((t, self._minute, bar["open"], bar["high"], bar["low"], bar["close"]))
        self._minute = None
        self._bars = {t: None for t in self.tickers}
        return closed


class SyncedPairFeed:
    """
    Сводит закрытые минутные бары двух тикеров в синхронизированные пары
    (ts, price_a, price_b) — эквивалент inner join, который в backtest_pairs_v2.py
    делает pandas (df_a.join(df_b, how="inner")). Минута, за которую БКС не
    вернул одну из ног (сбой опроса, дыра в данных), тихо не попадает в вывод —
    как и в бэктесте, где такая минута просто выпадает при join.

    Не копит пропущенные минуты бесконечно: если минута не укомплектовалась в
    течение max_wait_minutes после появления первой ноги, она отбрасывается
    (иначе одна зависшая нога блокирует память бота навечно). Отброс логируется —
    это тот самый "пропуск данных", для которого в ТЗ предусмотрен алертинг
    (см. открытый вопрос "порог алерта на пропуск данных", отдельная задача —
    здесь только даётся счётчик through on_missing_ticker).
    """

    def __init__(self, ticker_a: str, ticker_b: str, max_wait_minutes: int = 3, on_missing=None):
        self.ticker_a = ticker_a
        self.ticker_b = ticker_b
        self.max_wait_minutes = max_wait_minutes
        self.on_missing = on_missing  # callback(minute_ts, missing_ticker)
        self._pending = {}  # minute_ts -> {ticker: close_price}

    def add_closed_bars(self, closed_bars):
        """closed_bars: список (ticker, minute_ts, o, h, l, c) от MinuteBarAggregator.
        Возвращает список готовых синхронизированных пар [(minute_ts, price_a, price_b), ...]."""
        for ticker, minute_ts, _o, _h, _l, close in closed_bars:
            if ticker not in (self.ticker_a, self.ticker_b):
                continue
            self._pending.setdefault(minute_ts, {})[ticker] = close

        ready = []
        stale_cutoff_minutes = self.max_wait_minutes
        latest_minute = max(self._pending) if self._pending else None

        for minute_ts in sorted(self._pending):
            entry = self._pending[minute_ts]
            if self.ticker_a in entry and self.ticker_b in entry:
                ready.append((minute_ts, entry[self.ticker_a], entry[self.ticker_b]))

        for minute_ts, _, _ in ready:
            del self._pending[minute_ts]

        if latest_minute is not None:
            for minute_ts in list(self._pending):
                age_min = (latest_minute - minute_ts).total_seconds() / 60
                if age_min > stale_cutoff_minutes:
                    entry = self._pending.pop(minute_ts)
                    missing = self.ticker_b if self.ticker_a in entry else self.ticker_a
                    logger.warning(f"Минута {minute_ts} не укомплектована ({missing} не пришёл) — отброшена")
                    if self.on_missing:
                        self.on_missing(minute_ts, missing)

        return ready


def run_live(refresh_token: str = None, poll_interval_sec: float = 5.0, strategy: PairsStrategyV2 = None):
    """
    Основной живой цикл: опрос БКС → агрегация в минутки → синхронизация пары →
    PairsStrategyV2.on_bar(). Генератор — yield-ит события входа/выхода
    (журналирование в Supabase и Telegram-алерты — следующие задачи, не здесь).
    """
    refresh_token = refresh_token or os.getenv("BCS_REFRESH_TOKEN")
    strategy = strategy or PairsStrategyV2("TATN", "TATNP")

    client = BCSQuoteClient(refresh_token, tickers=[strategy.ticker_a, strategy.ticker_b])
    aggregator = MinuteBarAggregator([strategy.ticker_a, strategy.ticker_b])
    synced = SyncedPairFeed(strategy.ticker_a, strategy.ticker_b)

    logger.info(f"Запуск живого фида {strategy.pair}, опрос раз в {poll_interval_sec}с")

    while True:
        now = datetime.now(MOSCOW_TZ)
        try:
            prices = client.get_quotes()
        except Exception as e:
            logger.error(f"Ошибка получения котировок: {e}")
            prices = {}

        closed_bars = aggregator.add_tick(now, prices)
        for minute_ts, price_a, price_b in synced.add_closed_bars(closed_bars):
            event = strategy.on_bar(minute_ts, price_a, price_b)
            if event is not None:
                yield event

        time.sleep(poll_interval_sec)
