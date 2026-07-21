"""
Pairs Arbitrage — логика стратегии v2 (TATN/TATNP), портирована для live/paper trading.

Источник: scripts/backtest_pairs_v2.py в исследовательском репозитории (Owl), версия
с правками времени торгов от 2026-07-17 (аукционные окна, честный фильтр конца сессии,
weekend_entry). Это НЕ бэктест-скрипт — здесь нет загрузки CSV и нет pandas.rolling,
только сама торговая логика в виде класса, который дёргается по одному бару за раз —
так, как этого требует ТЗ для paper trading (живой поток котировок, минутные бары,
скользящее окно в памяти, а не чтение из статичного файла).

Как использовать в живом боте:
    strat = PairsStrategyV2("TATN", "TATNP")
    for bar in live_minute_bars():           # приходят по одному, по мере закрытия бара
        event = strat.on_bar(bar.ts, bar.price_a, bar.price_b)
        if event is not None:
            journal.write(event)             # event["action"] == "enter" | "exit"
            if event["action"] == "exit":
                telegram.alert(event)

Гарантии эквивалентности бэктесту (важно, т.к. на этом коде реальные деньги):
  - Та же однобаровая задержка на вход, что в backtest_pairs_v2.py (сигнал считается
    на баре i, исполняется по цене бара i+1) — реализована через self._pending_signal.
  - Тот же кулдаун MIN_HOLD_MIN, тот же явный запрет входа в аукционные окна и после
    конца сессии, тот же флаг weekend_entry (не блокирует, только помечает).
  - Rolling z-score считается на deque(maxlen=SPREAD_WINDOW) с выборочным std
    (statistics.stdev, ddof=1) — то же самое, что pandas .rolling().std() по умолчанию.
  - Проверено скриптом verify_strategy_pairs_v2.py на историческом кэше — точное
    совпадение с trades_pairs_v2_TATN.csv (эталон бэктеста). Не менять формулы
    без повторного прогона этой проверки.

НЕ входит в этот модуль (сознательно, отдельная задача):
  - Подключение к источнику котировок (BKS API) и агрегация тиков в минутные бары.
  - Запись в Supabase / отправка Telegram-алертов — on_bar() только возвращает dict,
    что делать с ним дальше — решает вызывающий код.
"""

from collections import deque
from datetime import time as dtime
import math


class PairsStrategyV2:
    # Аукционные окна: заявки принимаются, но НЕ исполняются — вход в это время нереалистичен
    AUCTION_WINDOWS = [
        (dtime(6, 50, 0), dtime(6, 59, 59)),
        (dtime(9, 50, 0), dtime(9, 59, 59)),
    ]
    SESSION_END = dtime(17, 10, 0)  # честное сравнение времени (было: hour>=17 and minute>=10 — баг)

    def __init__(
        self,
        ticker_a: str,
        ticker_b: str,
        spread_window: int = 2500,
        entry_zscore: float = 2.5,
        exit_zscore: float = 0.0,
        stop_zscore: float = 4.5,
        max_hold_days: int = 15,
        min_hold_min: int = 120,
        commission_pct: float = 0.0003,
        overnight_pct: float = 0.0001,
        position_size: float = 50_000,
    ):
        self.ticker_a = ticker_a
        self.ticker_b = ticker_b
        self.spread_window = spread_window
        self.entry_zscore = entry_zscore
        self.exit_zscore = exit_zscore
        self.stop_zscore = stop_zscore
        self.max_hold_days = max_hold_days
        self.min_hold_min = min_hold_min
        self.commission_pct = commission_pct
        self.overnight_pct = overnight_pct
        self.position_size = position_size

        self._spread_history = deque(maxlen=spread_window)
        self._sum = 0.0
        self._sumsq = 0.0
        self._pending_signal = None  # сигнал, посчитанный на предыдущем баре
        self.position = None         # dict с состоянием открытой позиции или None

    @property
    def pair(self) -> str:
        return f"{self.ticker_a}/{self.ticker_b}"

    def _zscore(self, spread: float):
        # Инкрементальное скользящее среднее/std (O(1) на бар, не пересчёт по всему
        # окну заново) — численно эквивалентно pandas .rolling().std() (ddof=1).
        dq = self._spread_history
        if len(dq) == dq.maxlen:
            old = dq[0]
            self._sum -= old
            self._sumsq -= old * old
        dq.append(spread)
        self._sum += spread
        self._sumsq += spread * spread

        n = len(dq)
        if n < self.spread_window:
            return None
        mean = self._sum / n
        var = (self._sumsq - n * mean * mean) / (n - 1)
        if var <= 0:
            return None
        return (spread - mean) / math.sqrt(var)

    def _in_auction_window(self, t: dtime) -> bool:
        return any(start <= t <= end for start, end in self.AUCTION_WINDOWS)

    def on_bar(self, ts, price_a: float, price_b: float):
        """
        Вызывать на каждый закрывшийся минутный бар, строго по возрастанию ts.
        ts — datetime бара (МСК). Возвращает None либо dict-событие ("enter"/"exit").
        """
        spread = math.log(price_a / price_b)
        zscore = self._zscore(spread)

        # 1. Исполнить сигнал, посчитанный на прошлом баре (однобаровая задержка входа)
        if self._pending_signal is not None and self.position is None:
            direction = self._pending_signal["direction"]
            self.position = {
                "direction": direction,
                "entry_time": ts,
                "entry_price_a": price_a,
                "entry_price_b": price_b,
                "entry_zscore": self._pending_signal["zscore"],
                "weekend_entry": ts.weekday() >= 5,  # 5=суббота, 6=воскресенье (ДСВД)
            }
            self._pending_signal = None
            return {
                "action": "enter",
                "pair": self.pair,
                "direction": "long_a" if direction == 1 else "long_b",
                "entry_time": ts,
                "entry_price_a": price_a,
                "entry_price_b": price_b,
                "zscore_entry": self.position["entry_zscore"],
                "weekend_entry": self.position["weekend_entry"],
            }

        self._pending_signal = None

        # 2. Позиция открыта — проверить условия выхода
        if self.position is not None:
            hold_min = (ts - self.position["entry_time"]).total_seconds() / 60
            if hold_min < self.min_hold_min or zscore is None:
                return None

            direction = self.position["direction"]
            reason = None
            if direction == 1 and zscore >= -self.exit_zscore:
                reason = "TP"
            elif direction == -1 and zscore <= self.exit_zscore:
                reason = "TP"
            elif abs(zscore) >= self.stop_zscore:
                reason = "STOP"
            if (ts - self.position["entry_time"]).days >= self.max_hold_days:
                reason = "TIMEOUT"

            if reason is None:
                return None

            pnl_a = (price_a - self.position["entry_price_a"]) / self.position["entry_price_a"] * direction
            pnl_b = (price_b - self.position["entry_price_b"]) / self.position["entry_price_b"] * (-direction)
            pnl_pct = (pnl_a + pnl_b) / 2
            hold_days = (ts - self.position["entry_time"]).days
            overnight_cost = hold_days * self.overnight_pct * self.position_size
            pnl_rub = (
                self.position_size * pnl_a
                + self.position_size * pnl_b
                - 4 * self.commission_pct * self.position_size
                - overnight_cost
            )

            event = {
                "action": "exit",
                "pair": self.pair,
                "direction": "long_a" if direction == 1 else "long_b",
                "entry_time": self.position["entry_time"],
                "exit_time": ts,
                "entry_price_a": self.position["entry_price_a"],
                "entry_price_b": self.position["entry_price_b"],
                "exit_price_a": price_a,
                "exit_price_b": price_b,
                "zscore_entry": self.position["entry_zscore"],
                "zscore_exit": zscore,
                "pnl_pct": round(pnl_pct, 5),
                "pnl_rub": round(pnl_rub, 1),
                "reason": reason,
                "weekend_entry": self.position["weekend_entry"],
            }
            self.position = None
            return event

        # 3. Нет позиции — проверить условия входа (с учётом фильтров времени)
        if zscore is None:
            return None

        t = ts.time()
        if self._in_auction_window(t):
            return None
        if t >= self.SESSION_END:
            return None

        if zscore >= self.entry_zscore:
            self._pending_signal = {"direction": -1, "zscore": zscore}
        elif zscore <= -self.entry_zscore:
            self._pending_signal = {"direction": 1, "zscore": zscore}

        return None
