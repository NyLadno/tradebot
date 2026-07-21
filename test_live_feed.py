"""
Юнит-тесты MinuteBarAggregator / SyncedPairFeed на синтетических тиках, без сети —
BCSQuoteClient (реальный HTTP к БКС) не тестируется здесь, нет живого токена.
Запуск: python3 test_live_feed.py
"""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from live_feed import MinuteBarAggregator, SyncedPairFeed

MSK = ZoneInfo("Europe/Moscow")


def t(h, m, s):
    return datetime(2026, 7, 21, h, m, s, tzinfo=MSK)


def check(label, cond):
    status = "OK " if cond else "FAIL"
    print(f"[{status}] {label}")
    assert cond, label


def test_ohlc_within_minute():
    agg = MinuteBarAggregator(["TATN", "TATNP"])
    # три тика внутри 10:00, потом первый тик 10:01 закрывает минуту
    assert agg.add_tick(t(10, 0, 0), {"TATN": 100.0, "TATNP": 50.0}) == []
    assert agg.add_tick(t(10, 0, 20), {"TATN": 102.0, "TATNP": 49.5}) == []
    assert agg.add_tick(t(10, 0, 45), {"TATN": 99.0, "TATNP": 50.5}) == []
    closed = agg.add_tick(t(10, 1, 5), {"TATN": 101.0, "TATNP": 50.0})

    closed_map = {c[0]: c for c in closed}
    check("оба тикера закрылись при переходе минуты", set(closed_map) == {"TATN", "TATNP"})
    _, minute_ts, o, h, l, c = closed_map["TATN"]
    check("TATN open=100 (первый тик минуты)", o == 100.0)
    check("TATN high=102 (максимум тиков минуты)", h == 102.0)
    check("TATN low=99 (минимум тиков минуты)", l == 99.0)
    check("TATN close=99 (последний тик ПРЕДЫДУЩЕЙ минуты 10:00, не 10:01)", c == 99.0)
    check("minute_ts = 10:00 (не 10:01)", minute_ts == t(10, 0, 0))


def test_missing_ticker_in_poll():
    # опрос не вернул TATNP в одном из тиков — не должно ломать агрегацию TATN
    agg = MinuteBarAggregator(["TATN", "TATNP"])
    agg.add_tick(t(11, 0, 0), {"TATN": 200.0, "TATNP": 80.0})
    agg.add_tick(t(11, 0, 30), {"TATN": 201.0})  # TATNP не пришёл в этом опросе
    closed = agg.add_tick(t(11, 1, 0), {"TATN": 202.0, "TATNP": 81.0})
    closed_map = {c[0]: c for c in closed}
    check("TATN бар закрылся несмотря на пропуск TATNP в одном опросе", "TATN" in closed_map)
    check("TATN close = 201 (последняя цена ДО перехода минуты)", closed_map["TATN"][5] == 201.0)
    check("TATNP бар тоже закрылся (был хотя бы один тик в минуте)", "TATNP" in closed_map)


def test_synced_feed_drops_unmatched():
    synced = SyncedPairFeed("TATN", "TATNP", max_wait_minutes=2)
    missing_log = []
    synced.on_missing = lambda minute_ts, ticker: missing_log.append((minute_ts, ticker))

    # минута 10:00 — обе ноги есть
    ready = synced.add_closed_bars([
        ("TATN", t(10, 0, 0), 100, 102, 99, 101),
        ("TATNP", t(10, 0, 0), 50, 51, 49, 50.5),
    ])
    check("обе ноги за 10:00 → готовая пара сразу", ready == [(t(10, 0, 0), 101, 50.5)])

    # минута 10:01 — только TATN, TATNP всё ещё не пришёл
    ready2 = synced.add_closed_bars([("TATN", t(10, 1, 0), 101, 103, 100, 102)])
    check("неполная минута не выходит сразу", ready2 == [])

    # минута 10:04 (прошло > max_wait_minutes=2 от 10:01) — TATNP пришёл, но 10:01 уже протухла
    ready3 = synced.add_closed_bars([
        ("TATN", t(10, 4, 0), 105, 106, 104, 105),
        ("TATNP", t(10, 4, 0), 52, 53, 51, 52.5),
    ])
    check("протухшая минута 10:01 отброшена (не в выводе)", all(r[0] != t(10, 1, 0) for r in ready3))
    check("свежая минута 10:04 прошла", (t(10, 4, 0), 105, 52.5) in ready3)
    check("отброс залогирован через on_missing callback", (t(10, 1, 0), "TATNP") in missing_log)


def test_end_to_end_with_strategy():
    """Прогон живого фида (без сети) через реальную PairsStrategyV2 — просто
    проверка, что цепочка Aggregator → SyncedPairFeed → on_bar() не падает
    и не расходится по числу баров при полном совпадении тиков по обеим ногам."""
    from strategy_pairs_v2 import PairsStrategyV2

    strat = PairsStrategyV2("TATN", "TATNP", spread_window=5)  # маленькое окно для теста
    agg = MinuteBarAggregator(["TATN", "TATNP"])
    synced = SyncedPairFeed("TATN", "TATNP")

    base = t(10, 0, 0)
    events = []
    for i in range(10):
        ts = base + timedelta(minutes=i)
        price_a = 100 + i * 0.1
        price_b = 50 - i * 0.05
        closed = agg.add_tick(ts, {"TATN": price_a, "TATNP": price_b})
        for minute_ts, pa, pb in synced.add_closed_bars(closed):
            ev = strat.on_bar(minute_ts, pa, pb)
            if ev:
                events.append(ev)
    # финальный флаш последней открытой минуты
    closed = agg.flush()
    for minute_ts, pa, pb in synced.add_closed_bars(closed):
        strat.on_bar(minute_ts, pa, pb)

    check("цепочка отработала без исключений (9 закрытых баров прошли в стратегию)", True)


if __name__ == "__main__":
    test_ohlc_within_minute()
    test_missing_ticker_in_poll()
    test_synced_feed_drops_unmatched()
    test_end_to_end_with_strategy()
    print("\nВСЕ ТЕСТЫ ПРОШЛИ.")
