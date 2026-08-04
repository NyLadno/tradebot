"""FastAPI application, scheduled news fetchers and the trading engine."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Header, HTTPException, Query

from app.config import GOOGLE_JOBS, NEWSAPI_JOBS, settings
from app.http_client import close_http_client, get_http_client
from app.logging_setup import get_logger
from app.sources.google import fetch_google_news
from app.sources.newsapi import fetch_newsapi
from app.sources.russian_rss import fetch_russian_rss
from app.storage.bot_state import get_bot_state, update_bot_state
from app.storage.heartbeat import record_heartbeat
from app.storage.logs import log_event
from app.storage.quote_gaps import get_unresolved_gaps
from app.storage.trades import get_open_trades, get_trades_closed_since

logger = get_logger("tradebot.main")

scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

# Движок создаётся в lifespan, если ENABLE_TRADING_ENGINE=true.
engine: Optional[Any] = None


def _add_windowed_job(func, args: list, minute_interval: str) -> None:
    """
    Schedule a job to run every ``minute_interval`` within 09:00–22:00 MSK.

    APScheduler's CronTrigger uses inclusive ranges, so we use two triggers:
    one for 09:00–21:55 and a single 22:00 fire to respect the exact window.
    """
    scheduler.add_job(func, CronTrigger(minute=minute_interval, hour="9-21"), args=args)
    scheduler.add_job(func, CronTrigger(minute="0", hour="22"), args=args)


async def _scheduled_google_fetch(
    query: str, hl: str, gl: str, ceid: str
) -> None:
    """Scheduler wrapper that always uses the current shared HTTP client."""
    await fetch_google_news(query, hl, gl, ceid, get_http_client())


async def _scheduled_newsapi_fetch(query: str, lang: str) -> None:
    """Scheduler wrapper that always uses the current shared HTTP client."""
    await fetch_newsapi(query, lang, get_http_client())


async def _scheduled_russian_rss_fetch() -> None:
    """Scheduler wrapper that always uses the current shared HTTP client."""
    await fetch_russian_rss(get_http_client())


async def _scheduled_heartbeat() -> None:
    """Write a periodic health snapshot to the heartbeat table."""
    client = get_http_client()
    try:
        await record_heartbeat(client, **await _health_metrics(client))
    except Exception as exc:
        logger.error("[Heartbeat] Не удалось записать heartbeat: %s", exc)


async def _scheduled_engine_tick() -> None:
    """Watchdog-тик стратегии.

    Основной триггер решений — приход бара из WebSocket; этот джоб нужен
    на случай, когда поток молчит (нерабочие часы, разрыв связи), чтобы
    таймауты и новостные блокировки всё равно обрабатывались.
    """
    if engine is None:
        return
    try:
        await engine.tick()
    except Exception as exc:  # noqa: BLE001 — движок глушит свои ошибки сам
        logger.error("[STRAT] Watchdog-тик упал: %s", exc)


async def _health_metrics(client) -> Dict[str, Any]:
    """Собрать показатели для heartbeat и /health."""
    metrics: Dict[str, Any] = {"status": "OK"}
    problems: List[str] = []

    try:
        open_trades = await get_open_trades(client)
        metrics["open_trade_count"] = len(open_trades)
    except Exception as exc:  # noqa: BLE001
        problems.append(f"trades: {exc}")

    try:
        state = await get_bot_state(client)
        if state:
            metrics["virtual_equity"] = float(state.get("virtual_balance") or 0.0)
            if state.get("emergency_stop_flag"):
                problems.append("выставлен emergency_stop_flag")
    except Exception as exc:  # noqa: BLE001
        problems.append(f"bot_state: {exc}")

    try:
        midnight_msk = datetime.now(timezone(timedelta(hours=3))).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        closed = await get_trades_closed_since(midnight_msk.isoformat(), client)
        metrics["today_pnl_rub"] = round(
            sum(float(row.get("net_pnl_rub") or 0.0) for row in closed), 2
        )
    except Exception as exc:  # noqa: BLE001
        problems.append(f"дневной P&L: {exc}")

    if engine is not None and engine.stream is not None:
        ages = [
            engine.stream.seconds_since_event(symbol) for symbol in engine.symbols
        ]
        known = [age for age in ages if age is not None]
        if known:
            metrics["last_quote_age_sec"] = int(max(known))
        if not engine.stream.connected:
            problems.append("WebSocket БКС не подключён")
        if engine.last_error:
            problems.append(f"движок: {engine.last_error}")

    try:
        gaps = await get_unresolved_gaps(client)
        if gaps:
            problems.append(f"незакрытых разрывов данных: {len(gaps)}")
    except Exception as exc:  # noqa: BLE001
        problems.append(f"quote_gaps: {exc}")

    if problems:
        metrics["status"] = "ERROR" if len(problems) > 1 else "WARNING"
        metrics["details"] = {"problems": problems}
    return metrics


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app lifecycle: start HTTP client, scheduler; shutdown cleanly."""
    client = get_http_client()

    for query, hl, gl, ceid in GOOGLE_JOBS:
        _add_windowed_job(
            _scheduled_google_fetch,
            args=[query, hl, gl, ceid],
            minute_interval="*/5",
        )

    for query, lang in NEWSAPI_JOBS:
        _add_windowed_job(
            _scheduled_newsapi_fetch,
            args=[query, lang],
            minute_interval="0,24,48",
        )

    _add_windowed_job(
        _scheduled_russian_rss_fetch,
        args=[],
        minute_interval="*/5",
    )

    scheduler.add_job(_scheduled_heartbeat, CronTrigger(minute="*/10"), args=[])

    await _start_engine(client)

    scheduler.start()
    logger.info(
        "Планировщик запущен (MSK 09:00–22:00). "
        "Google/RSS: каждые 5 мин. NewsAPI: 0/24/48 мин. Heartbeat: каждые 10 мин."
    )
    await log_event("INFO", "system", "Планировщик запущен", client)

    yield

    scheduler.shutdown()
    logger.info("Планировщик остановлен.")
    if engine is not None:
        await engine.stop()
    await log_event("INFO", "system", "Планировщик остановлен", client)
    await close_http_client()
    logger.info("HTTP-клиент закрыт.")


async def _start_engine(client) -> None:
    """Поднять торговый движок, если он включён и настроен.

    Сбой запуска движка не должен ронять новостной пайплайн: приложение
    продолжает работать, а причина пишется в логи и в таблицу logs.
    """
    global engine
    if not settings.enable_trading_engine:
        logger.info("[STRAT] Торговый движок отключён (ENABLE_TRADING_ENGINE=false)")
        return

    try:
        from app import telegram_bot
        from app.strategy.engine import PairsEngine

        engine = PairsEngine(client, notifier=telegram_bot)
        await engine.start()
        # Тик раз в минуту как подстраховка к событиям из WebSocket.
        scheduler.add_job(_scheduled_engine_tick, CronTrigger(minute="*"), args=[])
    except Exception as exc:  # noqa: BLE001 — движок не должен ронять приложение
        engine = None
        logger.error("[STRAT] Не удалось запустить торговый движок: %s", exc)
        await log_event(
            "ERROR", "system", f"Торговый движок не запущен: {exc}", client
        )


app = FastAPI(lifespan=lifespan)


async def _get_and_save_news(
    query: str,
    lang: str,
    hl: str,
    gl: str,
    ceid: str,
) -> Dict[str, Any]:
    """Fetch Google RSS and NewsAPI in parallel and persist new articles."""
    import asyncio

    client = get_http_client()
    google_items, newsapi_articles = await asyncio.gather(
        fetch_google_news(query, hl, gl, ceid, client),
        fetch_newsapi(query, lang, client),
    )
    return {
        "google": google_items,
        "newsapi": {
            "status": "ok",
            "totalResults": len(newsapi_articles),
            "articles": newsapi_articles,
        },
    }


@app.get("/politics_government_ru")
async def get_news_ru() -> Dict[str, Any]:
    """Fetch Russian-language Tatneft news."""
    return await _get_and_save_news("Татнефть", "ru", "ru", "RU", "RU:ru")


@app.get("/politics_government_en")
async def get_news_en() -> Dict[str, Any]:
    """Fetch English-language Tatneft news."""
    return await _get_and_save_news("Tatneft", "en", "en-US", "US", "US:en")


@app.get("/business_tatneft_de")
async def get_news_de() -> Dict[str, Any]:
    """Fetch German-language Tatneft news."""
    return await _get_and_save_news("Tatneft", "de", "de", "DE", "DE:de")


@app.get("/news")
async def get_news_dynamic(
    query: str = Query(..., description="Ключевое слово для поиска"),
    lang: str = Query("ru", description="Язык новостей (ru, en, de, etc.)"),
    country: str = Query("RU", description="Двухсимвольный код страны (RU, US, DE, etc.)"),
) -> Dict[str, Any]:
    """Dynamic endpoint to search and save arbitrary news."""
    hl = f"{lang}-{country}" if lang == "en" and country == "US" else lang
    gl = country
    ceid = f"{country}:{lang}"
    return await _get_and_save_news(query, lang, hl, gl, ceid)


@app.get("/fetch_russian_rss")
async def get_russian_rss() -> Dict[str, Any]:
    """Manually trigger the Russian RSS fetch."""
    client = get_http_client()
    items = await fetch_russian_rss(client)
    return {
        "status": "ok",
        "total_parsed": len(items),
        "articles": items,
    }


# ---------------------------------------------------------------------------
# Торговый контур
# ---------------------------------------------------------------------------


def _require_admin(token: Optional[str]) -> None:
    """Проверить секрет для эндпоинтов, меняющих торговое состояние.

    У приложения нет общей авторизации, а эти ручки останавливают и
    запускают торговлю, поэтому закрываем их отдельным секретом.
    """
    if not settings.bot_admin_token:
        raise HTTPException(
            status_code=503,
            detail="BOT_ADMIN_TOKEN не задан — управляющие эндпоинты отключены",
        )
    if token != settings.bot_admin_token:
        raise HTTPException(status_code=401, detail="Неверный X-Bot-Token")


@app.get("/health")
async def health() -> Dict[str, Any]:
    """Состояние приложения, потока котировок и торгового движка."""
    client = get_http_client()
    metrics = await _health_metrics(client)
    return {
        "status": metrics.get("status", "OK"),
        "scheduler_running": scheduler.running,
        "engine": engine.status() if engine is not None else {"started": False},
        "metrics": metrics,
    }


@app.get("/strategy/state")
async def strategy_state() -> Dict[str, Any]:
    """Снимок bot_state, активных параметров и текущего z-score."""
    client = get_http_client()
    from app.strategy.params import get_cache, get_params

    try:
        params = await get_params(client)
    except Exception as exc:  # noqa: BLE001
        params = {"error": str(exc)}

    return {
        "bot_state": await get_bot_state(client),
        "params": params,
        "params_error": get_cache().last_error,
        "engine": engine.status() if engine is not None else {"started": False},
    }


@app.post("/strategy/stop")
async def strategy_stop(
    emergency: bool = Query(False, description="Аварийная остановка: закрыть позицию"),
    x_bot_token: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """Остановить торговлю.

    Без ``emergency`` просто запрещает новые входы (``is_running=false``);
    с ``emergency=true`` выставляет ``emergency_stop_flag``, по которому
    движок на ближайшем тике закроет открытую позицию.
    """
    _require_admin(x_bot_token)
    client = get_http_client()
    patch: Dict[str, Any] = {"is_running": False}
    if emergency:
        patch["emergency_stop_flag"] = True
    await update_bot_state(patch, client)
    await log_event(
        "WARNING",
        "system",
        f"Торговля остановлена через API (emergency={emergency})",
        client,
    )
    return {"status": "ok", "applied": patch}


@app.post("/strategy/start")
async def strategy_start(
    x_bot_token: Optional[str] = Header(None),
) -> Dict[str, Any]:
    """Возобновить торговлю и снять аварийный флаг."""
    _require_admin(x_bot_token)
    client = get_http_client()
    patch = {"is_running": True, "emergency_stop_flag": False}
    await update_bot_state(patch, client)
    await log_event("INFO", "system", "Торговля возобновлена через API", client)
    return {"status": "ok", "applied": patch}


@app.get("/bcs/selftest")
async def bcs_selftest() -> Dict[str, Any]:
    """Проверка связности с БКС. Только чтение — заявки не отправляются."""
    from app.broker.rest import BcsRestClient

    client = get_http_client()
    result: Dict[str, Any] = {"status": "ok", "checks": {}}

    try:
        settings.validate_bcs()
    except ValueError as exc:
        return {"status": "error", "detail": str(exc)}

    rest = BcsRestClient(client)
    symbols = settings.pair_symbols

    async def _check(name: str, coro):
        try:
            result["checks"][name] = await coro
        except Exception as exc:  # noqa: BLE001
            result["status"] = "error"
            result["checks"][name] = {"error": f"{type(exc).__name__}: {exc}"}

    async def _auth() -> Dict[str, Any]:
        token = await rest.tokens.get_access_token(client)
        return {"ok": bool(token), "token_prefix": token[:6]}

    async def _instruments() -> Dict[str, Any]:
        found = await rest.get_instruments(symbols)
        return {
            symbol: (
                {
                    "lot_size": inst.lot_size,
                    "minimum_step": inst.minimum_step,
                    "scale": inst.scale,
                    "is_can_short": inst.is_can_short,
                    "is_can_margin": inst.is_can_margin,
                    "is_blocked": inst.is_blocked,
                    "boards": inst.class_codes,
                }
                if (inst := found.get(symbol))
                else None
            )
            for symbol in symbols
        }

    async def _candles() -> Dict[str, Any]:
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=3)
        out: Dict[str, Any] = {}
        for symbol in symbols:
            bars = await rest.get_candles(symbol, start, end)
            out[symbol] = {
                "bars": len(bars),
                "last_time": bars[-1].time.isoformat() if bars else None,
                "last_close": bars[-1].close if bars else None,
            }
        return out

    await _check("auth", _auth())
    await _check("instruments", _instruments())
    await _check("trading_status", rest.get_trading_status())
    await _check("candles", _candles())
    return result
