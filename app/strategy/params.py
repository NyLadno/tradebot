"""Кэшированное чтение параметров стратегии из таблицy ``strategy_params``.

Боевой цикл читает параметры на каждой итерации, чтобы их можно было менять
без рестарта (изменения идут через Owl, п. 8 ТЗ). Чтобы не долбить Supabase
раз в минуту на каждый тик, снимок кэшируется на ``strategy_params_ttl_sec``.

При невалидных значениях работаем на **последнем валидном** снимке, а не
на дефолтах: молча вернуться к «заводским» 2.5/2500 опаснее, чем продолжить
на тех параметрах, с которыми уже открыта позиция.
"""

from __future__ import annotations

import time
from typing import Any, Callable, Dict, Optional

from app.config import settings
from app.logging_setup import get_logger
from app.storage.strategy_params import get_strategy_params

logger = get_logger("tradebot.strategy.params")

# Значение по умолчанию → тип. Совпадает с DEFAULT'ами в схеме БД.
_NUMERIC_FIELDS: Dict[str, Callable[[Any], Any]] = {
    "commission_pct": float,
    "position_size": float,
    "spread_window": int,
    "entry_zscore": float,
    "exit_zscore": float,
    "stop_zscore": float,
    "max_hold_days": int,
    "min_hold_min": int,
    "overnight_pct": float,
    "session_end_buffer_min": int,
    "data_gap_alert_min": int,
    "news_check_interval_min": int,
    "news_cooldown_hours": float,
}

FALLBACK_PARAMS: Dict[str, Any] = {
    "commission_pct": 0.0003,
    "position_size": 50000.0,
    "spread_window": 2500,
    "entry_zscore": 2.5,
    "exit_zscore": 0.0,
    "stop_zscore": 4.5,
    "max_hold_days": 15,
    "min_hold_min": 120,
    "overnight_pct": 0.0001,
    "session_end_buffer_min": 90,
    "data_gap_alert_min": 10,
    "news_check_interval_min": 15,
    "news_cooldown_hours": 3.0,
}


class StrategyParamsError(ValueError):
    """Параметры в БД не проходят валидацию."""


def coerce(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Привести значения из PostgREST (numeric приходит строкой) к числам."""
    result: Dict[str, Any] = dict(FALLBACK_PARAMS)
    for key, caster in _NUMERIC_FIELDS.items():
        value = raw.get(key)
        if value is None:
            continue
        try:
            result[key] = caster(value)
        except (TypeError, ValueError):
            logger.warning(
                "[STRAT] Параметр %s имеет нечисловое значение %r — беру значение по умолчанию",
                key,
                value,
            )
    for key in ("updated_by", "updated_at", "update_comment"):
        if key in raw:
            result[key] = raw[key]
    return result


def validate(params: Dict[str, Any]) -> None:
    """Проверить параметры на осмысленность; бросить StrategyParamsError."""
    problems = []

    if params["spread_window"] < 100:
        problems.append(f"spread_window={params['spread_window']} < 100")
    if params["entry_zscore"] <= 0:
        problems.append(f"entry_zscore={params['entry_zscore']} должен быть > 0")
    if params["stop_zscore"] <= params["entry_zscore"]:
        problems.append(
            f"stop_zscore={params['stop_zscore']} должен быть > "
            f"entry_zscore={params['entry_zscore']}"
        )
    if abs(params["exit_zscore"]) >= params["entry_zscore"]:
        problems.append(
            f"|exit_zscore|={abs(params['exit_zscore'])} должен быть < "
            f"entry_zscore={params['entry_zscore']}"
        )
    if params["position_size"] <= 0:
        problems.append(f"position_size={params['position_size']} должен быть > 0")
    if params["min_hold_min"] < 0:
        problems.append(f"min_hold_min={params['min_hold_min']} должен быть >= 0")
    if params["max_hold_days"] <= 0:
        problems.append(f"max_hold_days={params['max_hold_days']} должен быть > 0")
    if not 0 <= params["commission_pct"] < 0.1:
        problems.append(f"commission_pct={params['commission_pct']} вне диапазона [0, 0.1)")

    if problems:
        raise StrategyParamsError("; ".join(problems))


class StrategyParamsCache:
    """Снимок параметров с TTL и запоминанием последнего валидного набора."""

    def __init__(self, ttl_sec: Optional[float] = None) -> None:
        self._ttl = ttl_sec if ttl_sec is not None else settings.strategy_params_ttl_sec
        self._snapshot: Optional[Dict[str, Any]] = None
        self._fetched_at: float = 0.0
        self._last_error: Optional[str] = None

    @property
    def last_error(self) -> Optional[str]:
        return self._last_error

    @property
    def snapshot(self) -> Dict[str, Any]:
        """Текущий снимок без обращения к БД (для эндпоинтов состояния)."""
        return dict(self._snapshot or FALLBACK_PARAMS)

    async def get(self, *, force: bool = False) -> Dict[str, Any]:
        """Вернуть параметры, перечитав их из БД не чаще раза в TTL."""
        now = time.monotonic()
        if not force and self._snapshot is not None and now - self._fetched_at < self._ttl:
            return dict(self._snapshot)

        try:
            raw = await get_strategy_params()
        except Exception as exc:  # noqa: BLE001 — сеть не должна останавливать торговлю
            self._last_error = f"чтение strategy_params не удалось: {exc}"
            logger.error("[STRAT] %s", self._last_error)
            return dict(self._snapshot or FALLBACK_PARAMS)

        if not raw:
            self._last_error = "strategy_params пуста (нет строки id=1)"
            logger.error("[STRAT] %s", self._last_error)
            return dict(self._snapshot or FALLBACK_PARAMS)

        params = coerce(raw)
        try:
            validate(params)
        except StrategyParamsError as exc:
            self._last_error = f"невалидные параметры в БД: {exc}"
            logger.error(
                "[STRAT] %s. Продолжаю на последнем валидном наборе.", self._last_error
            )
            return dict(self._snapshot or FALLBACK_PARAMS)

        if self._snapshot is not None:
            changed = {
                key: (self._snapshot.get(key), value)
                for key, value in params.items()
                if key in _NUMERIC_FIELDS and self._snapshot.get(key) != value
            }
            if changed:
                logger.info(
                    "[STRAT] Параметры обновлены (%s): %s",
                    params.get("updated_by") or "?",
                    ", ".join(f"{k}: {old}→{new}" for k, (old, new) in changed.items()),
                )

        self._snapshot = params
        self._fetched_at = now
        self._last_error = None
        return dict(params)


_cache: Optional[StrategyParamsCache] = None


def get_cache() -> StrategyParamsCache:
    """Синглтон кэша параметров."""
    global _cache
    if _cache is None:
        _cache = StrategyParamsCache()
    return _cache


async def get_params(*, force: bool = False) -> Dict[str, Any]:
    """Короткий доступ к параметрам стратегии."""
    return await get_cache().get(force=force)
