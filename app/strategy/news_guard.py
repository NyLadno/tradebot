"""Мост между новостным пайплайном и торговым состоянием.

Пайплайн новостей пишет блокировки в ``news_alerts`` (``is_blocked`` +
``block_until``), а торговый цикл читает ``bot_state.is_news_blocked``.
Раньше эти половины ничем не были связаны — здесь они соединяются:
перед каждым решением движок пересчитывает признак блокировки по
активным алертам и синхронизирует его в ``bot_state``.

Три линии защиты из схемы БД сохраняются: автоблок (AUTO),
ручное продление (MANUAL_EXTEND) и автоснятие по истечении block_until.
"""

from __future__ import annotations

from typing import Any, Dict, List

from app.config import settings
from app.logging_setup import get_logger
from app.storage.news_alerts import get_active_blocks

logger = get_logger("tradebot.strategy.news")


def summarize(blocks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Свести активные блокировки в патч для ``bot_state``."""
    if not blocks:
        return {
            "is_news_blocked": False,
            "news_block_until": None,
            "news_block_source": "NONE",
            "current_news_alert_id": None,
        }

    # Блоки могут перекрываться — торгуем не раньше самого позднего из них.
    latest = max(
        blocks,
        key=lambda row: str(row.get("extended_until") or row.get("block_until") or ""),
    )
    return {
        "is_news_blocked": True,
        "news_block_until": latest.get("extended_until") or latest.get("block_until"),
        "news_block_source": latest.get("block_source") or "AUTO",
        "current_news_alert_id": latest.get("id"),
    }


async def sync_bot_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """Пересчитать блокировку и вернуть патч, если состояние изменилось.

    Патч возвращается вызывающему коду, а не применяется здесь, чтобы
    движок мог объединить его с остальными обновлениями ``bot_state``
    в один PATCH-запрос.
    """
    try:
        blocks = await get_active_blocks()
    except Exception as exc:  # noqa: BLE001 — новостной сбой не должен ронять цикл
        logger.error("[NEWS] Не удалось прочитать активные блокировки: %s", exc)
        # Fail-safe совпадает с политикой LLM-оценщика: при неизвестности —
        # считаем, что блокировка есть, если она уже была выставлена.
        return {}

    desired = summarize(blocks)
    patch = {
        key: value
        for key, value in desired.items()
        if state.get(key) != value
    }

    if patch.get("is_news_blocked") is True:
        logger.warning(
            "[NEWS] Торговля заблокирована до %s (%s), алерт #%s: %s",
            desired["news_block_until"],
            desired["news_block_source"],
            desired["current_news_alert_id"],
            (blocks[0].get("article_title") or "")[:80],
        )
    elif patch.get("is_news_blocked") is False:
        logger.info("[NEWS] Новостная блокировка снята — торговля разрешена")

    return patch


def block_window_hours(params: Dict[str, Any]) -> float:
    """Длительность новостной блокировки.

    Единый источник истины — ``strategy_params.news_cooldown_hours``;
    ``settings.cooldown_hours`` остаётся запасным значением.
    """
    try:
        hours = float(params.get("news_cooldown_hours"))
    except (TypeError, ValueError):
        hours = 0.0
    return hours if hours > 0 else float(settings.cooldown_hours)
