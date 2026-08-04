"""Async Telegram bot for blocked-news alerts.

Sends a notification exactly once per record (guarded by the ``telegram_sent``
column in ``news_alerts``) and updates the flag after a successful send.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from app.config import settings
from app.logging_setup import get_logger
from app.storage.supabase import mark_telegram_sent
from app.storage.telegram_events import record_telegram_event

logger = get_logger("tradebot.telegram")


class TelegramAlertsBot:
    """Send Telegram notifications for blocked news and update telegram_sent."""

    def __init__(self) -> None:
        self.token = settings.telegram_bot_token
        self.chat_ids: List[str] = settings.telegram_chat_ids
        self.thread_ids: List[int] = settings.telegram_thread_ids
        self._lock = None

        self.enabled = bool(self.token and self.chat_ids)
        if not self.enabled:
            logger.warning(
                "Telegram-бот отключён: не заданы TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID"
            )

    def _ensure_lock(self):
        """Lazy asyncio.Lock to serialize chat_id auto-discovery."""
        if self._lock is None:
            import asyncio

            self._lock = asyncio.Lock()
        return self._lock

    async def _resolve_chat_ids(self, client: httpx.AsyncClient) -> List[str]:
        """Return configured chat IDs or auto-discover one from getUpdates."""
        if self.chat_ids:
            return self.chat_ids
        if not self.token:
            return []

        async with self._ensure_lock():
            # Double-checked locking pattern inside async.
            if self.chat_ids:
                return self.chat_ids

            url = f"https://api.telegram.org/bot{self.token}/getUpdates"
            resp = await client.get(url, timeout=10.0)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("ok"):
                raise RuntimeError(f"Telegram API error: {data}")

            results = data.get("result", [])
            if not results:
                raise RuntimeError(
                    "Нет сообщений для бота. Отправьте боту /start и попробуйте снова."
                )

            chat = self._extract_chat(results[-1])
            if not chat:
                raise RuntimeError("Не удалось извлечь chat из обновлений")

            chat_id = str(chat.get("id"))
            chat_type = chat.get("type", "unknown")
            chat_title = chat.get("title", "") or chat.get("username", "")
            logger.info(
                "[TG] Найден чат: type=%s, title=%s, id=%s",
                chat_type,
                chat_title,
                chat_id,
            )
            self.chat_ids = [chat_id]
            self.enabled = bool(self.token and self.chat_ids)
            return self.chat_ids

    @staticmethod
    def _extract_chat(update: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract the chat object from a Telegram update."""
        return (
            update.get("message", {}).get("chat")
            or update.get("callback_query", {}).get("message", {}).get("chat")
            or update.get("edited_message", {}).get("chat")
            or update.get("channel_post", {}).get("chat")
            or update.get("edited_channel_post", {}).get("chat")
        )

    @staticmethod
    def _chat_id_int(chat_id: str) -> int:
        """Числовой chat_id для таблицы telegram_events.

        Колонка ``chat_id`` объявлена NOT NULL, а чат может быть задан
        как ``@channelname``. В этом случае пишем 0: потерять строку
        телеметрии хуже, чем записать её без числового идентификатора.
        """
        try:
            return int(chat_id)
        except (TypeError, ValueError):
            logger.warning(
                "[TG] Нечисловой chat_id %r — в telegram_events записываю 0", chat_id
            )
            return 0

    async def _broadcast(
        self,
        message: str,
        event_type: str,
        client: httpx.AsyncClient,
        **event_fields: Any,
    ) -> List[str]:
        """Разослать сообщение во все чаты и записать события.

        Возвращает список чатов, куда доставка удалась. Общая часть для
        всех типов уведомлений: новости, входы/выходы, разрывы, ошибки.
        """
        chat_ids = await self._resolve_chat_ids(client)
        if not chat_ids:
            return []

        sent_to: List[str] = []
        failed: List[str] = []
        for index, chat_id in enumerate(chat_ids):
            thread_id = self._thread_id_for(index)
            try:
                await self._send_telegram_message(message, chat_id, client, thread_id)
                sent_to.append(chat_id)
                await record_telegram_event(
                    event_type,
                    self._chat_id_int(chat_id),
                    client,
                    message_text=message,
                    is_success=True,
                    **event_fields,
                )
            except Exception as exc:
                logger.error("[TG] Ошибка отправки в %s: %s", chat_id, exc)
                failed.append(chat_id)
                await record_telegram_event(
                    event_type,
                    self._chat_id_int(chat_id),
                    client,
                    message_text=message,
                    is_success=False,
                    error_text=str(exc),
                    **event_fields,
                )

        if failed:
            logger.warning(
                "[TG] %s: не удалось отправить в %s чат(ов): %s",
                event_type,
                len(failed),
                ", ".join(failed),
            )
        return sent_to

    async def _send(
        self,
        message: str,
        event_type: str,
        client: Optional[httpx.AsyncClient],
        **event_fields: Any,
    ) -> bool:
        """Обёртка над _broadcast, управляющая временным HTTP-клиентом."""
        if not self.token:
            logger.debug("TELEGRAM_BOT_TOKEN не задан, пропускаем уведомление")
            return False

        should_close = client is None
        active = client or httpx.AsyncClient(timeout=settings.http_timeout)
        try:
            sent_to = await self._broadcast(message, event_type, active, **event_fields)
            return bool(sent_to)
        except Exception as exc:  # noqa: BLE001 — уведомления не ломают бизнес-путь
            logger.error("[TG] Ошибка отправки %s: %s", event_type, exc)
            return False
        finally:
            if should_close:
                await active.aclose()

    async def notify_blocked_news(
        self,
        news_record: Dict[str, Any],
        client: Optional[httpx.AsyncClient] = None,
    ) -> bool:
        """
        Send a Telegram notification for a blocked news record.

        The notification is idempotent: it is skipped when ``telegram_sent`` is
        already true and the ``telegram_sent`` flag is updated in Supabase after
        a successful send.

        Args:
            news_record: news_alerts row (must contain article_url, article_title)
            client: reusable httpx.AsyncClient

        Returns:
            True if the message was sent, otherwise False.
        """
        if not news_record.get("is_blocked"):
            logger.debug("Новость не заблокирована, уведомление не требуется")
            return False

        if news_record.get("telegram_sent"):
            logger.debug("Уведомление уже отправлено ранее, пропускаем")
            return False

        if not self.token:
            logger.debug("TELEGRAM_BOT_TOKEN не задан, пропускаем уведомление")
            return False

        should_close = client is None
        active = client or httpx.AsyncClient(timeout=settings.http_timeout)
        sent = False
        try:
            message = self._format_message(news_record)
            sent_to = await self._broadcast(
                message,
                "NEWS_BLOCK",
                active,
                related_news_alert_id=news_record.get("id"),
            )
            sent = bool(sent_to)
            if sent:
                logger.info(
                    "[TG] Уведомление отправлено в %s чат(ов): %s...",
                    len(sent_to),
                    str(news_record.get("article_title", ""))[:50],
                )
            # Флаг обновляем ДО закрытия клиента: иначе запрос уйдёт
            # в уже закрытое соединение и алерт продублируется.
            if sent and news_record.get("id"):
                try:
                    await mark_telegram_sent(news_record["id"], active)
                    news_record["telegram_sent"] = True
                except Exception as exc:
                    logger.error("[TG] Не удалось обновить флаг telegram_sent: %s", exc)
        except Exception as exc:
            logger.error("[TG] Ошибка отправки: %s", exc)
        finally:
            if should_close:
                await active.aclose()

        return sent

    # ------------------------------------------------------------------
    # Торговые уведомления
    # ------------------------------------------------------------------

    async def notify_trade_entry(
        self, trade: Dict[str, Any], client: Optional[httpx.AsyncClient] = None
    ) -> bool:
        """Уведомление об открытии парной позиции."""
        return await self._send(
            self._format_entry(trade),
            "ENTRY",
            client,
            trade_uuid=trade.get("trade_uuid"),
        )

    async def notify_trade_exit(
        self, trade: Dict[str, Any], client: Optional[httpx.AsyncClient] = None
    ) -> bool:
        """Уведомление о закрытии позиции с итоговым P&L."""
        return await self._send(
            self._format_exit(trade),
            "EXIT",
            client,
            trade_uuid=trade.get("trade_uuid"),
        )

    async def notify_data_gap(
        self, gap: Dict[str, Any], client: Optional[httpx.AsyncClient] = None
    ) -> bool:
        """Уведомление о пропаже потока котировок."""
        lines = [
            "📉 <b>РАЗРЫВ ДАННЫХ</b>",
            "",
            self._escape_html(str(gap.get("message") or "Поток котировок БКС молчит")),
        ]
        if gap.get("started_at"):
            lines.append(f"🕐 Начало: <code>{self._escape_html(str(gap['started_at']))}</code>")
        lines.append("")
        lines.append("⚠️ Решения по стратегии приостановлены до восстановления данных.")
        return await self._send("\n".join(lines), "DATA_GAP", client)

    async def notify_error(
        self,
        component: str,
        message: str,
        client: Optional[httpx.AsyncClient] = None,
        *,
        trade_uuid: Optional[str] = None,
    ) -> bool:
        """Уведомление об ошибке торгового контура."""
        text = "\n".join(
            [
                "❗️ <b>ОШИБКА БОТА</b>",
                "",
                f"⚙️ Компонент: <code>{self._escape_html(component)}</code>",
                f"📝 {self._escape_html(message)}",
            ]
        )
        return await self._send(text, "ERROR", client, trade_uuid=trade_uuid)

    def _format_entry(self, trade: Dict[str, Any]) -> str:
        """HTML-сообщение об открытии позиции."""
        direction = str(trade.get("direction") or "")
        leg1 = trade.get("leg1_symbol") or settings.pair_leg1
        leg2 = trade.get("leg2_symbol") or settings.pair_leg2
        long_leg1 = direction.startswith("LONG_")
        mode = trade.get("mode") or "PAPER"

        lines = [
            f"📈 <b>ВХОД В ПОЗИЦИЮ</b> <code>{self._escape_html(str(mode))}</code>",
            "",
            f"🧭 Направление: <code>{self._escape_html(direction)}</code>",
            (
                f"🟢 Лонг {self._escape_html(str(leg1 if long_leg1 else leg2))} "
                f"× {self._num(trade.get('leg1_qty' if long_leg1 else 'leg2_qty'), 0)} "
                f"@ {self._num(trade.get('leg1_entry_price' if long_leg1 else 'leg2_entry_price'))}"
            ),
            (
                f"🔴 Шорт {self._escape_html(str(leg2 if long_leg1 else leg1))} "
                f"× {self._num(trade.get('leg2_qty' if long_leg1 else 'leg1_qty'), 0)} "
                f"@ {self._num(trade.get('leg2_entry_price' if long_leg1 else 'leg1_entry_price'))}"
            ),
            "",
            f"📊 z-score входа: <b>{self._num(trade.get('zscore_entry'))}</b>",
            f"💰 Размер позиции: <code>{self._num(trade.get('position_size_rub'))} ₽</code>",
        ]
        if trade.get("trade_uuid"):
            lines.append(f"🆔 <code>{self._escape_html(str(trade['trade_uuid']))}</code>")
        return "\n".join(lines)

    def _format_exit(self, trade: Dict[str, Any]) -> str:
        """HTML-сообщение о закрытии позиции."""
        reason = str(trade.get("exit_reason") or "?")
        icons = {
            "TP": "✅", "STOP": "🛑", "TIMEOUT": "⏰", "NEWS": "📰", "MANUAL": "✋",
        }
        try:
            net = float(trade.get("net_pnl_rub") or 0.0)
        except (TypeError, ValueError):
            net = 0.0

        lines = [
            f"{icons.get(reason, '📉')} <b>ВЫХОД ИЗ ПОЗИЦИИ</b> — {self._escape_html(reason)}",
            "",
            f"🧭 <code>{self._escape_html(str(trade.get('direction') or ''))}</code>",
            f"📊 z-score выхода: <b>{self._num(trade.get('zscore_exit'))}</b>",
            "",
            f"{'🟢' if net >= 0 else '🔴'} <b>P&amp;L: {net:+.2f} ₽</b>",
            f"    валовый: {self._num(trade.get('gross_pnl_rub'))} ₽",
            f"    комиссия: −{self._num(trade.get('commission_rub'), 2)} ₽",
            f"    перенос: −{self._num(trade.get('overnight_fees_rub'), 2)} ₽",
            "",
            f"⏱ Удержано: <code>{trade.get('hold_time_min', '?')} мин</code>",
        ]
        if trade.get("max_drawdown_rub") is not None:
            lines.append(f"📉 Макс. просадка: <code>{self._num(trade.get('max_drawdown_rub'))} ₽</code>")
        if trade.get("trade_uuid"):
            lines.append(f"🆔 <code>{self._escape_html(str(trade['trade_uuid']))}</code>")
        return "\n".join(lines)

    @staticmethod
    def _num(value: Any, digits: int = 2) -> str:
        """Аккуратно отформатировать число, пришедшее из PostgREST строкой."""
        if value is None:
            return "—"
        try:
            return f"{float(value):,.{digits}f}".replace(",", " ")
        except (TypeError, ValueError):
            return str(value)

    def _format_message(self, news: Dict[str, Any]) -> str:
        """Build an HTML message for Telegram."""
        title = news.get("article_title", "Без названия")
        url = news.get("article_url", "")
        source_api = news.get("source_api", "unknown")
        query = news.get("query_keywords", "")
        published = news.get("article_published_at", "")
        llm_eval = (
            news.get("llm_evaluation", {})
            if isinstance(news.get("llm_evaluation"), dict)
            else {}
        )

        score = llm_eval.get("score")
        tier0 = llm_eval.get("tier0", False)
        reason = llm_eval.get("reason", "")

        lines = [
            "🚨 <b>ТОРГОВЛЯ ОСТАНОВЛЕНА</b>",
            "",
            f"📰 <b>{self._escape_html(title)}</b>",
        ]

        if url:
            lines.append(f"🔗 <a href='{self._escape_html(url)}'>Открыть статью</a>")

        lines.extend(
            [
                f"📡 Источник: <code>{self._escape_html(source_api)}</code>",
                f"🔍 Ключевое слово: <code>{self._escape_html(query)}</code>",
            ]
        )

        if published:
            try:
                dt = datetime.fromisoformat(published.replace("Z", "+00:00"))
                lines.append(
                    f"🕐 Опубликовано: <code>{dt.strftime('%d.%m.%Y %H:%M')}</code>"
                )
            except Exception:
                lines.append(f"🕐 Опубликовано: <code>{self._escape_html(published)}</code>")

        lines.append("")
        lines.append("📊 <b>Оценка риска LLM:</b>")

        if tier0:
            lines.append("⚠️ <b>TIER-0</b> — критический триггер!")
        elif score is not None:
            lines.append(f"🔴 RiskScore: <b>{score:.2f}</b> / 10.00")
        else:
            lines.append("🔴 Блокировка по умолчанию (fail-safe)")

        if reason:
            lines.append(f"📝 {self._escape_html(reason)}")

        lines.append("")
        lines.append("⛔ <b>Торговля приостановлена до разблокировки.</b>")

        return "\n".join(lines)

    @staticmethod
    def _escape_html(text: str) -> str:
        """Escape HTML special characters for Telegram HTML parse mode."""
        if not text:
            return ""
        return (
            text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    def _thread_id_for(self, chat_index: int) -> Optional[int]:
        """Return the message thread ID for a given chat index.

        When ``self.thread_ids`` contains a single value it is applied to
        every chat. When it contains multiple values they are mapped
        positionally to ``self.chat_ids``.
        """
        if not self.thread_ids:
            return None
        if len(self.thread_ids) == 1:
            return self.thread_ids[0]
        if chat_index < len(self.thread_ids):
            return self.thread_ids[chat_index]
        return None

    async def _send_telegram_message(
        self,
        message: str,
        chat_id: str,
        client: httpx.AsyncClient,
        thread_id: Optional[int] = None,
    ) -> None:
        """POST a message to the Telegram Bot API."""
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": False,
        }

        if thread_id is not None:
            payload["message_thread_id"] = thread_id

        resp = await client.post(url, json=payload, timeout=30.0)
        resp.raise_for_status()

        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram API error: {data.get('description', 'unknown')}")


_telegram_bot: Optional[TelegramAlertsBot] = None


def get_telegram_bot() -> TelegramAlertsBot:
    """Return the singleton TelegramAlertsBot instance."""
    global _telegram_bot
    if _telegram_bot is None:
        _telegram_bot = TelegramAlertsBot()
    return _telegram_bot


async def notify_blocked(
    news_record: Dict[str, Any], client: Optional[httpx.AsyncClient] = None
) -> bool:
    """Convenience wrapper around the singleton bot."""
    bot = get_telegram_bot()
    return await bot.notify_blocked_news(news_record, client)


async def notify_trade_entry(
    trade: Dict[str, Any], client: Optional[httpx.AsyncClient] = None
) -> bool:
    """Уведомить об открытии позиции."""
    return await get_telegram_bot().notify_trade_entry(trade, client)


async def notify_trade_exit(
    trade: Dict[str, Any], client: Optional[httpx.AsyncClient] = None
) -> bool:
    """Уведомить о закрытии позиции."""
    return await get_telegram_bot().notify_trade_exit(trade, client)


async def notify_data_gap(
    gap: Dict[str, Any], client: Optional[httpx.AsyncClient] = None
) -> bool:
    """Уведомить о разрыве потока котировок."""
    return await get_telegram_bot().notify_data_gap(gap, client)


async def notify_error(
    component: str, message: str, client: Optional[httpx.AsyncClient] = None
) -> bool:
    """Уведомить об ошибке торгового контура."""
    return await get_telegram_bot().notify_error(component, message, client)
