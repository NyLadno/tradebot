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
        if client is None:
            client = httpx.AsyncClient(timeout=settings.http_timeout)

        sent_to: List[str] = []
        failed: List[str] = []
        try:
            chat_ids = await self._resolve_chat_ids(client)
            if not chat_ids:
                return False

            message = self._format_message(news_record)
            for index, chat_id in enumerate(chat_ids):
                thread_id = self._thread_id_for(index)
                try:
                    await self._send_telegram_message(
                        message, chat_id, client, thread_id
                    )
                    sent_to.append(chat_id)
                except Exception as exc:
                    logger.error("[TG] Ошибка отправки в %s: %s", chat_id, exc)
                    failed.append(chat_id)

            title = news_record.get("article_title", "")
            if sent_to:
                logger.info(
                    "[TG] Уведомление отправлено в %s чат(ов): %s...",
                    len(sent_to),
                    title[:50],
                )
            if failed:
                logger.warning(
                    "[TG] Не удалось отправить в %s чат(ов): %s",
                    len(failed),
                    ", ".join(failed),
                )
        except Exception as exc:
            logger.error("[TG] Ошибка отправки: %s", exc)
        finally:
            if should_close:
                await client.aclose()

        sent = bool(sent_to)
        if sent and news_record.get("id"):
            try:
                await mark_telegram_sent(news_record["id"], client)
                news_record["telegram_sent"] = True
            except Exception as exc:
                logger.error("[TG] Не удалось обновить флаг telegram_sent: %s", exc)

        return sent

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
