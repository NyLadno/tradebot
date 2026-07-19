"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Dict, List

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    """Immutable application settings."""

    news_api_key: str = field(default_factory=lambda: os.getenv("NEWS_API", ""))
    supabase_url: str = field(
        default_factory=lambda: os.getenv("NEXT_PUBLIC_SUPABASE_URL", "").rstrip("/")
    )
    supabase_key: str = field(
        default_factory=lambda: os.getenv("NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY", "")
    )
    supabase_table: str = field(
        default_factory=lambda: os.getenv("SUPABASE_TABLE", "news_alerts")
    )
    gemini_api_key: str = field(
        default_factory=lambda: os.getenv("GEMINI_API_KEY")
        or os.getenv("GOOGLE_API_KEY")
        or ""
    )
    gemini_model_name: str = field(
        default_factory=lambda: os.getenv("GEMINI_MODEL_NAME", "gemini-3.1-flash-lite")
    )
    enable_llm_evaluation: bool = field(
        default_factory=lambda: os.getenv("ENABLE_LLM_EVALUATION", "true").lower()
        == "true"
    )
    telegram_bot_token: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_BOT_TOKEN", "")
    )
    telegram_chat_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_CHAT_ID", "")
    )
    telegram_thread_id: str = field(
        default_factory=lambda: os.getenv("TELEGRAM_THREAD_ID", "")
    )
    http_timeout: float = 30.0
    scrape_timeout: float = 15.0

    @property
    def telegram_chat_ids(self) -> List[str]:
        """Return non-empty list of configured Telegram chat IDs.

        Supports a single chat or multiple chats separated by commas, e.g.:
        ``TELEGRAM_CHAT_ID=123,456,789``.
        """
        return [chat.strip() for chat in self.telegram_chat_id.split(",") if chat.strip()]

    @property
    def telegram_thread_ids(self) -> List[int]:
        """Return parsed list of configured Telegram thread IDs.

        Supports a single thread or multiple threads separated by commas.
        When the count matches ``telegram_chat_ids``, values are mapped
        positionally to each chat; otherwise the first value is reused
        for every chat.
        """
        if not self.telegram_thread_id:
            return []
        thread_ids: List[int] = []
        for part in self.telegram_thread_id.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                thread_ids.append(int(part))
            except ValueError:
                # Ignore malformed values.
                continue
        return thread_ids
    cooldown_hours: int = 1
    news_lookback_days: int = 7
    retry_max_attempts: int = 3
    retry_min_wait: float = 1.0
    retry_max_wait: float = 10.0
    llm_max_concurrency: int = 5
    llm_max_article_chars: int = 8000

    @property
    def supabase_rest_url(self) -> str:
        """Full Supabase REST URL for the configured table."""
        return f"{self.supabase_url}/rest/v1/{self.supabase_table}"

    def validate_supabase(self) -> None:
        """Raise if Supabase credentials are missing."""
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL и SUPABASE_KEY должны быть в .env")


settings = Settings()

# Shared browser-like headers for RSS / news fetches
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

RSS_HEADERS: Dict[str, str] = {
    "User-Agent": USER_AGENT,
    "Accept": "application/rss+xml,application/xml;q=0.9,*/*;q=0.8",
}

HTML_HEADERS: Dict[str, str] = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Tracked company queries for scheduled jobs
GOOGLE_JOBS = [
    ("Татнефть", "ru", "RU", "RU:ru"),
    ("Tatneft", "en-US", "US", "US:en"),
    ("Tatneft", "de", "DE", "DE:de"),
]

NEWSAPI_JOBS = [
    ("Татнефть", "ru"),
    ("Tatneft", "en"),
    ("Tatneft", "de"),
]

RUSSIAN_RSS_SOURCES: Dict[str, str] = {
    "ТАСС": "https://tass.ru/rss/v2.xml",
    "РИА Новости": "https://ria.ru/export/rss2/index.xml",
    "Интерфакс": "https://www.interfax.ru/rss.asp",
    "Ведомости": "https://www.vedomosti.ru/rss/news",
    "Лента.ру": "https://lenta.ru/rss/",
    "Новости Mail.ru": "https://news.mail.ru/rss/90/",
}

# Keywords that may affect Tatneft stock quotes
RELEVANT_KEYWORDS: List[str] = [
    r"татнефть",
    r"tatneft",
    r"танэко",
    r"taneco",
    r"маганов",
    r"нефть",
    r"нефтян",
    r"нефтепродукт",
    r"нефтеперераб",
    r"нефтедобыч",
    r"нпз",
    r"нефтепровод",
    r"\bndpi\b",
    r"\bндпи\b",
    r"\bндд\b",
    r"демпфер",
    r"\bопек",
    r"\bopec",
    r"\bbrent\b",
    r"брент",
    r"\burals\b",
    r"юралс",
    r"баррел",
    r"бензин",
    r"дизел",
    r"топлив",
    r"лукойл",
    r"lukoil",
    r"роснефть",
    r"rosneft",
    r"сургутнефтег",
    r"surgutneft",
    r"газпром\s*нефть",
    r"транснефть",
    r"transneft",
]

# Risk formula constants (must match SYSTEM_INSTRUCTION)
RISK_THRESHOLD = 6.0
RISK_WEIGHT_S = 0.35
RISK_WEIGHT_M = 0.40
RISK_WEIGHT_U = 0.25
LAMBDA_BASE = 0.347
LAMBDA_SCALE = 0.277
