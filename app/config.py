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
    bot_admin_token: str = field(
        default_factory=lambda: os.getenv("BOT_ADMIN_TOKEN", "")
    )

    # --- БКС Trade API ---
    bcs_api_base: str = field(
        default_factory=lambda: os.getenv("BCS_API_BASE", "https://be.broker.ru").rstrip("/")
    )
    bcs_ws_base: str = field(
        default_factory=lambda: os.getenv("BCS_WS_BASE", "wss://ws.broker.ru").rstrip("/")
    )
    bcs_refresh_token_read: str = field(
        default_factory=lambda: os.getenv("BCS_REFRESH_TOKEN_READ", "")
    )
    bcs_refresh_token_write: str = field(
        default_factory=lambda: os.getenv("BCS_REFRESH_TOKEN_WRITE", "")
    )
    bcs_token_store: str = field(
        default_factory=lambda: os.getenv("BCS_TOKEN_STORE", ".bcs_tokens.json")
    )
    bcs_class_code: str = field(
        default_factory=lambda: os.getenv("BCS_CLASS_CODE", "TQBR")
    )

    # --- Торговый движок ---
    enable_trading_engine: bool = field(
        default_factory=lambda: os.getenv("ENABLE_TRADING_ENGINE", "true").lower() == "true"
    )
    trading_mode: str = field(
        default_factory=lambda: os.getenv("BCS_TRADING_MODE", "PAPER").upper()
    )
    pair_leg1: str = field(default_factory=lambda: os.getenv("PAIR_LEG1", "TATN"))
    pair_leg2: str = field(default_factory=lambda: os.getenv("PAIR_LEG2", "TATNP"))

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

    # Тайминги торгового контура (не выносим в env — меняются только с кодом)
    bcs_token_refresh_margin_sec: int = 300
    bcs_ws_reconnect_min: float = 1.0
    bcs_ws_reconnect_max: float = 60.0
    bcs_ws_ping_interval: float = 20.0
    bcs_order_poll_sec: float = 1.0
    bcs_order_timeout_sec: float = 30.0
    strategy_params_ttl_sec: float = 60.0
    strategy_max_consecutive_errors: int = 3

    @property
    def pair_symbols(self) -> List[str]:
        """Оба тикера пары в порядке leg1, leg2."""
        return [self.pair_leg1, self.pair_leg2]

    @property
    def is_live_trading(self) -> bool:
        """True, если движку разрешено отправлять реальные заявки."""
        return self.trading_mode == "LIVE"

    def rest_url(self, table: str) -> str:
        """Full Supabase REST URL for an arbitrary table."""
        return f"{self.supabase_url}/rest/v1/{table}"

    @property
    def supabase_rest_url(self) -> str:
        """Full Supabase REST URL for the configured (news_alerts) table."""
        return self.rest_url(self.supabase_table)

    def validate_supabase(self) -> None:
        """Raise if Supabase credentials are missing."""
        if not self.supabase_url or not self.supabase_key:
            raise ValueError("SUPABASE_URL и SUPABASE_KEY должны быть в .env")

    def validate_bcs(self) -> None:
        """Raise if БКС credentials are missing for the configured trading mode.

        Вызывается лениво (при старте движка), а не при импорте, чтобы
        новостной пайплайн продолжал работать без токенов брокера.
        """
        if self.trading_mode not in ("PAPER", "LIVE"):
            raise ValueError(
                f"BCS_TRADING_MODE должен быть PAPER или LIVE, получено: {self.trading_mode!r}"
            )
        if not self.bcs_refresh_token_read:
            raise ValueError("BCS_REFRESH_TOKEN_READ должен быть в .env")
        if self.is_live_trading and not self.bcs_refresh_token_write:
            raise ValueError(
                "BCS_TRADING_MODE=LIVE требует BCS_REFRESH_TOKEN_WRITE (права trade-api-write)"
            )


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

# Human-readable keywords stored in query_keywords for Russian RSS articles.
RSS_QUERY_KEYWORDS: str = (
    "Татнефть, Tatneft, ТАНЭКО, Taneco, Маганов, "
    "нефть, нефтяной, нефтепродукты, нефтепереработка, нефтедобыча, "
    "НПЗ, нефтепровод, НДПИ, НДД, демпфер, ОПЕК, OPEC, "
    "Brent, Брент, Urals, Юралс, баррель, бензин, дизель, топливо, "
    "Лукойл, Lukoil, Роснефть, Rosneft, Сургутнефтегаз, Surgutneft, "
    "Газпром нефть, Транснефть, Transneft"
)

# Risk formula constants (must match SYSTEM_INSTRUCTION)
RISK_THRESHOLD = 6.0
RISK_WEIGHT_S = 0.35
RISK_WEIGHT_M = 0.40
RISK_WEIGHT_U = 0.25
LAMBDA_BASE = 0.347
LAMBDA_SCALE = 0.277
