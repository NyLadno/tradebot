"""LLM-based news risk evaluator (Google Gemini, stateless)."""

from __future__ import annotations

import asyncio
import logging
import math
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

from app.config import (
    HTML_HEADERS,
    LAMBDA_BASE,
    LAMBDA_SCALE,
    RISK_THRESHOLD,
    RISK_WEIGHT_M,
    RISK_WEIGHT_S,
    RISK_WEIGHT_U,
    settings,
)
from app.llm.models import Factors, NewsRiskDecision
from app.llm.prompts import SYSTEM_INSTRUCTION
from app.logging_setup import get_logger
from app.retry import async_retry
from app.storage.logs import log_event

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover
    BeautifulSoup = None

try:
    from google import genai
    from google.genai import types
except ImportError:  # pragma: no cover
    genai = None
    types = None

logger = get_logger("tradebot.llm_evaluator")

MAX_ARTICLE_CHARS = settings.llm_max_article_chars


class LLMRiskEvaluator:
    """
    Stateless risk evaluation of news articles via Google Gemini.
    Each article is processed in an independent request without conversation memory.
    """

    def __init__(self, model_name: Optional[str] = None):
        if genai is None:
            raise RuntimeError("Установите пакет google-genai: pip install google-genai")

        self.model_name = model_name or settings.gemini_model_name
        api_key = settings.gemini_api_key
        if not api_key:
            logger.warning(
                "GEMINI_API_KEY или GOOGLE_API_KEY не установлен, "
                "попытка работы с дефолтным окружением"
            )
        self.client = genai.Client(api_key=api_key) if api_key else genai.Client()

    @staticmethod
    def calculate_trading_hours_tau(pub_time: datetime, now_time: datetime) -> float:
        """Hours elapsed between publication and now (tau for decay formula)."""
        if pub_time.tzinfo is None:
            pub_time = pub_time.replace(tzinfo=timezone.utc)
        if now_time.tzinfo is None:
            now_time = now_time.replace(tzinfo=timezone.utc)

        diff_seconds = (now_time - pub_time).total_seconds()
        if diff_seconds < 0:
            return 0.0
        return round(diff_seconds / 3600.0, 3)

    @staticmethod
    def compute_bot_formula(factors: Factors, tau: float) -> float:
        """
        Step 4 math (bot-side, not LLM):
        RiskScore_base = (R/10) × C × (0.35×S + 0.40×M + 0.25×U)
        λ(M) = 0.347 − 0.277 × (M/10)
        T_decay = exp(−λ(M) × τ)
        RiskScore_final = RiskScore_base × T_decay
        """
        r = float(factors.R.value)
        c = float(factors.C.value)
        s = float(factors.S.value)
        m = float(factors.M.value)
        u = float(factors.U.value)

        risk_score_base = (r / 10.0) * c * (
            RISK_WEIGHT_S * s + RISK_WEIGHT_M * m + RISK_WEIGHT_U * u
        )
        lambda_m = LAMBDA_BASE - LAMBDA_SCALE * (m / 10.0)
        t_decay = math.exp(-lambda_m * max(0.0, tau))
        return round(risk_score_base * t_decay, 2)

    async def fetch_article_text(
        self,
        url: str,
        fallback_text: str = "",
        client: Optional[httpx.AsyncClient] = None,
    ) -> str:
        """Scrape article body text; fall back to title/description on failure."""
        if not url or not url.startswith("http"):
            return fallback_text or "Текст отсутствует."

        should_close = False
        if client is None:
            client = httpx.AsyncClient(
                timeout=settings.scrape_timeout, follow_redirects=True
            )
            should_close = True

        try:
            resp = await client.get(url, headers=HTML_HEADERS)
            if resp.status_code == 200 and resp.text:
                extracted = self._extract_text_from_html(resp.text)
                if extracted:
                    return extracted
        except Exception as exc:
            logger.debug(
                "Не удалось спарсить страницу %s: %s. Используем fallback.",
                url,
                exc,
            )
        finally:
            if should_close:
                await client.aclose()

        return (
            fallback_text
            or "Текст статьи недоступен. Оценка по заголовку и метаданным."
        )

    @staticmethod
    def _extract_text_from_html(html: str) -> Optional[str]:
        if BeautifulSoup:
            soup = BeautifulSoup(html, "html.parser")
            for element in soup(
                ["script", "style", "nav", "footer", "header", "noscript"]
            ):
                element.decompose()

            paragraphs = soup.find_all("p")
            text_blocks = [
                p.get_text(strip=True)
                for p in paragraphs
                if len(p.get_text(strip=True)) > 25
            ]
            extracted_text = "\n".join(text_blocks)
            if len(extracted_text) > 100:
                return extracted_text[:MAX_ARTICLE_CHARS]

            body_text = soup.get_text(separator="\n", strip=True)
            clean_lines = [
                line for line in body_text.split("\n") if len(line.strip()) > 30
            ]
            if clean_lines:
                return "\n".join(clean_lines)[:MAX_ARTICLE_CHARS]
            return None

        clean_html = re.sub(r"<[^>]+>", " ", html)
        clean_text = " ".join(clean_html.split())
        if len(clean_text) > 100:
            return clean_text[:MAX_ARTICLE_CHARS]
        return None

    @async_retry()
    async def _call_llm(self, user_content: str) -> NewsRiskDecision:
        """Call Gemini with retry and structured JSON output."""
        if types is None:
            raise RuntimeError("google-genai types are not available")

        response = await self.client.aio.models.generate_content(
            model=self.model_name,
            contents=user_content,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_INSTRUCTION,
                response_mime_type="application/json",
                response_schema=NewsRiskDecision,
                temperature=0.1,
            ),
        )
        return response.parsed

    async def evaluate_single_news(
        self,
        payload: Dict[str, Any],
        http_client: Optional[httpx.AsyncClient] = None,
    ) -> Dict[str, Any]:
        """
        Evaluate one article: scrape → tau → LLM → recompute score → set is_blocked.
        """
        url = payload.get("article_url", "")
        title = payload.get("article_title", "")
        raw = (
            payload.get("raw_article", {})
            if isinstance(payload.get("raw_article"), dict)
            else {}
        )
        fallback = raw.get("description", "") or raw.get("summary", "") or title

        news_text = await self.fetch_article_text(
            url,
            fallback_text=f"{title}. {fallback}",
            client=http_client,
        )

        now_dt = datetime.now(timezone.utc)
        pub_str = payload.get("article_published_at")
        try:
            pub_dt = (
                datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                if pub_str
                else now_dt
            )
        except Exception:
            pub_dt = now_dt

        tau = self.calculate_trading_hours_tau(pub_dt, now_dt)

        user_content = (
            f"--- ТЕКСТ НОВОСТИ НАЧАЛО ---\n{news_text}\n--- ТЕКСТ НОВОСТИ КОНЕЦ ---\n\n"
            f"--- МЕТАДАННЫЕ ---\n"
            f"Время публикации: {pub_dt.isoformat()}\n"
            f"Текущее время: {now_dt.isoformat()}\n"
            f"Время в торговых часах (τ): {tau}\n"
        )

        try:
            result: NewsRiskDecision = await self._call_llm(user_content)

            if not result.tier0 and result.factors:
                bot_score = self.compute_bot_formula(result.factors, tau)
                result.score = bot_score
                result.decision = bot_score < RISK_THRESHOLD
            elif result.tier0:
                result.score = None
                result.decision = False

            # decision=true  → торговля разрешена  → is_blocked=false
            # decision=false → торговля запрещена  → is_blocked=true
            payload["is_blocked"] = not result.decision
            payload["llm_evaluation"] = result.model_dump()
            logger.info(
                "[LLM] Новость: '%s...' -> decision=%s, tier0=%s, score=%s, is_blocked=%s",
                title[:50],
                result.decision,
                result.tier0,
                result.score,
                payload["is_blocked"],
            )
        except Exception as exc:
            logger.error(
                "[LLM] Ошибка при оценке новости '%s...': %s. "
                "Фолбек: is_blocked=True (безопасный режим).",
                title[:50],
                exc,
            )
            payload["is_blocked"] = True
            payload["llm_evaluation"] = {
                "error": str(exc),
                "decision": False,
                "tier0": False,
                "reason": "Ошибка вызова/парсинга LLM, сработал fail-safe",
            }
            if http_client is not None:
                await log_event(
                    "ERROR",
                    "risk",
                    f"LLM fail-safe block: {title[:50]}",
                    http_client,
                    details={"error": str(exc), "url": url},
                )

        return payload

    async def evaluate_news_batch(
        self,
        payloads: List[Dict[str, Any]],
        http_client: Optional[httpx.AsyncClient] = None,
        max_concurrency: int = settings.llm_max_concurrency,
    ) -> List[Dict[str, Any]]:
        """Parallel stateless evaluation with a concurrency semaphore."""
        if not payloads:
            return []

        semaphore = asyncio.Semaphore(max_concurrency)

        async def _bound_evaluate(item: Dict[str, Any]) -> Dict[str, Any]:
            async with semaphore:
                return await self.evaluate_single_news(item, http_client)

        evaluated = await asyncio.gather(*[_bound_evaluate(p) for p in payloads])
        return list(evaluated)
