"""Авторизация в БКС Trade API (Keycloak / OIDC).

Схема из документации: в веб-версии БКС выпускается **refresh-токен**
(90 суток, привязан к одному брокерскому счёту и одному набору прав),
из него запросом к Keycloak получается **access-токен** (24 часа).

Критичный нюанс: Keycloak возвращает в ответе новый ``refresh_token``.
Если его не сохранить, при следующем обновлении можно остаться без доступа,
и придётся выпускать токен заново через веб-версию. Поэтому каждый полученный
refresh-токен пишется в локальный store (JSON, права 0600).
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import httpx

from app.broker.errors import BcsAuthError, BcsUnavailableError
from app.config import settings
from app.logging_setup import get_logger
from app.retry import fetch_with_retry

logger = get_logger("tradebot.bcs.auth")

TOKEN_PATH = "/trade-api-keycloak/realms/tradeapi/protocol/openid-connect/token"

CLIENT_ID_READ = "trade-api-read"
CLIENT_ID_WRITE = "trade-api-write"


def _mask(token: str) -> str:
    """Безопасное для логов представление токена."""
    if not token:
        return "<пусто>"
    return f"{token[:6]}…(len={len(token)})"


class BcsTokenManager:
    """Держит актуальный access-токен для одного набора прав."""

    def __init__(
        self,
        refresh_token: str,
        client_id: str,
        *,
        store_path: Optional[str] = None,
        api_base: Optional[str] = None,
    ) -> None:
        self.client_id = client_id
        self._api_base = (api_base or settings.bcs_api_base).rstrip("/")
        self._store_path = Path(store_path or settings.bcs_token_store)
        self._lock: Optional[asyncio.Lock] = None
        self._access_token: str = ""
        self._expires_at: float = 0.0
        self._refresh_token: str = self._load_stored_refresh() or refresh_token

    # --- store ---------------------------------------------------------

    def _read_store(self) -> Dict[str, Any]:
        try:
            if self._store_path.exists():
                return json.loads(self._store_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001 — store не должен ломать старт
            logger.warning("[BCS] Не удалось прочитать %s: %s", self._store_path, exc)
        return {}

    def _load_stored_refresh(self) -> Optional[str]:
        """Взять refresh-токен из store — он свежее того, что в .env."""
        stored = self._read_store().get(self.client_id)
        if isinstance(stored, dict) and stored.get("refresh_token"):
            logger.info(
                "[BCS] refresh-токен для %s взят из %s", self.client_id, self._store_path
            )
            return str(stored["refresh_token"])
        return None

    def _persist_refresh(self, refresh_token: str, refresh_expires_in: Any) -> None:
        """Записать ротированный refresh-токен рядом с остальными."""
        data = self._read_store()
        data[self.client_id] = {
            "refresh_token": refresh_token,
            "refresh_expires_in": refresh_expires_in,
            "saved_at": time.time(),
        }
        try:
            self._store_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = self._store_path.with_suffix(self._store_path.suffix + ".tmp")
            tmp_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
            os.chmod(tmp_path, 0o600)
            tmp_path.replace(self._store_path)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "[BCS] НЕ УДАЛОСЬ сохранить ротированный refresh-токен в %s: %s. "
                "Если процесс перезапустится, токен придётся выпускать заново.",
                self._store_path,
                exc,
            )

    # --- публичное API -------------------------------------------------

    def _ensure_lock(self) -> asyncio.Lock:
        """Создать Lock лениво — вне работающего event loop его делать нельзя."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    @property
    def has_valid_token(self) -> bool:
        return bool(self._access_token) and time.time() < self._expires_at

    async def get_access_token(self, client: httpx.AsyncClient) -> str:
        """Вернуть действующий access-токен, обновив его при необходимости."""
        if self.has_valid_token:
            return self._access_token
        async with self._ensure_lock():
            # Пока ждали лок, токен мог обновить другой вызов.
            if self.has_valid_token:
                return self._access_token
            return await self._refresh(client)

    async def force_refresh(self, client: httpx.AsyncClient) -> str:
        """Принудительно обновить токен (вызывается при 401)."""
        async with self._ensure_lock():
            self._expires_at = 0.0
            return await self._refresh(client)

    def invalidate(self) -> None:
        """Пометить текущий access-токен протухшим."""
        self._expires_at = 0.0

    # --- внутреннее ----------------------------------------------------

    async def _request_token(self, client: httpx.AsyncClient) -> Dict[str, Any]:
        """Один запрос к Keycloak за парой токенов."""
        try:
            response = await fetch_with_retry(
                client,
                "POST",
                f"{self._api_base}{TOKEN_PATH}",
                data={
                    "client_id": self.client_id,
                    "refresh_token": self._refresh_token,
                    "grant_type": "refresh_token",
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
            )
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            body = exc.response.text[:500]
            if status in (400, 401, 403):
                raise BcsAuthError(
                    f"БКС отклонил refresh-токен {self.client_id} "
                    f"(HTTP {status}). Скорее всего он истёк или удалён — "
                    f"выпустите новый в веб-версии. Ответ: {body}",
                    status_code=status,
                ) from exc
            raise BcsUnavailableError(
                f"Сервис авторизации БКС недоступен (HTTP {status}): {body}",
                status_code=status,
            ) from exc
        except httpx.HTTPError as exc:
            raise BcsUnavailableError(
                f"Сетевая ошибка при авторизации в БКС: {exc}"
            ) from exc

        return response.json()

    async def _refresh(self, client: httpx.AsyncClient) -> str:
        if not self._refresh_token:
            raise BcsAuthError(
                f"Нет refresh-токена для {self.client_id}. "
                "Выпустите его в веб-версии БКС и положите в .env."
            )

        try:
            data = await self._request_token(client)
        except BcsAuthError:
            # Токен отвергнут. Возможно, его ротировал другой процесс
            # (uvicorn --reload поднимает воркеры заново, и старый может
            # успеть обновиться первым). Перечитываем store: если там
            # лежит более свежий токен — пробуем ещё раз, прежде чем
            # объявлять 90-дневный токен потерянным.
            stored = self._load_stored_refresh()
            if not stored or stored == self._refresh_token:
                raise
            logger.warning(
                "[BCS] refresh-токен %s отвергнут, но в %s есть более свежий — повторяю",
                self.client_id,
                self._store_path,
            )
            self._refresh_token = stored
            data = await self._request_token(client)

        access_token = data.get("access_token")
        if not access_token:
            raise BcsAuthError(
                f"В ответе БКС нет access_token: {str(data)[:300]}"
            )

        try:
            expires_in = float(data.get("expires_in") or 0)
        except (TypeError, ValueError):
            expires_in = 0.0
        if expires_in <= 0:
            # Документация обещает 24 часа; при отсутствии поля берём час.
            expires_in = 3600.0

        self._access_token = str(access_token)
        self._expires_at = time.time() + max(
            60.0, expires_in - settings.bcs_token_refresh_margin_sec
        )

        new_refresh = data.get("refresh_token")
        if new_refresh and new_refresh != self._refresh_token:
            logger.warning(
                "[BCS] refresh-токен %s ротирован брокером (%s → %s), сохраняю в %s",
                self.client_id,
                _mask(self._refresh_token),
                _mask(str(new_refresh)),
                self._store_path,
            )
            self._refresh_token = str(new_refresh)
            self._persist_refresh(self._refresh_token, data.get("refresh_expires_in"))
        elif new_refresh:
            # Токен тот же — всё равно фиксируем, чтобы store не отставал от .env.
            self._persist_refresh(self._refresh_token, data.get("refresh_expires_in"))

        logger.info(
            "[BCS] access-токен %s получен: %s, действует %.0f сек (scope=%s)",
            self.client_id,
            _mask(self._access_token),
            expires_in,
            data.get("scope", "?"),
        )
        return self._access_token


_read_manager: Optional[BcsTokenManager] = None
_write_manager: Optional[BcsTokenManager] = None


def get_read_tokens() -> BcsTokenManager:
    """Синглтон менеджера токенов с правами только на чтение."""
    global _read_manager
    if _read_manager is None:
        _read_manager = BcsTokenManager(
            settings.bcs_refresh_token_read, CLIENT_ID_READ
        )
    return _read_manager


def get_write_tokens() -> BcsTokenManager:
    """Синглтон менеджера токенов с правами на торговлю."""
    global _write_manager
    if _write_manager is None:
        if not settings.bcs_refresh_token_write:
            raise BcsAuthError(
                "BCS_REFRESH_TOKEN_WRITE не задан — торговые заявки отправлять нечем."
            )
        _write_manager = BcsTokenManager(
            settings.bcs_refresh_token_write, CLIENT_ID_WRITE
        )
    return _write_manager


def reset_token_managers() -> None:
    """Сбросить синглтоны (используется в тестах и при перезапуске движка)."""
    global _read_manager, _write_manager
    _read_manager = None
    _write_manager = None
