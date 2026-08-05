"""Ошибки БКС Trade API.

Документация описывает единый набор текстовых кодов для всех сервисов:
UNAUTHORIZED (401), NOT_FOUND (404), VALIDATION_ERROR / BAD_REQUEST (400),
INTERNAL_SERVER_ERROR (5xx).
"""

from __future__ import annotations

from typing import Any, Dict, Optional


class BcsError(Exception):
    """Базовая ошибка при работе с БКС Trade API."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        error_code: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.payload = payload or {}


class BcsAuthError(BcsError):
    """401 UNAUTHORIZED: токен невалиден или протух и обновление не помогло."""


class BcsValidationError(BcsError):
    """400 VALIDATION_ERROR / BAD_REQUEST: запрос отвергнут брокером."""


class BcsNotFoundError(BcsError):
    """404 NOT_FOUND: данные не найдены (например, неизвестный clientOrderId)."""


class BcsUnavailableError(BcsError):
    """5xx или сетевая недоступность после исчерпания ретраев."""


class BcsStreamError(BcsError):
    """Ошибка внутри WebSocket-потока (коды 0 Undefined, 1 NoData, 2 NotFound, 3 InvalidJson)."""
