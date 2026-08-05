"""Shared retry helpers with exponential backoff.

Uses ``tenacity`` when available; falls back to a tiny internal implementation
so the code still works if the dependency is missing.
"""

from __future__ import annotations

import functools
import logging
from typing import Any, Callable, Coroutine, TypeVar

import httpx

try:
    from tenacity import retry as tenacity_retry
    from tenacity import retry_if_exception, stop_after_attempt, wait_exponential

    HAS_TENACITY = True
except ImportError:  # pragma: no cover
    HAS_TENACITY = False

T = TypeVar("T")
logger = logging.getLogger("tradebot.retry")


def _is_retryable(exc: BaseException) -> bool:
    """Return True for transient network/HTTP errors that are worth retrying."""
    if isinstance(exc, httpx.HTTPStatusError):
        code = exc.response.status_code
        return code >= 500 or code == 429
    return isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.ReadError,
            httpx.WriteError,
            httpx.RemoteProtocolError,
        ),
    )


def async_retry(
    max_attempts: int = 3,
    wait_min: float = 1.0,
    wait_max: float = 10.0,
) -> Callable[[Callable[..., Coroutine[Any, Any, T]]], Callable[..., Coroutine[Any, Any, T]]]:
    """Decorator that retries an async coroutine with exponential backoff."""

    def decorator(
        coro: Callable[..., Coroutine[Any, Any, T]]
    ) -> Callable[..., Coroutine[Any, Any, T]]:
        if HAS_TENACITY:
            return tenacity_retry(
                stop=stop_after_attempt(max_attempts),
                wait=wait_exponential(multiplier=1, min=wait_min, max=wait_max),
                retry=retry_if_exception(_is_retryable),
                reraise=True,
            )(coro)

        @functools.wraps(coro)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return await coro(*args, **kwargs)
                except Exception as exc:
                    last_exc = exc
                    if attempt >= max_attempts or not _is_retryable(exc):
                        raise
                    wait = min(wait_max, wait_min * (2 ** (attempt - 1)))
                    logger.warning(
                        "Retry attempt %s/%s after %.1fs: %s",
                        attempt,
                        max_attempts,
                        wait,
                        exc,
                    )
            raise last_exc  # pragma: no cover

        return wrapper

    return decorator


@async_retry()
async def fetch_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs: Any,
) -> httpx.Response:
    """Make an HTTP request and retry on transient failures."""
    resp = await client.request(method, url, **kwargs)
    resp.raise_for_status()
    return resp
