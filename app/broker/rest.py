"""HTTP-клиент БКС Trade API.

Все пути и имена полей — из официальной документации
(https://cdn.bcs.ru/static/bcs/files/trade-api-docs.pdf).
Сетевые ретраи (5xx / 429 / таймауты) берём из общего ``app.retry``;
здесь добавляется только однократное обновление токена при 401.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

import httpx

from app.broker.auth import BcsTokenManager, get_read_tokens, get_write_tokens
from app.broker.errors import (
    BcsAuthError,
    BcsError,
    BcsNotFoundError,
    BcsUnavailableError,
    BcsValidationError,
)
from app.broker.models import (
    Bar,
    Instrument,
    OrderState,
    OrderType,
    Side,
    format_bcs_datetime,
)
from app.config import settings
from app.logging_setup import get_logger
from app.retry import fetch_with_retry

logger = get_logger("tradebot.bcs.rest")

# --- пути сервисов ------------------------------------------------------
MARKET_DATA = "/trade-api-market-data-connector/api/v1"
OPERATIONS = "/trade-api-bff-operations/api/v1"
INFORMATION = "/trade-api-information-service/api/v1"
PORTFOLIO = "/trade-api-bff-portfolio/api/v1"
LIMITS = "/trade-api-bff-limit/api/v1"
MARGINAL = "/trade-api-bff-marginal-indicators/api/v1"

# Один запрос /candles-chart на слишком широкий диапазон брокер может обрезать,
# поэтому историю набираем окнами.
_CANDLE_WINDOW_DAYS = 3
_CANDLE_MAX_WINDOWS = 40


def new_client_order_id() -> str:
    """Сгенерировать clientOrderId. БКС требует UUID и использует его для идемпотентности."""
    return str(uuid.uuid4())


class BcsRestClient:
    """Тонкая обёртка над REST-эндпоинтами БКС.

    ``token_manager`` — с правами на чтение; ``write_token_manager`` нужен
    только для методов работы с заявками и создаётся лениво.
    """

    def __init__(
        self,
        http_client: httpx.AsyncClient,
        *,
        token_manager: Optional[BcsTokenManager] = None,
        write_token_manager: Optional[BcsTokenManager] = None,
        api_base: Optional[str] = None,
    ) -> None:
        self._http = http_client
        self._tokens = token_manager or get_read_tokens()
        self._write_tokens = write_token_manager
        self._api_base = (api_base or settings.bcs_api_base).rstrip("/")

    @property
    def tokens(self) -> BcsTokenManager:
        """Менеджер токенов на чтение — им же авторизуются WebSocket-потоки."""
        return self._tokens

    def _write_manager(self) -> BcsTokenManager:
        if self._write_tokens is None:
            self._write_tokens = get_write_tokens()
        return self._write_tokens

    # --- транспорт -----------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        write: bool = False,
    ) -> Any:
        """Выполнить запрос с Bearer-токеном; при 401 — один повтор после обновления."""
        manager = self._write_manager() if write else self._tokens
        url = f"{self._api_base}{path}"

        for attempt in (1, 2):
            token = await manager.get_access_token(self._http)
            headers = {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
            }
            try:
                response = await fetch_with_retry(
                    self._http,
                    method,
                    url,
                    params=params,
                    json=json_body,
                    headers=headers,
                )
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status == 401 and attempt == 1:
                    logger.info("[BCS] 401 на %s — обновляю access-токен", path)
                    await manager.force_refresh(self._http)
                    continue
                raise self._map_error(exc, path) from exc
            except httpx.HTTPError as exc:
                raise BcsUnavailableError(
                    f"Сетевая ошибка при обращении к {path}: {exc}"
                ) from exc

            if not response.content:
                return {}
            try:
                return response.json()
            except ValueError as exc:
                raise BcsError(
                    f"БКС вернул не-JSON на {path}: {response.text[:300]}"
                ) from exc

        raise BcsAuthError(f"Не удалось авторизоваться для {path}")

    @staticmethod
    def _map_error(exc: httpx.HTTPStatusError, path: str) -> BcsError:
        """Перевести HTTP-ошибку в типизированное исключение."""
        status = exc.response.status_code
        body: Dict[str, Any] = {}
        try:
            parsed = exc.response.json()
            if isinstance(parsed, dict):
                body = parsed
        except ValueError:
            pass
        code = body.get("code") or body.get("error") or body.get("status")
        text = exc.response.text[:500]
        message = f"БКС {path} → HTTP {status} {code or ''}: {text}".strip()

        if status == 401:
            return BcsAuthError(message, status_code=status, error_code=code, payload=body)
        if status == 404:
            return BcsNotFoundError(message, status_code=status, error_code=code, payload=body)
        if status >= 500:
            return BcsUnavailableError(message, status_code=status, error_code=code, payload=body)
        return BcsValidationError(message, status_code=status, error_code=code, payload=body)

    # --- рыночные данные -----------------------------------------------

    async def get_candles(
        self,
        ticker: str,
        start: datetime,
        end: datetime,
        *,
        class_code: Optional[str] = None,
        timeframe: str = "M1",
    ) -> List[Bar]:
        """GET /candles-chart — исторические свечи за диапазон."""
        payload = await self._request(
            "GET",
            f"{MARKET_DATA}/candles-chart",
            params={
                "classCode": class_code or settings.bcs_class_code,
                "ticker": ticker,
                "startDate": format_bcs_datetime(start),
                "endDate": format_bcs_datetime(end),
                "timeFrame": timeframe,
            },
        )
        raw_bars = (payload or {}).get("bars") or []
        bars = [Bar.from_rest(ticker, raw) for raw in raw_bars]
        return [bar for bar in bars if bar is not None]

    async def get_recent_candles(
        self,
        ticker: str,
        count: int,
        *,
        end: Optional[datetime] = None,
        class_code: Optional[str] = None,
        timeframe: str = "M1",
    ) -> List[Bar]:
        """Набрать последние ``count`` свечей, шагая окнами назад от ``end``.

        Нужно для прогрева скользящего окна (2500 минутных баров — это около
        семи торговых дней, за один запрос брокер такой объём отдавать
        не обязан). Возвращает бары по возрастанию времени.
        """
        window_end = end or datetime.now(timezone.utc)
        collected: Dict[datetime, Bar] = {}

        for _ in range(_CANDLE_MAX_WINDOWS):
            window_start = window_end - timedelta(days=_CANDLE_WINDOW_DAYS)
            bars = await self.get_candles(
                ticker,
                window_start,
                window_end,
                class_code=class_code,
                timeframe=timeframe,
            )
            for bar in bars:
                collected[bar.time] = bar
            if len(collected) >= count:
                break
            window_end = window_start

        ordered = [collected[key] for key in sorted(collected)]
        if len(ordered) < count:
            logger.warning(
                "[BCS] Для %s набрано только %s свечей из %s запрошенных",
                ticker,
                len(ordered),
                count,
            )
        return ordered[-count:]

    # --- справочники ---------------------------------------------------

    async def get_instruments(self, tickers: Sequence[str]) -> Dict[str, Instrument]:
        """POST /instruments/by-tickers → словарь ticker → Instrument."""
        payload = await self._request(
            "POST",
            f"{INFORMATION}/instruments/by-tickers",
            json_body={"tickers": list(tickers)},
        )
        result: Dict[str, Instrument] = {}
        for raw in (payload or {}).get("instruments") or []:
            instrument = Instrument.from_response(raw)
            if instrument is not None:
                result[instrument.ticker] = instrument
        missing = [t for t in tickers if t not in result]
        if missing:
            logger.warning("[BCS] Инструменты не найдены: %s", ", ".join(missing))
        return result

    async def get_trading_status(
        self, class_code: Optional[str] = None
    ) -> Dict[str, Any]:
        """GET /trading-schedule/status — текущий статус торговой сессии площадки."""
        return await self._request(
            "GET",
            f"{INFORMATION}/trading-schedule/status",
            params={"classCode": class_code or settings.bcs_class_code},
        )

    async def get_daily_schedule(
        self, ticker: str, *, class_code: Optional[str] = None
    ) -> Dict[str, Any]:
        """GET /trading-schedule/daily-schedule — расписание сессий на сегодня."""
        return await self._request(
            "GET",
            f"{INFORMATION}/trading-schedule/daily-schedule",
            params={
                "classCode": class_code or settings.bcs_class_code,
                "ticker": ticker,
            },
        )

    # --- портфель ------------------------------------------------------

    async def get_portfolio(self) -> Dict[str, Any]:
        """GET /portfolio — позиции по брокерскому счёту."""
        return await self._request("GET", f"{PORTFOLIO}/portfolio")

    async def get_limits(self) -> Dict[str, Any]:
        """GET /limits — депо-, денежные и срочные лимиты."""
        return await self._request("GET", f"{LIMITS}/limits")

    async def get_discounts(self) -> List[Dict[str, Any]]:
        """GET /instruments-discounts — ставки дисконта лонг/шорт."""
        payload = await self._request("GET", f"{MARGINAL}/instruments-discounts")
        return payload if isinstance(payload, list) else []

    # --- заявки --------------------------------------------------------

    async def create_order(
        self,
        *,
        client_order_id: str,
        ticker: str,
        side: Side,
        order_type: OrderType,
        quantity: int,
        price: Optional[float] = None,
        class_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        """POST /orders — создать заявку.

        ``client_order_id`` передаётся снаружи: он и есть ключ идемпотентности,
        по нему потом запрашивается статус.
        """
        if quantity <= 0:
            raise BcsValidationError(f"orderQuantity должен быть > 0, получено {quantity}")
        body: Dict[str, Any] = {
            "clientOrderId": client_order_id,
            "side": int(side),
            "orderType": int(order_type),
            "orderQuantity": int(quantity),
            "ticker": ticker,
            "classCode": class_code or settings.bcs_class_code,
        }
        if order_type is OrderType.LIMIT:
            if price is None or price <= 0:
                raise BcsValidationError("Лимитная заявка требует price > 0")
            body["price"] = round(float(price), 8)

        logger.info(
            "[BCS] Заявка %s: %s %s x%s %s",
            client_order_id,
            side.name,
            ticker,
            quantity,
            f"@{body.get('price')}" if order_type is OrderType.LIMIT else "по рынку",
        )
        return await self._request(
            "POST", f"{OPERATIONS}/orders", json_body=body, write=True
        )

    async def modify_order(
        self,
        *,
        original_client_order_id: str,
        new_client_order_id: str,
        price: float,
        quantity: int,
        class_code: Optional[str] = None,
    ) -> Dict[str, Any]:
        """POST /orders/{originalClientOrderId} — изменить заявку.

        Технически это отмена старой и создание новой, поэтому нужен новый UUID.
        """
        return await self._request(
            "POST",
            f"{OPERATIONS}/orders/{original_client_order_id}",
            json_body={
                "clientOrderId": new_client_order_id,
                "price": round(float(price), 8),
                "orderQuantity": int(quantity),
                "classCode": class_code or settings.bcs_class_code,
            },
            write=True,
        )

    async def cancel_order(
        self,
        original_client_order_id: str,
        *,
        cancel_client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """POST /orders/{originalClientOrderId}/cancel — отменить заявку.

        Документация требует в теле новый UUID, отличный от отменяемого.
        """
        return await self._request(
            "POST",
            f"{OPERATIONS}/orders/{original_client_order_id}/cancel",
            json_body={"clientOrderId": cancel_client_order_id or new_client_order_id()},
            write=True,
        )

    async def get_order(self, original_client_order_id: str) -> OrderState:
        """GET /orders/{originalClientOrderId} — статус ранее созданной заявки."""
        payload = await self._request(
            "GET",
            f"{OPERATIONS}/orders/{original_client_order_id}",
            write=True,
        )
        state = OrderState.from_response(payload or {})
        if not state.client_order_id:
            state.client_order_id = original_client_order_id
        return state
