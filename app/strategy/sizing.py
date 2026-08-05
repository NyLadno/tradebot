"""Расчёт объёма ног и округление цен под требования биржи.

``lot_size`` / ``minimum_step`` / ``scale`` берутся из
``/instruments/by-tickers`` при старте движка и никогда не хардкодятся:
у разных инструментов они разные, а заявка с некорректным шагом цены
будет отклонена биржей.
"""

from __future__ import annotations

import math
from typing import Optional

from app.broker.models import Instrument


def compute_leg_quantity(
    price: float, position_size_rub: float, lot_size: int = 1
) -> int:
    """Сколько штук инструмента влезает в заданный размер позиции.

    Округление вниз до целого числа лотов: заявка на нецелое число лотов
    будет отклонена, а превышение размера позиции нам не нужно.
    Возвращает 0, если на позицию не хватает даже одного лота.
    """
    if price <= 0 or position_size_rub <= 0:
        return 0
    lot_size = max(1, int(lot_size))
    lots = math.floor(position_size_rub / (price * lot_size))
    return int(lots * lot_size)


def round_to_step(
    price: float, minimum_step: float, scale: Optional[int] = None
) -> float:
    """Округлить цену к ближайшему шагу цены инструмента."""
    if price <= 0:
        return 0.0
    if minimum_step and minimum_step > 0:
        price = round(price / minimum_step) * minimum_step
    if scale is not None and scale >= 0:
        price = round(price, int(scale))
    return price


def protective_limit_price(
    reference_price: float,
    *,
    is_buy: bool,
    instrument: Optional[Instrument],
    slippage_steps: int = 3,
) -> float:
    """Цена лимитной заявки с запасом в несколько шагов цены.

    Мы не шлём рыночные заявки на вход: в неликвидном стакане рыночная
    заявка съедает несколько уровней. Вместо этого ставим лимитную «с
    проскальзыванием» — на покупку чуть выше ask, на продажу чуть ниже bid.
    Заявка исполняется почти как рыночная, но с гарантированным потолком.
    """
    step = instrument.minimum_step if instrument else 0.0
    scale = instrument.scale if instrument else None
    offset = step * max(0, slippage_steps)
    price = reference_price + offset if is_buy else reference_price - offset
    return round_to_step(max(price, step or 0.01), step, scale)


def notional(quantity: int, price: float) -> float:
    """Стоимость ноги в рублях."""
    return abs(quantity) * price


def estimate_commission(quantity: int, price: float, commission_pct: float) -> float:
    """Комиссия за одну сделку по одной ноге."""
    return notional(quantity, price) * max(0.0, commission_pct)
