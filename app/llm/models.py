"""Pydantic models for structured LLM risk evaluation responses."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field


class RBreakdown(BaseModel):
    R_geo: int = Field(
        description=(
            "0=не РФ, 2=РФ косвенно, 4=РФ прямо, 6=МосБиржа/рынок акций, "
            "8=Банковский/финансовый сектор РФ, 10=Эмитент Татнефть напрямую"
        )
    )
    R_asset: int = Field(
        description=(
            "+0=не упоминается конкретный эмитент, "
            "+2=упоминается тикер TATN, TATNEFT или название Татнефть"
        )
    )


class RFactor(BaseModel):
    value: int = Field(description="R = R_geo + R_asset (максимум 10)")
    breakdown: RBreakdown


class SBreakdown(BaseModel):
    S_macro: Optional[int] = Field(
        default=None,
        description="ВВП, инфляция, безработица, курс рубля, рецессия [0-10] или null",
    )
    S_monetary: Optional[int] = Field(
        default=None,
        description="Ключевая ставка ЦБ, валютные ограничения, QE/QT [0-10] или null",
    )
    S_regulatory: Optional[int] = Field(
        default=None,
        description="Санкции, законы, лицензии, запреты, регуляторные решения [0-10] или null",
    )
    S_sector: Optional[int] = Field(
        default=None,
        description="Прибыль/убыток эмитента, NPL, дефолт, капитализация [0-10] или null",
    )


class SFactor(BaseModel):
    value: int = Field(
        description="MAX(всех упомянутых блоков), либо 0, если ничего явно не упомянуто"
    )
    breakdown: SBreakdown


class MFactor(BaseModel):
    value: int = Field(
        description=(
            "Масштаб [0–10]: 2=Одна компания, 4=Сектор, 6=Рынок/МосБиржа, "
            "8=Макроэкономика РФ, 10=Глобальный/геополитический"
        )
    )


class UFactor(BaseModel):
    value: int = Field(
        description=(
            "Неожиданность [0–10]: 1=Плановое/календарь, 3=Ожидаемо/аналитики, "
            "6=Сюрприз/превысил прогнозы, 10=Абсолютный шок/черный лебедь"
        )
    )


class CFactor(BaseModel):
    value: float = Field(
        description=(
            "Достоверность источника [0.1–1.0]: 1.0=Регулятор/эмитент, "
            "0.8=Tier-1 СМИ с подтверждением, 0.6=Tier-1 со ссылкой, "
            "0.4=Tier-2/TG/агрегатор, 0.2=Слух, 0.1=Фейк/спам"
        )
    )


class Factors(BaseModel):
    R: RFactor
    S: SFactor
    M: MFactor
    U: UFactor
    C: CFactor


class NewsRiskDecision(BaseModel):
    decision: bool = Field(
        description=(
            "true если RiskScore_final < 6.0 (торговля разрешена), "
            "false если RiskScore_final >= 6.0 или tier0=true (торговля запрещена)"
        )
    )
    tier0: bool = Field(
        description=(
            "true при наличии триггеров Tier-0 "
            "(приостановка торгов, санкции, военное положение, дефолт, сбой системы)"
        )
    )
    score: Optional[float] = Field(
        default=None,
        description="RiskScore_final (число 0.00-10.00) или null, если tier0=true",
    )
    factors: Factors
    reason: str = Field(description="Краткое обоснование на русском (1-2 предложения)")
