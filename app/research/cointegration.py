"""Проверка пары на пригодность к mean-reversion торговле.

Отвечает на вопрос, который бэктест v2 обходил стороной: а спред вообще
стационарен? Окно 2500 и вход z=2.5 были взяты «на глаз» по полугодовому
периоду; без теста на коинтеграцию нет оснований считать, что расхождение
спреда вообще обязано схлопываться.

Три инструмента:

* ``adf_test`` — расширенный тест Дики-Фуллера на стационарность спреда;
* ``engle_granger`` — двухшаговый тест на коинтеграцию с оценкой хедж-коэффициента;
* ``half_life`` — период полураспада отклонения, из которого следует
  осмысленный размер скользящего окна.
"""

from __future__ import annotations

import math
from typing import Any, Dict, Optional, Sequence

import numpy as np
import pandas as pd
from statsmodels.regression.linear_model import OLS
from statsmodels.tools.tools import add_constant
from statsmodels.tsa.stattools import adfuller, coint

# Порог значимости по умолчанию: p-value ниже — отвергаем гипотезу
# о единичном корне, то есть считаем ряд стационарным.
DEFAULT_ALPHA = 0.05

# Потолок числа лагов. По умолчанию statsmodels берёт maxlag ≈ 12·(n/100)^¼,
# что на 130 000 минутных баров даёт ~70 лагов, и с autolag="AIC" тест
# перебирает все 70 моделей на полном ряде — это десятки минут. На
# внутридневных данных такая глубина не нужна: автокорреляция спреда
# исчерпывается несколькими барами.
DEFAULT_MAX_LAG = 24

# Размер ряда, начиная с которого включается прореживание.
LARGE_SERIES = 20_000


def _clean(series: Sequence[float]) -> pd.Series:
    values = pd.Series(list(series), dtype=float)
    return values.replace([np.inf, -np.inf], np.nan).dropna()


def _downsample(values: pd.Series, limit: int = LARGE_SERIES) -> pd.Series:
    """Проредить очень длинный ряд равномерным шагом.

    Тесты на единичный корень на сотнях тысяч точек считаются долго, а
    выводы от прореживания не меняются: стационарность — свойство процесса,
    а не частоты выборки. Шаг равномерный, чтобы не сместить оценку.
    """
    if len(values) <= limit:
        return values
    step = len(values) // limit + 1
    return values.iloc[::step].reset_index(drop=True)


def adf_test(
    series: Sequence[float],
    *,
    alpha: float = DEFAULT_ALPHA,
    maxlag: Optional[int] = DEFAULT_MAX_LAG,
) -> Dict[str, Any]:
    """Расширенный тест Дики-Фуллера.

    Нулевая гипотеза: у ряда есть единичный корень (нестационарен).
    ``is_stationary=True`` означает, что нулевую гипотезу отвергли —
    спред возвращается к среднему.
    """
    values = _clean(series)
    if len(values) < 20:
        return {"error": f"недостаточно данных: {len(values)}"}

    original_size = len(values)
    values = _downsample(values)

    stat, pvalue, used_lag, n_obs, crit, _icbest = adfuller(
        values, maxlag=maxlag, autolag="AIC"
    )
    result = {
        "statistic": round(float(stat), 4),
        "pvalue": round(float(pvalue), 6),
        "used_lag": int(used_lag),
        "n_obs": int(n_obs),
        "critical_values": {key: round(float(v), 4) for key, v in crit.items()},
        "is_stationary": bool(pvalue < alpha),
        "alpha": alpha,
    }
    if len(values) < original_size:
        result["downsampled_from"] = original_size
    return result


def hedge_ratio(prices_a: Sequence[float], prices_b: Sequence[float]) -> Dict[str, Any]:
    """OLS-регрессия A на B: сколько единиц B хеджируют единицу A.

    Стратегия в её нынешнем виде торгует спред ``log(A/B)``, что
    эквивалентно фиксированному коэффициенту 1. Если оценка заметно
    отличается от единицы, ноги стоит взвешивать — иначе позиция
    получается направленной, а не рыночно-нейтральной.
    """
    a = _clean(prices_a)
    b = _clean(prices_b)
    size = min(len(a), len(b))
    if size < 20:
        return {"error": f"недостаточно данных: {size}"}

    a, b = a.iloc[:size].reset_index(drop=True), b.iloc[:size].reset_index(drop=True)
    model = OLS(np.log(a), add_constant(np.log(b))).fit()
    beta = float(model.params.iloc[1])
    return {
        "beta": round(beta, 6),
        "intercept": round(float(model.params.iloc[0]), 6),
        "r_squared": round(float(model.rsquared), 4),
        "residuals": (np.log(a) - model.params.iloc[0] - beta * np.log(b)).tolist(),
    }


def engle_granger(
    prices_a: Sequence[float],
    prices_b: Sequence[float],
    *,
    alpha: float = DEFAULT_ALPHA,
) -> Dict[str, Any]:
    """Двухшаговый тест Энгла-Грейнджера на коинтеграцию.

    Нулевая гипотеза: коинтеграции нет. ``is_cointegrated=True`` —
    ряды связаны долгосрочным равновесием, и торговать спред осмысленно.
    """
    a = _clean(prices_a)
    b = _clean(prices_b)
    size = min(len(a), len(b))
    if size < 50:
        return {"error": f"недостаточно данных: {size}"}

    a, b = a.iloc[:size].reset_index(drop=True), b.iloc[:size].reset_index(drop=True)
    original_size = size
    a, b = _downsample(a), _downsample(b)
    # maxlag ограничен по той же причине, что и в adf_test.
    stat, pvalue, crit = coint(
        np.log(a), np.log(b), maxlag=DEFAULT_MAX_LAG, autolag="AIC"
    )

    result = {
        "statistic": round(float(stat), 4),
        "pvalue": round(float(pvalue), 6),
        "critical_values": {
            "1%": round(float(crit[0]), 4),
            "5%": round(float(crit[1]), 4),
            "10%": round(float(crit[2]), 4),
        },
        "is_cointegrated": bool(pvalue < alpha),
        "alpha": alpha,
    }
    if len(a) < original_size:
        result["downsampled_from"] = original_size
    hedge = hedge_ratio(a, b)
    if "beta" in hedge:
        result["hedge_beta"] = hedge["beta"]
        result["hedge_r_squared"] = hedge["r_squared"]
    return result


def half_life(spread: Sequence[float]) -> Optional[float]:
    """Период полураспада отклонения спреда, в барах.

    Оценивается регрессией Орнштейна-Уленбека в дискретной форме:
    ``Δs_t = λ · s_{t-1} + c``, откуда ``half_life = −ln(2) / λ``.
    Возвращает None, если процесс не возвращается к среднему (λ >= 0).
    """
    values = _clean(spread)
    if len(values) < 30:
        return None

    lagged = values.shift(1).dropna()
    delta = values.diff().dropna()
    size = min(len(lagged), len(delta))
    lagged, delta = lagged.iloc[-size:], delta.iloc[-size:]

    model = OLS(delta.values, add_constant(lagged.values)).fit()
    lam = float(model.params[1])
    if lam >= 0:
        return None
    return float(-math.log(2) / lam)


def assess_pair(
    prices_a: Sequence[float],
    prices_b: Sequence[float],
    *,
    alpha: float = DEFAULT_ALPHA,
) -> Dict[str, Any]:
    """Полная оценка пары: коинтеграция, стационарность спреда, half-life.

    В ``recommended_window`` возвращается размер скользящего окна, который
    следует из half-life (примерно 2–4 периода полураспада). Это и есть
    ответ на вопрос, откуда должно браться число вместо магического 2500.
    """
    a = _clean(prices_a)
    b = _clean(prices_b)
    size = min(len(a), len(b))
    if size < 50:
        return {"error": f"недостаточно данных: {size}"}

    a, b = a.iloc[:size].reset_index(drop=True), b.iloc[:size].reset_index(drop=True)
    spread = np.log(a / b)

    hl = half_life(spread)
    coint_result = engle_granger(a, b, alpha=alpha)
    adf_result = adf_test(spread, alpha=alpha)

    tradable = bool(
        coint_result.get("is_cointegrated")
        and adf_result.get("is_stationary")
        and hl is not None
    )

    assessment: Dict[str, Any] = {
        "n_bars": int(size),
        "spread_mean": round(float(spread.mean()), 6),
        "spread_std": round(float(spread.std(ddof=1)), 6),
        "half_life_bars": round(hl, 1) if hl is not None else None,
        "engle_granger": coint_result,
        "adf_spread": adf_result,
        "is_tradable": tradable,
    }

    if hl is not None:
        assessment["recommended_window"] = {
            "min": int(hl * 2),
            "max": int(hl * 4),
            "comment": (
                "Окно должно покрывать 2–4 периода полураспада: короче — "
                "среднее гоняется за шумом, длиннее — не поспевает за сдвигом режима"
            ),
        }

    assessment["verdict"] = _verdict(assessment)
    return assessment


def _verdict(assessment: Dict[str, Any]) -> str:
    """Короткий вывод на русском для отчёта."""
    eg = assessment.get("engle_granger") or {}
    adf = assessment.get("adf_spread") or {}

    if not eg.get("is_cointegrated"):
        return (
            f"Коинтеграция НЕ подтверждена (p={eg.get('pvalue')}). "
            "Торговать спред как mean-reversion нельзя."
        )
    if not adf.get("is_stationary"):
        return (
            f"Коинтеграция есть, но спред не прошёл ADF (p={adf.get('pvalue')}). "
            "Параметры ненадёжны."
        )
    hl = assessment.get("half_life_bars")
    window = assessment.get("recommended_window") or {}
    return (
        f"Пара коинтегрирована (p={eg.get('pvalue')}), спред стационарен "
        f"(ADF p={adf.get('pvalue')}), half-life ≈ {hl} баров. "
        f"Разумное окно: {window.get('min')}–{window.get('max')} баров."
    )


def format_assessment(assessment: Dict[str, Any], title: str = "Оценка пары") -> str:
    """Человекочитаемый отчёт."""
    if "error" in assessment:
        return f"{title}: {assessment['error']}"

    eg = assessment["engle_granger"]
    adf = assessment["adf_spread"]
    lines = [
        title,
        "─" * 46,
        f"Баров:              {assessment['n_bars']}",
        f"Спред: mean={assessment['spread_mean']}  std={assessment['spread_std']}",
        "",
        f"Engle-Granger:      stat={eg.get('statistic')}  p={eg.get('pvalue')}  "
        f"→ {'коинтегрированы' if eg.get('is_cointegrated') else 'НЕ коинтегрированы'}",
        f"Хедж-коэффициент:   beta={eg.get('hedge_beta')}  R²={eg.get('hedge_r_squared')}",
        f"ADF по спреду:      stat={adf.get('statistic')}  p={adf.get('pvalue')}  "
        f"→ {'стационарен' if adf.get('is_stationary') else 'НЕ стационарен'}",
        f"Half-life:          {assessment.get('half_life_bars')} баров",
    ]
    window = assessment.get("recommended_window")
    if window:
        lines.append(f"Рекомендуемое окно: {window['min']}–{window['max']} баров")
    lines += ["", f"Вывод: {assessment['verdict']}"]
    return "\n".join(lines)
