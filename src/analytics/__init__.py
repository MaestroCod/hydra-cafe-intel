"""Analytics - Camada Gold (inteligencia de negocio sobre o Data Lake).

Modulos:
    gold -> visao consolidada de estresse hidrico x mercado (correlacoes)

Simbolos expostos via import preguicoso (PEP 562).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:  # pragma: no cover - apenas para type checkers
    from src.analytics.gold import (
        COMMODITY_TO_TICKERS,
        CorrelationResult,
        GoldError,
        GoldResult,
        build_gold_analytics,
        build_gold_weekly_analytics,
        compute_correlations,
    )

_EXPORTS: Final[dict[str, tuple[str, str]]] = {
    "COMMODITY_TO_TICKERS": ("src.analytics.gold", "COMMODITY_TO_TICKERS"),
    "CorrelationResult": ("src.analytics.gold", "CorrelationResult"),
    "GoldError": ("src.analytics.gold", "GoldError"),
    "GoldResult": ("src.analytics.gold", "GoldResult"),
    "aggregate_weekly": ("src.analytics.gold", "aggregate_weekly"),
    "build_gold_analytics": ("src.analytics.gold", "build_gold_analytics"),
    "build_gold_weekly_analytics": ("src.analytics.gold", "build_gold_weekly_analytics"),
    "compute_correlations": ("src.analytics.gold", "compute_correlations"),
}

__all__ = [
    "COMMODITY_TO_TICKERS",
    "CorrelationResult",
    "GoldError",
    "GoldResult",
    "aggregate_weekly",
    "build_gold_analytics",
    "build_gold_weekly_analytics",
    "compute_correlations",
]


def __getattr__(name: str) -> Any:
    """Importa o modulo da Gold somente quando o simbolo e acessado."""
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    module_name, attribute = target
    return getattr(import_module(module_name), attribute)


def __dir__() -> list[str]:
    """Lista os simbolos publicos."""
    return list(__all__)
