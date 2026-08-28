"""ETAPA 4 - Motor de geoprocessamento e transformacao (camada Silver).

Modulos:
    geometry          -> polos produtores de referencia (GeoDataFrame WGS84)
    zonal_stats       -> estatistica zonal de CHIRPS (GeoTIFF) e ERA5 (NetCDF)
    water_balance     -> Tmax/Tmin/Tmean, ETP e balanco hidrico FAO-56 simplificado
    finance_transform -> conversao das cotacoes para BRL/saca + retornos e volatilidade
    pipeline          -> orquestracao Raw -> Processed com particionamento Hive

Os simbolos publicos usam import preguicoso (PEP 562) para nao carregar
geopandas/rasterio/xarray quando apenas o pacote e importado.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:  # pragma: no cover - apenas para type checkers
    from src.processing.finance_transform import (
        CONVERSION_SPECS,
        ConversionSpec,
        FinanceTransformError,
        add_risk_metrics,
        apply_conversion,
        read_raw_finance,
        split_fx,
        transform_finance,
    )
    from src.processing.geometry import (
        POLO_DEFINITIONS,
        GeometryError,
        PoloDefinition,
        load_polos,
        polos_dataframe,
    )
    from src.processing.pipeline import (
        PipelineError,
        PipelineResult,
        run_climate_pipeline,
        run_finance_pipeline,
        run_pipeline,
    )
    from src.processing.water_balance import (
        WaterBalanceError,
        daily_from_hourly,
        hargreaves_et0,
        water_balance,
    )
    from src.processing.zonal_stats import (
        ZonalStatsError,
        chirps_zonal_stats,
        era5_zonal_hourly,
    )

#: Mapeia cada simbolo publico ao modulo que o define.
_EXPORTS: Final[dict[str, tuple[str, str]]] = {
    "GeometryError": ("src.processing.geometry", "GeometryError"),
    "PoloDefinition": ("src.processing.geometry", "PoloDefinition"),
    "POLO_DEFINITIONS": ("src.processing.geometry", "POLO_DEFINITIONS"),
    "load_polos": ("src.processing.geometry", "load_polos"),
    "polos_dataframe": ("src.processing.geometry", "polos_dataframe"),
    "ZonalStatsError": ("src.processing.zonal_stats", "ZonalStatsError"),
    "chirps_zonal_stats": ("src.processing.zonal_stats", "chirps_zonal_stats"),
    "era5_zonal_hourly": ("src.processing.zonal_stats", "era5_zonal_hourly"),
    "WaterBalanceError": ("src.processing.water_balance", "WaterBalanceError"),
    "daily_from_hourly": ("src.processing.water_balance", "daily_from_hourly"),
    "hargreaves_et0": ("src.processing.water_balance", "hargreaves_et0"),
    "water_balance": ("src.processing.water_balance", "water_balance"),
    "CONVERSION_SPECS": ("src.processing.finance_transform", "CONVERSION_SPECS"),
    "ConversionSpec": ("src.processing.finance_transform", "ConversionSpec"),
    "FinanceTransformError": (
        "src.processing.finance_transform",
        "FinanceTransformError",
    ),
    "add_risk_metrics": ("src.processing.finance_transform", "add_risk_metrics"),
    "apply_conversion": ("src.processing.finance_transform", "apply_conversion"),
    "read_raw_finance": ("src.processing.finance_transform", "read_raw_finance"),
    "split_fx": ("src.processing.finance_transform", "split_fx"),
    "transform_finance": ("src.processing.finance_transform", "transform_finance"),
    "PipelineError": ("src.processing.pipeline", "PipelineError"),
    "PipelineResult": ("src.processing.pipeline", "PipelineResult"),
    "run_climate_pipeline": ("src.processing.pipeline", "run_climate_pipeline"),
    "run_finance_pipeline": ("src.processing.pipeline", "run_finance_pipeline"),
    "run_pipeline": ("src.processing.pipeline", "run_pipeline"),
}

__all__ = [
    "CONVERSION_SPECS",
    "POLO_DEFINITIONS",
    "ConversionSpec",
    "FinanceTransformError",
    "GeometryError",
    "PipelineError",
    "PipelineResult",
    "PoloDefinition",
    "WaterBalanceError",
    "ZonalStatsError",
    "add_risk_metrics",
    "apply_conversion",
    "chirps_zonal_stats",
    "daily_from_hourly",
    "era5_zonal_hourly",
    "hargreaves_et0",
    "load_polos",
    "polos_dataframe",
    "read_raw_finance",
    "run_climate_pipeline",
    "run_finance_pipeline",
    "run_pipeline",
    "split_fx",
    "transform_finance",
    "water_balance",
]


def __getattr__(name: str) -> Any:
    """Importa o modulo de processamento apenas quando o simbolo e acessado."""
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from importlib import import_module

    module_name, attribute = target
    return getattr(import_module(module_name), attribute)


def __dir__() -> list[str]:
    """Lista os simbolos publicos para autocomplete."""
    return list(__all__)
