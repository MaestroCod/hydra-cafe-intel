"""Ingestores das fontes externas do projeto (camada raw do Data Lake).

Modulos:
    finance -> cotacoes de commodities e cambio via Yahoo Finance (yfinance)
    chirps  -> precipitacao diaria CHIRPS (GeoTIFF recortado para o Brasil)
    era5    -> reanalise horaria ERA5-Land (NetCDF recortado no CDS)
    retry   -> utilitario de retentativas com backoff exponencial

Os simbolos publicos sao expostos de forma preguicosa (PEP 562) para que
`python -m src.ingestion.<modulo>` nao importe o modulo duas vezes e para que
importar o pacote nao carregue rasterio/xarray sem necessidade.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:  # pragma: no cover - apenas para type checkers
    from src.ingestion.chirps import (
        ChirpsIngestionError,
        ChirpsIngestionResult,
        ChirpsIngestionRun,
        ChirpsNotAvailableError,
        RasterStats,
    )
    from src.ingestion.chirps import ingest_date as ingest_chirps_date
    from src.ingestion.chirps import ingest_dates as ingest_chirps_dates
    from src.ingestion.era5 import (
        Era5IngestionError,
        Era5IngestionResult,
        Era5IngestionRun,
        NetCdfStats,
        resolve_variables,
    )
    from src.ingestion.era5 import ingest_date as ingest_era5_date
    from src.ingestion.era5 import ingest_dates as ingest_era5_dates
    from src.ingestion.finance import (
        FinanceIngestionError,
        IngestionResult,
        IngestionRun,
        ingest_ticker,
        ingest_tickers,
    )
    from src.ingestion.retry import retry_call

#: Mapeia cada simbolo publico ao modulo que o define (import preguicoso).
_EXPORTS: Final[dict[str, tuple[str, str]]] = {
    # Financeiro
    "FinanceIngestionError": ("src.ingestion.finance", "FinanceIngestionError"),
    "IngestionResult": ("src.ingestion.finance", "IngestionResult"),
    "IngestionRun": ("src.ingestion.finance", "IngestionRun"),
    "ingest_ticker": ("src.ingestion.finance", "ingest_ticker"),
    "ingest_tickers": ("src.ingestion.finance", "ingest_tickers"),
    # CHIRPS
    "ChirpsIngestionError": ("src.ingestion.chirps", "ChirpsIngestionError"),
    "ChirpsNotAvailableError": ("src.ingestion.chirps", "ChirpsNotAvailableError"),
    "ChirpsIngestionResult": ("src.ingestion.chirps", "ChirpsIngestionResult"),
    "ChirpsIngestionRun": ("src.ingestion.chirps", "ChirpsIngestionRun"),
    "RasterStats": ("src.ingestion.chirps", "RasterStats"),
    "ingest_chirps_date": ("src.ingestion.chirps", "ingest_date"),
    "ingest_chirps_dates": ("src.ingestion.chirps", "ingest_dates"),
    # ERA5-Land
    "Era5IngestionError": ("src.ingestion.era5", "Era5IngestionError"),
    "Era5IngestionResult": ("src.ingestion.era5", "Era5IngestionResult"),
    "Era5IngestionRun": ("src.ingestion.era5", "Era5IngestionRun"),
    "NetCdfStats": ("src.ingestion.era5", "NetCdfStats"),
    "resolve_variables": ("src.ingestion.era5", "resolve_variables"),
    "ingest_era5_date": ("src.ingestion.era5", "ingest_date"),
    "ingest_era5_dates": ("src.ingestion.era5", "ingest_dates"),
    # Infra comum
    "retry_call": ("src.ingestion.retry", "retry_call"),
}

__all__ = [
    "ChirpsIngestionError",
    "ChirpsIngestionResult",
    "ChirpsIngestionRun",
    "ChirpsNotAvailableError",
    "Era5IngestionError",
    "Era5IngestionResult",
    "Era5IngestionRun",
    "FinanceIngestionError",
    "IngestionResult",
    "IngestionRun",
    "NetCdfStats",
    "RasterStats",
    "ingest_chirps_date",
    "ingest_chirps_dates",
    "ingest_era5_date",
    "ingest_era5_dates",
    "ingest_ticker",
    "ingest_tickers",
    "resolve_variables",
    "retry_call",
]


def __getattr__(name: str) -> Any:
    """Importa o modulo do ingestor somente quando o simbolo e acessado."""
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    from importlib import import_module

    module_name, attribute = target
    return getattr(import_module(module_name), attribute)


def __dir__() -> list[str]:
    """Lista os simbolos publicos para autocomplete em REPL/notebooks."""
    return list(__all__)

