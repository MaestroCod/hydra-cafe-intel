"""Camada de persistencia relacional (PostgreSQL/PostGIS com fallback SQLite).

Modulos:
    models -> tabelas ORM (SQLAlchemy): DimPolo, FactClimaSemanalCafe,
              FactCotacoesCafeSaca
    sync   -> sincronizacao idempotente (UPSERT) da camada Gold para o banco

O banco default e o RDS PostGIS via `settings.postgres_dsn`; quando indisponivel,
o `sync` cai automaticamente para `sqlite:///data_lake/hydra.db` (uso local/demo).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

if TYPE_CHECKING:  # pragma: no cover - apenas para type checkers
    from sqlalchemy.engine import Engine  # noqa: F401

    from src.database.models import (  # noqa: F401
        Base,
        DimPolo,
        FactClimaSemanalCafe,
        FactCotacoesCafeSaca,
        create_engine_from_settings,
    )
    from src.database.sync import (  # noqa: F401
        sync_gold_to_db,
        sync_weekly_gold_to_db,
    )

_EXPORTS: Final[dict[str, tuple[str, str]]] = {
    "Base": ("src.database.models", "Base"),
    "DimPolo": ("src.database.models", "DimPolo"),
    "FactClimaSemanalCafe": ("src.database.models", "FactClimaSemanalCafe"),
    "FactCotacoesCafeSaca": ("src.database.models", "FactCotacoesCafeSaca"),
    "create_engine_from_settings": (
        "src.database.models",
        "create_engine_from_settings",
    ),
    "sync_gold_to_db": ("src.database.sync", "sync_gold_to_db"),
    "sync_weekly_gold_to_db": ("src.database.sync", "sync_weekly_gold_to_db"),
}

__all__ = list(_EXPORTS)


def __getattr__(name: str) -> Any:
    """Importa o modulo do banco somente quando o simbolo e acessado."""
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    from importlib import import_module

    module_name, attribute = target
    return getattr(import_module(module_name), attribute)


def __dir__() -> list[str]:
    """Lista os simbolos publicos."""
    return list(__all__)
