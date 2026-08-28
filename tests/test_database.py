"""Testes da Etapa 5: Gold semanal de cafe e persistencia relacional (SQLite).

Nenhuma dependencia externa: usa um Data Lake temporario (LocalStorage) e um
banco SQLite em tmp_path. O caminho do PostgreSQL/PostGIS (RDS) so e exercitado
via `enable_postgis` quando um banco real estiver disponivel.

Execucao:
    .\\.venv\\Scripts\\python.exe -m pytest tests -q
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Engine

from src.analytics import gold as gold_module
from src.analytics.gold import (
    GOLD_WEEKLY_FILENAME,
    WEEKLY_COMMODITY,
    WEEKLY_POLO,
    aggregate_weekly,
    build_gold_weekly_analytics,
)
from src.config import Settings
from src.database.models import Base, create_engine_from_settings
from src.database.sync import sync_weekly_gold_to_db
from src.storage import LocalStorage


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    """Settings isolado com lake temporario."""
    return Settings(data_lake_root=str(tmp_path / "lake"), aoi_name="brasil")


@pytest.fixture()
def storage(settings: Settings) -> LocalStorage:
    """Backend local apontando para o lake temporario."""
    return LocalStorage(root=settings.lake_root_path)


@pytest.fixture()
def sqlite_engine(tmp_path: Path) -> Engine:
    """Engine SQLite isolado para os testes de sincronizacao."""
    engine = sa.create_engine(f"sqlite:///{tmp_path / 'test.db'}", future=True)
    yield engine
    engine.dispose()


def test_gold_weekly_fallback_deterministico(
    settings: Settings, storage: LocalStorage
) -> None:
    """Sem dados processados, o fallback sintetico e reprodutivel (seed=42)."""
    df = build_gold_weekly_analytics(storage=storage, write_output=True)
    assert not df.empty
    assert df["data_semana"].nunique() == df.shape[0]
    assert (df["polo"] == WEEKLY_POLO).all()
    assert df["crop_stress_index"].ge(0).all()
    assert {"preco_brl_saca", "retorno_semanal_pct", "volatilidade_21d_anualizada"}.issubset(
        df.columns
    )

    df2 = build_gold_weekly_analytics(storage=storage, write_output=False)
    assert np.allclose(df["preco_brl_saca"], df2["preco_brl_saca"])
    assert storage.exists(f"gold/{GOLD_WEEKLY_FILENAME}")


def test_aggregate_weekly_sinal_e_agregacao() -> None:
    """CSI = max(-deficit, 0); soma semanal de precipitacao; alerta > 15 mm."""
    clima = pd.DataFrame(
        {
            "data": pd.to_datetime(["2022-12-26", "2022-12-27"]),  # mesma semana W-MON
            "polo": [WEEKLY_POLO] * 2,
            "commodity": [WEEKLY_COMMODITY] * 2,
            "precipitacao_mm": [5.0, 3.0],
            "et0_mm": [10.0, 10.0],
            "deficit_hidrico": [-6.0, -4.0],  # negativo = estresse
        }
    )
    fin = pd.DataFrame(
        {
            "data": pd.to_datetime(["2022-12-26"]),
            "ticker": ["KC=F"],
            "preco_brl_saca": [1000.0],
            "volatilidade": [0.2],
        }
    )

    df = aggregate_weekly(clima, fin)
    # Semana 2022-12-26 (W-SUN): precipitacao 8, deficit -10 -> CSI 10 (sem alerta).
    linha1 = df[df["data_semana"].dt.date == date(2022, 12, 26)].iloc[0]
    assert linha1["precipitacao_semanal_mm"] == 8.0
    assert linha1["deficit_hidrico_semanal"] == -10.0
    assert linha1["crop_stress_index"] == 10.0
    assert not linha1["alerta_estresse"]
    assert linha1["preco_brl_saca"] == 1000.0
    assert len(df) == 1


def _seed_processed_real(storage: LocalStorage) -> None:
    """Gera balanco hidrico e cotacoes reais de Sul de Minas no lake."""
    clima = pd.DataFrame(
        {
            "dt": pd.to_datetime(["2023-01-02", "2023-01-03"]),
            "polo_produtor": [WEEKLY_POLO] * 2,
            "commodity": [WEEKLY_COMMODITY] * 2,
            "precipitacao_chirps_mm": [2.0, 3.0],
            "etp_mm": [4.0, 4.0],
            "deficit_hidrico_mm": [-2.0, -1.0],
            "alerta_estresse_hidrico": [False, False],
        }
    )
    fin = pd.DataFrame(
        {
            "dt": pd.to_datetime(["2023-01-02", "2023-01-03"]),
            "ticker": ["KC=F"] * 2,
            "preco_brl_saca": [1200.0, 1210.0],
            "volatilidade_21d_anualizada": [15.0, 16.0],
        }
    )
    storage.write_parquet("processed/climate/dt=2023-01-02/water_balance.parquet", clima)
    storage.write_parquet(
        "processed/finance/ticker_safe=KC_F/cotacoes_brl_saca.parquet", fin
    )


def test_gold_weekly_usa_dados_reais_quando_disponiveis(
    settings: Settings, storage: LocalStorage
) -> None:
    """Com processed real de Sul de Minas, o fallback sintetico nao e usado."""
    _seed_processed_real(storage)

    def _boom() -> tuple[pd.DataFrame, pd.DataFrame]:
        raise AssertionError("fallback nao deveria ser chamado")

    original = gold_module._fallback_semanal
    gold_module._fallback_semanal = _boom  # type: ignore[assignment]
    try:
        df = build_gold_weekly_analytics(storage=storage, write_output=False)
    finally:
        gold_module._fallback_semanal = original

    assert not df.empty
    assert df["data_semana"].dt.date.iloc[0] == date(2023, 1, 2)
    assert df["precipitacao_semanal_mm"].iloc[0] == 5.0
    assert df["preco_brl_saca"].iloc[0] == 1210.0


def test_sync_upsert_idempotente_sqlite(
    settings: Settings, storage: LocalStorage, sqlite_engine: Engine
) -> None:
    """Sync grava e reexecuta sem duplicar (UPSERT pela chave natural)."""
    build_gold_weekly_analytics(storage=storage, write_output=True)

    total1 = sync_weekly_gold_to_db(
        engine=sqlite_engine, storage=storage, settings=settings
    )
    total2 = sync_weekly_gold_to_db(
        engine=sqlite_engine, storage=storage, settings=settings
    )

    assert total1 == total2
    with sqlite_engine.connect() as conn:
        polos = conn.execute(sa.text("SELECT COUNT(*) FROM dim_polo")).scalar()
        clima = conn.execute(
            sa.text("SELECT COUNT(*) FROM fact_clima_semanal_cafe")
        ).scalar()
        cotas = conn.execute(
            sa.text("SELECT COUNT(*) FROM fact_cotacoes_cafe_saca")
        ).scalar()
    assert polos == 1
    assert clima == cotas
    assert total1 == 1 + clima + cotas


def test_sync_sem_gold_levanta_runtime_error(
    settings: Settings, storage: LocalStorage, sqlite_engine: Engine
) -> None:
    with pytest.raises(RuntimeError, match="ausente no lake"):
        sync_weekly_gold_to_db(
            engine=sqlite_engine, storage=storage, settings=settings
        )


def test_models_criacao_tabelas(sqlite_engine: Engine) -> None:
    """O schema ORM e criado com sucesso no SQLite."""
    Base.metadata.create_all(sqlite_engine)
    with sqlite_engine.connect() as conn:
        tabelas = (
            conn.execute(
                sa.text("SELECT name FROM sqlite_master WHERE type='table'")
            )
            .scalars()
            .all()
        )
    assert {"dim_polo", "fact_clima_semanal_cafe", "fact_cotacoes_cafe_saca"}.issubset(
        set(tabelas)
    )


def test_create_engine_sem_senha_usar_sqlite(settings: Settings) -> None:
    """Sem POSTGRES_PASSWORD, o engine cai para SQLite (fallback local)."""
    sem_senha = replace(settings, postgres_password=None)
    engine = create_engine_from_settings(sem_senha)
    assert engine.dialect.name == "sqlite"
    engine.dispose()


