"""ETAPA 5 - Sincronizacao idempotente (UPSERT) da camada Gold para o banco.

Fluxo:
    gold/analytics_coffee_stress_weekly.parquet
    -> sync_weekly_gold_to_db()
       -> DimPolo (upsert por nome)
       -> FactClimaSemanalCafe (upsert por data_semana + polo_id)
       -> FactCotacoesCafeSaca (upsert por data_semana + ticker)

O banco de destino e o PostgreSQL/PostGIS (RDS) via `settings.postgres_dsn`;
quando indisponivel, o engine cai automaticamente para SQLite local
(`sqlite:///data_lake/hydra.db`), permitindo rodar e testar sem infraestrutura.

Execucao:
    python -m src.database.sync
"""

from __future__ import annotations

import io

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.analytics.gold import GOLD_WEEKLY_FILENAME, WEEKLY_POLO
from src.config import Settings, get_logger, get_settings
from src.database.models import (
    Base,
    DimPolo,
    FactClimaSemanalCafe,
    FactCotacoesCafeSaca,
    create_engine_from_settings,
    enable_postgis,
)
from src.storage import ObjectNotFoundError, StorageBackend, get_storage

logger = get_logger("database.sync")


def _upsert_statement(engine: Engine, model: type[Base], index_elements: list[str]):
    """Constroi a clausula ON CONFLICT adequada ao dialeto do banco.

    O `set_` usa `excluded.<coluna>` para que o SQL referencie os valores novos
    (INSERT ... ON CONFLICT DO UPDATE SET col = excluded.col).

    Args:
        engine: engine do banco (postgresql ou sqlite).
        model: classe ORM de destino.
        index_elements: colunas que definem a chave natural do UPSERT.

    Returns:
        Objeto insert com `on_conflict_do_update`.
    """
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.dialects.sqlite import insert as sqlite_insert

    insert = sqlite_insert(model) if engine.dialect.name == "sqlite" else pg_insert(model)
    set_colunas = {
        col.name: getattr(insert.excluded, col.name)
        for col in model.__table__.columns
        if col.name not in ("id", "created_at")
    }
    return insert.on_conflict_do_update(
        index_elements=index_elements,
        set_=set_colunas,
    )


def _upsert_rows(engine: Engine, model: type[Base], registros: list[dict]) -> int:
    """Executa um lote de UPSERT idempotente.

    Args:
        engine: engine do banco.
        model: classe ORM de destino.
        registros: lista de dicts com os valores das colunas.

    Returns:
        Quantidade de linhas gravadas/atualizadas.

    Raises:
        RuntimeError: se a gravacao falhar (ex.: constraint/valor invalido).
    """
    from typing import cast

    from sqlalchemy import Table, UniqueConstraint

    if not registros:
        return 0

    tabela = cast(Table, model.__table__)
    chave_natural: list[str] = []
    for constraint in tabela.constraints:
        if isinstance(constraint, UniqueConstraint):
            chave_natural = [col.name for col in constraint.columns]
            break
    if not chave_natural:
        # `unique=True` + `index=True` vira Index unico (nao UniqueConstraint).
        for index in tabela.indexes:
            if index.unique:
                chave_natural = [col.name for col in index.columns]
                break
    if not chave_natural:
        chave_natural = ["id"]

    stmt = _upsert_statement(engine, model, chave_natural)
    try:
        with engine.begin() as conn:
            conn.execute(stmt, registros)
    except Exception as exc:
        raise RuntimeError(f"Falha no UPSERT de {model.__tablename__}: {exc}") from exc

    logger.info("UPSERT %s | linhas=%d", model.__tablename__, len(registros))
    return len(registros)


def _ensure_polo(engine: Engine, settings: Settings) -> int:
    """Garante a dimensao do polo Sul de Minas e devolve o seu `id`."""
    from src.processing.geometry import POLOS_BY_NAME

    polo = POLOS_BY_NAME[WEEKLY_POLO]
    registros = [
        {
            "nome": polo.nome,
            "uf": polo.uf,
            "commodity": polo.commodity,
            "min_lon": polo.bbox[0],
            "min_lat": polo.bbox[1],
            "max_lon": polo.bbox[2],
            "max_lat": polo.bbox[3],
            "geometry_wkt": _bbox_wkt(polo.bbox),
        }
    ]
    _upsert_rows(engine, DimPolo, registros)

    with Session(engine) as session:
        dim = session.query(DimPolo).filter(DimPolo.nome == WEEKLY_POLO).one()
        return int(dim.id)


def _bbox_wkt(bbox: tuple[float, float, float, float]) -> str:
    """Converte uma bounding box em um POLYGON WKT (EPSG:4326)."""
    min_lon, min_lat, max_lon, max_lat = bbox
    return (
        f"POLYGON(({min_lon} {min_lat},{max_lon} {min_lat},"
        f"{max_lon} {max_lat},{min_lon} {max_lat},{min_lon} {min_lat}))"
    )


def sync_weekly_gold_to_db(
    engine: Engine | None = None,
    storage: StorageBackend | None = None,
    settings: Settings | None = None,
    enable_geo: bool = True,
) -> int:
    """Sincroniza a Gold semanal de cafe para as tabelas relacionais.

    Lê `gold/analytics_coffee_stress_weekly.parquet` via `get_storage()`,
    garante a `DimPolo` e faz o UPSERT dos fatos de clima e cotacoes.

    Args:
        engine: engine SQLAlchemy; None cria via `create_engine_from_settings`.
        storage: backend de leitura da Gold; None resolve via factory.
        settings: configuracao; None usa `get_settings()`.
        enable_geo: tenta habilitar PostGIS no PostgreSQL (best effort).

    Returns:
        Total de linhas gravadas (dim + clima + cotacoes).

    Raises:
        RuntimeError: se a Gold semanal nao existir no lake ou a gravacao falhar.
    """
    cfg = settings or get_settings()
    backend = storage or get_storage(settings=cfg)
    db = engine or create_engine_from_settings(cfg)

    Base.metadata.create_all(db)
    if enable_geo:
        enable_postgis(db)

    chave = f"gold/{GOLD_WEEKLY_FILENAME}"
    try:
        payload = backend.read_bytes(chave)
    except ObjectNotFoundError as exc:
        raise RuntimeError(
            f"{chave} ausente no lake. Execute `python -m src.analytics.gold "
            "--weekly` antes do sync."
        ) from exc

    import pandas as pd_mod

    df = pd_mod.read_parquet(io.BytesIO(payload))
    df["data_semana"] = pd_mod.to_datetime(df["data_semana"]).dt.date
    if df.empty:
        logger.warning("Gold semanal vazia; nada a sincronizar")
        return 0

    polo_id = _ensure_polo(db, cfg)
    linhas = 1  # dim_polo

    clima = (
        df.groupby("data_semana", as_index=False)
        .agg(
            precipitacao_semanal_mm=("precipitacao_semanal_mm", "first"),
            et0_semanal_mm=("et0_semanal_mm", "first"),
            deficit_hidrico_semanal=("deficit_hidrico_semanal", "first"),
            crop_stress_index=("crop_stress_index", "first"),
            alerta_estresse=("alerta_estresse", "first"),
        )
        .copy()
    )
    clima["polo_id"] = polo_id
    linhas += _upsert_rows(
        db,
        FactClimaSemanalCafe,
        clima.to_dict(orient="records"),
    )

    cotacoes = df[["data_semana", "ticker", "preco_brl_saca"]].copy()
    # Mapeia as metricas da Gold refatorada para as colunas do fato relacional.
    if "retorno_semanal_pct" in df.columns:
        cotacoes["retorno_semanal"] = df["retorno_semanal_pct"]
    elif "retorno_semanal" in df.columns:
        cotacoes["retorno_semanal"] = df["retorno_semanal"]
    if "volatilidade_21d_anualizada" in df.columns:
        cotacoes["volatilidade"] = df["volatilidade_21d_anualizada"]
    elif "volatilidade" in df.columns:
        cotacoes["volatilidade"] = df["volatilidade"]
    cotacoes = cotacoes.dropna(subset=["preco_brl_saca"])
    linhas += _upsert_rows(
        db,
        FactCotacoesCafeSaca,
        cotacoes.to_dict(orient="records"),
    )

    logger.info(
        "Sync Gold -> banco concluido | engine=%s | linhas_total=%d",
        db.dialect.name,
        linhas,
    )
    return linhas


def sync_gold_to_db(
    engine: Engine | None = None,
    storage: StorageBackend | None = None,
    settings: Settings | None = None,
) -> int:
    """API principal de sincronizacao da Gold para o banco relacional.

    Conecta ao PostgreSQL/PostGIS (RDS) via `settings.postgres_dsn` com fallback
    automatico para SQLite local (`sqlite:///data_lake/hydra.db`) e executa o
    UPSERT idempotente das tabelas `DimPolo`, `FactClimaSemanalCafe` e
    `FactCotacoesCafeSaca` a partir da Gold semanal.

    Args:
        engine: engine SQLAlchemy; None cria via `create_engine_from_settings`.
        storage: backend de leitura da Gold; None resolve via factory.
        settings: configuracao; None usa `get_settings()`.

    Returns:
        Total de linhas gravadas/atualizadas.
    """
    return sync_weekly_gold_to_db(engine=engine, storage=storage, settings=settings)


def main() -> int:
    """Ponto de entrada da CLI: sincroniza a Gold semanal para o banco.

    Returns:
        0 = sucesso; 1 = falha (Gold ausente ou banco indisponivel).
    """
    from src.config import configure_logging

    settings = get_settings()
    configure_logging(log_file="database_sync.log", settings=settings)

    try:
        total = sync_weekly_gold_to_db(settings=settings)
    except Exception as exc:  # rede de seguranca da CLI
        logger.critical("Sync falhou: %s: %s", type(exc).__name__, exc)
        return 1

    logger.info("SYNC OK | linhas_gravadas=%d", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

