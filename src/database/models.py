"""ETAPA 5 - Modelos ORM para o PostgreSQL/PostGIS (fallback SQLite local).

Esquema relacional do escopo Hydra (Cafe Arabica, Sul de Minas, semanal):

    DimPolo                  -> dimensao dos polos produtores (geometria WKT)
    FactClimaSemanalCafe     -> fato climatico semanal (precipitacao, ET0,
                                deficit, Crop Stress Index, alerta)
    FactCotacoesCafeSaca     -> fato de mercado semanal (preco BRL/saca,
                                retorno e volatilidade)

PostGIS: quando o banco e PostgreSQL, `enable_postgis` ativa a extensao e cria
a coluna espacial `geom` a partir do WKT (SIRGAS/WGS84, SRID 4326). No SQLite o
modelo funciona sem geometria (coluna `geometry_wkt` textual).

Uso:
    engine = create_engine_from_settings()
    Base.metadata.create_all(engine)
"""

from __future__ import annotations

from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from src.config import Settings, get_logger, get_settings

logger = get_logger("database.models")

#: SRID padrao das geometrias (WGS84).
SRID_WGS84: int = 4326
#: Caminho do banco SQLite de fallback local.
SQLITE_FALLBACK_URL: str = "sqlite:///data_lake/hydra.db"


class Base(DeclarativeBase):
    """Base declarativa das tabelas da plataforma."""


class DimPolo(Base):
    """Dimensao dos polos produtores de referencia.

    Attributes:
        id: chave primaria.
        nome: identificador do polo (ex.: "Sul_de_Minas").
        uf: unidade federativa.
        commodity: cultura predominante.
        min_lon/min_lat/max_lon/max_lat: bounding box WGS84.
        area_km2: area aproximada (projecao de area igual).
        geometry_wkt: poligono em WKT (EPSG:4326) para PostGIS.
        created_at: timestamp de criacao do registro.
    """

    __tablename__ = "dim_polo"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nome: Mapped[str] = mapped_column(
        String(80), unique=True, nullable=False, index=True
    )
    uf: Mapped[str] = mapped_column(String(2), nullable=False)
    commodity: Mapped[str] = mapped_column(String(40), nullable=False)
    min_lon: Mapped[float] = mapped_column(Float, nullable=False)
    min_lat: Mapped[float] = mapped_column(Float, nullable=False)
    max_lon: Mapped[float] = mapped_column(Float, nullable=False)
    max_lat: Mapped[float] = mapped_column(Float, nullable=False)
    area_km2: Mapped[float] = mapped_column(Float, nullable=True)
    geometry_wkt: Mapped[str] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    fatos_clima: Mapped[list[FactClimaSemanalCafe]] = relationship(
        back_populates="polo"
    )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<DimPolo id={self.id} nome={self.nome!r} commodity={self.commodity!r}>"
        )


class FactClimaSemanalCafe(Base):
    """Fato climatico semanal do cafe arabica por polo.

    Unidades: milimetros (mm) de precipitacao, ET0 e deficit acumulados na
    semana. `crop_stress_index` e o deficit semanal com sinal invertido truncado
    em zero (positivo = estresse). `alerta_estresse` liga acima do limiar.
    """

    __tablename__ = "fact_clima_semanal_cafe"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    data_semana: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    polo_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("dim_polo.id"), nullable=False, index=True
    )
    precipitacao_semanal_mm: Mapped[float] = mapped_column(Float, nullable=True)
    et0_semanal_mm: Mapped[float] = mapped_column(Float, nullable=True)
    deficit_hidrico_semanal: Mapped[float] = mapped_column(Float, nullable=True)
    crop_stress_index: Mapped[float] = mapped_column(Float, nullable=True)
    alerta_estresse: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    polo: Mapped[DimPolo] = relationship(back_populates="fatos_clima")

    __table_args__ = (
        UniqueConstraint("data_semana", "polo_id", name="uq_clima_semana_polo"),
    )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<FactClimaSemanalCafe semana={self.data_semana} "
            f"polo_id={self.polo_id} alerta={self.alerta_estresse}>"
        )


class FactCotacoesCafeSaca(Base):
    """Fato de mercado semanal: cafe arabica em BRL por saca de 60 kg.

    Attributes:
        data_semana: inicio da semana (segunda-feira).
        ticker: contrato (KC=F/ICF=F).
        preco_brl_saca: ultimo fechamento da semana em BRL/saca.
        retorno_semanal: variacao percentual da semana em relacao a anterior.
        volatilidade: media semanal da volatilidade movel diaria (%).
    """

    __tablename__ = "fact_cotacoes_cafe_saca"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    data_semana: Mapped[date] = mapped_column(Date, nullable=False, index=True)
    ticker: Mapped[str] = mapped_column(String(12), nullable=False, index=True)
    preco_brl_saca: Mapped[float] = mapped_column(Float, nullable=False)
    retorno_semanal: Mapped[float] = mapped_column(Float, nullable=True)
    volatilidade: Mapped[float] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("data_semana", "ticker", name="uq_cotacao_semana_ticker"),
    )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"<FactCotacoesCafeSaca semana={self.data_semana} "
            f"ticker={self.ticker!r} preco={self.preco_brl_saca:.2f}>"
        )


def create_engine_from_settings(settings: Settings | None = None) -> Engine:
    """Cria o SQLAlchemy Engine a partir do `postgres_dsn` do .env.

    Args:
        settings: configuracao; se None usa `get_settings()`.

    Returns:
        Engine apontando para o PostgreSQL/PostGIS (RDS) ou SQLite de fallback
        quando o DSN nao estiver preenchido.
    """
    cfg = settings or get_settings()
    if not cfg.postgres_password:
        logger.warning(
            "POSTGRES_PASSWORD vazio no .env; usando fallback SQLite "
            "(%s). Defina o RDS para persistir no PostGIS.",
            SQLITE_FALLBACK_URL,
        )
        return create_engine(SQLITE_FALLBACK_URL, future=True)

    engine = create_engine(cfg.postgres_dsn, future=True, pool_pre_ping=True)
    logger.info(
        "Engine PostgreSQL criado | host=%s:%s",
        cfg.postgres_host,
        cfg.postgres_port,
    )
    return engine


def enable_postgis(engine: Engine) -> bool:
    """Ativa o PostGIS e cria a coluna espacial `geom` no DimPolo (best effort).

    Args:
        engine: engine do banco.

    Returns:
        True se o PostGIS foi aplicado; False quando o banco nao e PostgreSQL
        ou a extensao nao esta disponivel.
    """
    if engine.dialect.name != "postgresql":
        return False
    try:
        from sqlalchemy import text

        with engine.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            conn.execute(
                text(
                    "SELECT AddGeometryColumn('public','dim_polo','geom',"
                    + str(SRID_WGS84)
                    + ",'POLYGON',2)"
                )
            )
        logger.info("PostGIS habilitado e coluna geom criada em dim_polo")
        return True
    except Exception as exc:  # extensao ausente/permissao negada
        logger.warning("PostGIS indisponivel (%s); mantendo geometry_wkt", exc)
        return False


