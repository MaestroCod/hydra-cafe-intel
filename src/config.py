"""ETAPA 2 - Centralizador de configuracao tipado (fonte unica de verdade).

Le o arquivo `.env` uma unica vez e expoe um objeto `Settings` imutavel,
usado por todos os modulos (storage, ingestao, transformacao). Tambem
concentra a configuracao de logging estruturado (console + arquivo),
espelhando o formato usado no `setup_environment.py` da Etapa 1.

Exemplo:
    >>> from src.config import get_settings, configure_logging
    >>> settings = get_settings()
    >>> logger = configure_logging()
    >>> settings.storage_backend
    'local'
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from logging import Logger
from pathlib import Path
from typing import Final, Literal

# -----------------------------------------------------------------------------
# Constantes globais
# -----------------------------------------------------------------------------
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
ENV_FILE: Final[Path] = PROJECT_ROOT / ".env"
ROOT_LOGGER_NAME: Final[str] = "agro_intel"

StorageBackendName = Literal["local", "s3"]

#: Prefixos canonicos do Data Lake (identicos no disco local e no S3).
RAW_FINANCE_PREFIX: Final[str] = "raw/finance"
RAW_CHIRPS_PREFIX: Final[str] = "raw/climate_chirps"
RAW_ERA5_PREFIX: Final[str] = "raw/climate_era5"
PROCESSED_FINANCE_PREFIX: Final[str] = "processed/finance"
PROCESSED_CLIMATE_PREFIX: Final[str] = "processed/climate"

#: Valores que devem ser interpretados como "nao configurado".
_PLACEHOLDERS: Final[frozenset[str]] = frozenset(
    {"", "cole_seu_token_pessoal_aqui", "changeme", "todo", "none", "null"}
)
_TRUE_VALUES: Final[frozenset[str]] = frozenset({"1", "true", "t", "yes", "y", "on"})


class ConfigError(RuntimeError):
    """Erro de configuracao invalida ou ausente no .env."""


# -----------------------------------------------------------------------------
# Parsers tipados de variaveis de ambiente
# -----------------------------------------------------------------------------
def _raw(name: str) -> str | None:
    """Retorna o valor bruto da variavel de ambiente, tratando placeholders."""
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return None if stripped.lower() in _PLACEHOLDERS else stripped


def env_str(name: str, default: str = "") -> str:
    """Le uma variavel de ambiente textual."""
    return _raw(name) or default


def env_int(name: str, default: int) -> int:
    """Le uma variavel de ambiente inteira.

    Raises:
        ConfigError: se o valor presente nao for um inteiro valido.
    """
    value = _raw(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ConfigError(f"{name} deve ser inteiro, recebido {value!r}") from exc


def env_float(name: str, default: float) -> float:
    """Le uma variavel de ambiente de ponto flutuante.

    Raises:
        ConfigError: se o valor presente nao for um float valido.
    """
    value = _raw(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ConfigError(f"{name} deve ser numerico, recebido {value!r}") from exc


def env_bool(name: str, default: bool = False) -> bool:
    """Le uma variavel de ambiente booleana (1/true/yes/on)."""
    value = _raw(name)
    return default if value is None else value.lower() in _TRUE_VALUES


def env_tuple(name: str, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    """Le uma lista separada por virgulas e devolve uma tupla imutavel."""
    value = _raw(name)
    if value is None:
        return default
    return tuple(item.strip() for item in value.split(",") if item.strip())


def env_bbox(name: str, default: tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    """Le um bounding box "W,S,E,N" e devolve uma tupla de floats.

    Raises:
        ConfigError: se o formato nao tiver exatamente 4 numeros.
    """
    value = _raw(name)
    if value is None:
        return default
    parts = [item.strip() for item in value.split(",") if item.strip()]
    if len(parts) != 4:
        raise ConfigError(f"{name} deve ter 4 valores (W,S,E,N), recebido {value!r}")
    try:
        west, south, east, north = (float(item) for item in parts)
    except ValueError as exc:
        raise ConfigError(f"{name} contem valores nao numericos: {value!r}") from exc
    if west >= east or south >= north:
        raise ConfigError(
            f"{name} deve seguir a ordem (min_lon,min_lat,max_lon,max_lat) com "
            f"min < max; recebido {value!r}"
        )
    return west, south, east, north


def resolve_relative_date(value: str, ref: date | None = None) -> str:
    """Converte uma data relativa ("1y", "6M", "180d") em data literal.

    Args:
        value: "1y"/"2y", "6M", "180d" ou "YYYY-MM-DD" (retornada como esta).
        ref: data de referencia (default: hoje em UTC).

    Returns:
        Data em formato "YYYY-MM-DD".

    Raises:
        ConfigError: se o valor relativo for invalido.
    """
    valor = value.strip().lower()
    if valor and valor[-1] in "ymd" and valor[:-1].isdigit():
        quantidade = int(valor[:-1])
        hoje = ref or datetime.now(tz=UTC).date()
        if valor.endswith("y"):
            inicio = hoje - timedelta(days=365 * quantidade)
        elif valor.endswith("m"):
            inicio = hoje - timedelta(days=30 * quantidade)
        else:
            inicio = hoje - timedelta(days=quantidade)
        return inicio.isoformat()
    try:
        date.fromisoformat(valor)
    except ValueError as exc:
        raise ConfigError(
            f"Data invalida {value!r}; use '1y', '6M', '180d' ou 'YYYY-MM-DD'"
        ) from exc
    return valor


# -----------------------------------------------------------------------------
# Objeto de configuracao imutavel
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class Settings:
    """Configuracao completa da plataforma (imutavel e hashavel).

    Campos sensiveis usam `repr=False` para nunca vazarem em logs/tracebacks.
    """

    # Ambiente / observabilidade
    app_env: str = "local"
    log_level: str = "INFO"
    log_dir: str = "logs"

    # Abstracao de storage
    storage_backend: str = "local"
    data_lake_root: str = "data_lake"

    # AWS
    aws_region: str = "us-east-1"
    aws_access_key_id: str | None = field(default=None, repr=False)
    aws_secret_access_key: str | None = field(default=None, repr=False)
    aws_session_token: str | None = field(default=None, repr=False)
    s3_bucket_raw: str = "agro-intel-raw"
    s3_bucket_processed: str = "agro-intel-processed"
    s3_endpoint_url: str | None = None  # util para MinIO/LocalStack

    # Copernicus / ERA5-Land
    cdsapi_url: str = "https://cds.climate.copernicus.eu/api"
    cdsapi_key: str | None = field(default=None, repr=False)

    # CHIRPS
    chirps_base_url: str = (
        "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05"
    )
    chirps_prelim_base_url: str = (
        "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/tifs/p05"
    )
    chirps_timeout_seconds: int = 180
    chirps_max_retries: int = 3
    #: Latencia do produto CHIRPS final (~1 a 2 meses); usada como data default.
    chirps_lag_days: int = 45
    chirps_chunk_size_bytes: int = 1024 * 1024
    #: Historico (em dias) puxado no backfill de 1 ano.
    chirps_lookback_days: int = 365

    # ERA5-Land (Copernicus CDS)
    era5_dataset: str = "reanalysis-era5-land"
    era5_variables: tuple[str, ...] = (
        "2m_temperature",
        "maximum_2m_temperature_since_previous_post_processing",
        "minimum_2m_temperature_since_previous_post_processing",
        "total_precipitation",
        "potential_evaporation",
        "volumetric_soil_water_layer_1",
    )
    era5_max_retries: int = 3
    #: ERA5-Land consolidado tem latencia de ~5 dias (ERA5-Land-T e mais rapido).
    era5_lag_days: int = 6
    era5_data_format: str = "netcdf"
    era5_download_format: str = "unarchived"
    #: Historico (em dias) puxado no backfill de 1 ano.
    era5_lookback_days: int = 365

    # Eixo financeiro
    #: Escopo do projeto: apenas cafe arabica (tipo 4/5).
    finance_tickers: tuple[str, ...] = ("KC=F", "ICF=F")
    #: Tickers cambiais (dolar/real) usados para converter contratos em USD.
    finance_fx_tickers: tuple[str, ...] = ("BRL=X",)
    #: Data inicial; aceita relativa ("1y", "6M", "180d") ou "YYYY-MM-DD".
    finance_start_date: str = "1y"
    finance_interval: str = "1d"
    finance_max_retries: int = 3
    finance_retry_backoff_seconds: float = 2.0

    # Recorte espacial (Brasil)
    aoi_name: str = "brasil"
    #: Bounding box WGS84 no formato (min_lon, min_lat, max_lon, max_lat).
    aoi_bbox: tuple[float, float, float, float] = (-73.98, -33.75, -28.85, 5.27)

    # Processamento (camada Silver / processed)
    #: Escopo de commodities ativas (vazio = todas). Default do projeto: cafe.
    scope_commodities: tuple[str, ...] = ("coffee_arabica",)
    #: Escopo de polos ativos (vazio = todos). Default do projeto: polos de cafe.
    scope_polos: tuple[str, ...] = ("Sul_de_Minas", "Cerrado_Mineiro")
    #: GeoJSON opcional com os polos produtores; vazio = usa as bboxes internas.
    polos_geojson_path: str | None = None
    #: Janela do deficit hidrico acumulado (dias).
    water_stress_window_days: int = 7
    #: Limiar critico: deficit acumulado abaixo deste valor liga o alerta (mm).
    water_stress_deficit_mm: float = -30.0
    #: Peso de 1 saca de cafe em kg (padrao do mercado brasileiro).
    saca_weight_kg: float = 60.0

    # PostGIS
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "agro_intel"
    postgres_user: str = "postgres"
    postgres_password: str | None = field(default=None, repr=False)

    # -- Propriedades derivadas ------------------------------------------------
    @property
    def is_s3_backend(self) -> bool:
        """True quando o backend configurado e o AWS S3."""
        return self.storage_backend.strip().lower() == "s3"

    @property
    def lake_root_path(self) -> Path:
        """Raiz absoluta do Data Lake local (ignorada quando backend=s3)."""
        candidate = Path(self.data_lake_root).expanduser()
        return candidate if candidate.is_absolute() else (PROJECT_ROOT / candidate).resolve()

    @property
    def log_dir_path(self) -> Path:
        """Diretorio absoluto de logs."""
        candidate = Path(self.log_dir).expanduser()
        return candidate if candidate.is_absolute() else (PROJECT_ROOT / candidate).resolve()

    @property
    def s3_bucket_map(self) -> dict[str, str]:
        """Mapeia a camada do lake para o bucket S3 correspondente."""
        return {"raw": self.s3_bucket_raw, "processed": self.s3_bucket_processed}

    @property
    def aoi_bbox_wgs84(self) -> tuple[float, float, float, float]:
        """Bounding box no formato geografico (min_lon, min_lat, max_lon, max_lat).

        Ordem esperada por `rioxarray.rio.clip_box`, shapely e GeoPandas.

        Example:
            >>> get_settings().aoi_bbox_wgs84
            (-73.98, -33.75, -28.85, 5.27)
        """
        return self.aoi_bbox

    @property
    def aoi_bbox_cds(self) -> tuple[float, float, float, float]:
        """Bounding box no formato do Copernicus CDS: (North, West, South, East).

        Example:
            >>> get_settings().aoi_bbox_cds
            (5.27, -73.98, -33.75, -28.85)
        """
        min_lon, min_lat, max_lon, max_lat = self.aoi_bbox
        return (max_lat, min_lon, min_lat, max_lon)

    @property
    def all_finance_tickers(self) -> tuple[str, ...]:
        """Tickers de commodities somados aos cambiais (ex.: BRL=X), sem repeticao."""
        seen: dict[str, None] = {}
        for ticker in (*self.finance_tickers, *self.finance_fx_tickers):
            seen.setdefault(ticker, None)
        return tuple(seen)

    @property
    def postgres_dsn(self) -> str:
        """DSN SQLAlchemy para o RDS PostGIS (usado nas etapas seguintes)."""
        password = self.postgres_password or ""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    def require(self, *field_names: str) -> None:
        """Garante que os campos informados estao preenchidos.

        Args:
            *field_names: nomes de atributos de Settings a validar.

        Raises:
            ConfigError: se algum campo estiver vazio/None.
        """
        missing = [name for name in field_names if not getattr(self, name, None)]
        if missing:
            raise ConfigError(
                "Configuracao ausente no .env: " + ", ".join(sorted(missing))
            )


# -----------------------------------------------------------------------------
# Carregamento do .env
# -----------------------------------------------------------------------------
def load_dotenv_file(env_file: Path = ENV_FILE, override: bool = False) -> bool:
    """Carrega o arquivo .env para `os.environ`.

    Args:
        env_file: caminho do arquivo .env.
        override: se True, sobrescreve variaveis ja presentes no ambiente.

    Returns:
        True se o arquivo foi carregado; False se ausente ou python-dotenv
        indisponivel (nesse caso as variaveis do proprio SO sao usadas).
    """
    if not env_file.is_file():
        return False
    try:
        from dotenv import load_dotenv as _load_dotenv
    except ImportError:  # pragma: no cover - dependencia validada na Etapa 1
        return False
    return bool(_load_dotenv(dotenv_path=env_file, override=override))


def build_settings() -> Settings:
    """Monta o objeto Settings a partir das variaveis de ambiente.

    Returns:
        Settings preenchido e validado.

    Raises:
        ConfigError: se algum valor tipado estiver em formato invalido.
    """
    load_dotenv_file()

    backend = env_str("STORAGE_BACKEND", "local").lower()
    if backend not in ("local", "s3"):
        raise ConfigError(
            f"STORAGE_BACKEND invalido: {backend!r} (esperado 'local' ou 's3')"
        )

    return Settings(
        app_env=env_str("APP_ENV", "local"),
        log_level=env_str("LOG_LEVEL", "INFO").upper(),
        log_dir=env_str("LOG_DIR", "logs"),
        storage_backend=backend,
        data_lake_root=env_str("DATA_LAKE_ROOT", "data_lake"),
        aws_region=env_str("AWS_REGION", "us-east-1"),
        aws_access_key_id=_raw("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=_raw("AWS_SECRET_ACCESS_KEY"),
        aws_session_token=_raw("AWS_SESSION_TOKEN"),
        s3_bucket_raw=env_str("S3_BUCKET_RAW", "agro-intel-raw"),
        s3_bucket_processed=env_str("S3_BUCKET_PROCESSED", "agro-intel-processed"),
        s3_endpoint_url=_raw("S3_ENDPOINT_URL"),
        cdsapi_url=env_str("CDSAPI_URL", "https://cds.climate.copernicus.eu/api"),
        cdsapi_key=_raw("CDSAPI_KEY"),
        chirps_base_url=env_str(
            "CHIRPS_BASE_URL",
            "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05",
        ),
        chirps_prelim_base_url=env_str(
            "CHIRPS_PRELIM_BASE_URL",
            "https://data.chc.ucsb.edu/products/CHIRPS-2.0/prelim/global_daily/tifs/p05",
        ),
        chirps_timeout_seconds=env_int("CHIRPS_TIMEOUT_SECONDS", 180),
        chirps_max_retries=env_int("CHIRPS_MAX_RETRIES", 3),
        chirps_lag_days=env_int("CHIRPS_LAG_DAYS", 45),
        chirps_chunk_size_bytes=env_int("CHIRPS_CHUNK_SIZE_BYTES", 1024 * 1024),
        chirps_lookback_days=env_int("CHIRPS_LOOKBACK_DAYS", 365),
        era5_dataset=env_str("ERA5_DATASET", "reanalysis-era5-land"),
        era5_variables=env_tuple(
            "ERA5_VARIABLES",
            (
                "2m_temperature",
                "maximum_2m_temperature_since_previous_post_processing",
                "minimum_2m_temperature_since_previous_post_processing",
                "total_precipitation",
                "potential_evaporation",
                "volumetric_soil_water_layer_1",
            ),
        ),
        era5_max_retries=env_int("ERA5_MAX_RETRIES", 3),
        era5_lag_days=env_int("ERA5_LAG_DAYS", 6),
        era5_data_format=env_str("ERA5_DATA_FORMAT", "netcdf"),
        era5_download_format=env_str("ERA5_DOWNLOAD_FORMAT", "unarchived"),
        era5_lookback_days=env_int("ERA5_LOOKBACK_DAYS", 365),
        finance_tickers=env_tuple("FINANCE_TICKERS", ("KC=F", "ICF=F")),
        finance_fx_tickers=env_tuple("FINANCE_FX_TICKERS", ("BRL=X",)),
        finance_start_date=env_str("FINANCE_START_DATE", "1y"),
        finance_interval=env_str("FINANCE_INTERVAL", "1d"),
        finance_max_retries=env_int("FINANCE_MAX_RETRIES", 3),
        aoi_name=env_str("AOI_NAME", "brasil"),
        aoi_bbox=env_bbox("AOI_BBOX", (-73.98, -33.75, -28.85, 5.27)),
        scope_commodities=env_tuple("SCOPE_COMMODITIES", ("coffee_arabica",)),
        scope_polos=env_tuple("SCOPE_POLOS", ("Sul_de_Minas", "Cerrado_Mineiro")),
        polos_geojson_path=_raw("POLOS_GEOJSON_PATH"),
        water_stress_window_days=env_int("WATER_STRESS_WINDOW_DAYS", 7),
        water_stress_deficit_mm=env_float("WATER_STRESS_DEFICIT_MM", -30.0),
        saca_weight_kg=env_float("SACA_WEIGHT_KG", 60.0),
        postgres_host=env_str("POSTGRES_HOST", "localhost"),
        postgres_port=env_int("POSTGRES_PORT", 5432),
        postgres_db=env_str("POSTGRES_DB", "agro_intel"),
        postgres_user=env_str("POSTGRES_USER", "postgres"),
        postgres_password=_raw("POSTGRES_PASSWORD"),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Retorna a instancia unica (cacheada) de Settings."""
    return build_settings()


def reload_settings() -> Settings:
    """Limpa o cache e recarrega o .env (util em testes e notebooks)."""
    get_settings.cache_clear()
    load_dotenv_file(override=True)
    return get_settings()


# -----------------------------------------------------------------------------
# Logging estruturado (mesmo formato da Etapa 1 -> pronto para CloudWatch)
# -----------------------------------------------------------------------------
LOG_FORMAT: Final[str] = (
    "%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s | %(message)s"
)
LOG_DATE_FORMAT: Final[str] = "%Y-%m-%dT%H:%M:%S"


def configure_logging(
    level: str | None = None,
    log_file: str = "ingestion.log",
    settings: Settings | None = None,
) -> Logger:
    """Configura o logger raiz da aplicacao (console + arquivo).

    A funcao e idempotente: chamadas repetidas nao duplicam handlers.

    Args:
        level: nivel textual; se None usa `settings.log_level`.
        log_file: nome do arquivo de log dentro de `settings.log_dir`.
        settings: configuracao; se None usa `get_settings()`.

    Returns:
        Logger raiz da aplicacao ("agro_intel").
    """
    cfg = settings or get_settings()
    logger = logging.getLogger(ROOT_LOGGER_NAME)
    logger.setLevel(getattr(logging, (level or cfg.log_level).upper(), logging.INFO))
    logger.propagate = False

    if logger.handlers:
        return logger

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    try:
        log_dir = cfg.log_dir_path
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except OSError as exc:  # nao bloqueia a execucao: segue apenas com console
        logger.warning("Nao foi possivel criar o arquivo de log: %s", exc)

    return logger


def get_logger(name: str) -> Logger:
    """Retorna um logger filho do logger raiz da aplicacao.

    Args:
        name: sufixo do logger (ex.: "ingestion.finance").

    Returns:
        Logger nomeado "agro_intel.<name>".
    """
    return logging.getLogger(f"{ROOT_LOGGER_NAME}.{name}")
