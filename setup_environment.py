"""ETAPA 1 - Bootstrap do ambiente local da plataforma de Inteligencia
Climatica e Financeira para o Agronegocio.

Responsabilidades deste script:
    1. Criar a arvore de pastas do Data Lake local simulando os buckets S3
       (Medallion Architecture: raw -> processed).
    2. Validar se as dependencias criticas de ingestao estao instaladas e
       funcionais (nao apenas importaveis: checa GDAL, HDF5/NetCDF, etc.).
    3. Validar a presenca das credenciais locais no arquivo .env.
    4. Emitir log estruturado (console + arquivo) simulando CloudWatch.

Uso:
    python setup_environment.py
    python setup_environment.py --network-check          # testa yfinance/CHIRPS
    python setup_environment.py --root "D:/lake" -v      # raiz alternativa
    python setup_environment.py --write-cdsapirc         # gera ~/.cdsapirc

Codigos de saida:
    0 = ambiente pronto        1 = falha bloqueante (dependencia/pasta)
    2 = ambiente criado, porem com pendencias de credenciais/opcionais
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import platform
import sys
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from importlib import metadata
from logging import Logger
from pathlib import Path
from typing import Final

# -----------------------------------------------------------------------------
# Constantes de configuracao
# -----------------------------------------------------------------------------
PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parent
DEFAULT_LAKE_DIRNAME: Final[str] = "data_lake"
LOGGER_NAME: Final[str] = "agro_intel.setup"

#: Estrutura do Data Lake local. Cada caminho equivale a um "prefixo" no S3.
DATA_LAKE_STRUCTURE: Final[tuple[str, ...]] = (
    "raw/finance",
    "raw/climate_chirps",
    "raw/climate_era5",
    "processed/finance",
    "processed/climate",
)

#: Pastas auxiliares do projeto (fora das camadas do lake).
SUPPORT_DIRECTORIES: Final[tuple[str, ...]] = (
    "logs",
    "src",
    "notebooks",
    "tests",
)

#: modulo importavel -> nome da distribuicao no PyPI (bloqueantes).
REQUIRED_PACKAGES: Final[dict[str, str]] = {
    "pandas": "pandas",
    "numpy": "numpy",
    "pyarrow": "pyarrow",
    "requests": "requests",
    "dotenv": "python-dotenv",
    "yfinance": "yfinance",
    "rasterio": "rasterio",
    "rioxarray": "rioxarray",
    "xarray": "xarray",
    "netCDF4": "netCDF4",
    "cdsapi": "cdsapi",
}

#: Nao bloqueiam a Etapa 1, mas serao usados nas etapas seguintes.
OPTIONAL_PACKAGES: Final[dict[str, str]] = {
    "boto3": "boto3",
    "geopandas": "geopandas",
    "shapely": "shapely",
    "dask": "dask",
    "sqlalchemy": "SQLAlchemy",
    "matplotlib": "matplotlib",
}

#: Variaveis de ambiente obrigatorias para a ingestao climatica/financeira.
REQUIRED_ENV_VARS: Final[tuple[str, ...]] = (
    "STORAGE_BACKEND",
    "DATA_LAKE_ROOT",
    "CDSAPI_URL",
    "CDSAPI_KEY",
    "CHIRPS_BASE_URL",
    "FINANCE_TICKERS",
)

#: Obrigatorias apenas quando STORAGE_BACKEND=s3 (futuro deploy AWS).
AWS_ENV_VARS: Final[tuple[str, ...]] = (
    "AWS_REGION",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "S3_BUCKET_RAW",
    "S3_BUCKET_PROCESSED",
)

#: Valores placeholder que devem ser tratados como "nao configurado".
PLACEHOLDER_VALUES: Final[frozenset[str]] = frozenset(
    {"", "cole_seu_token_pessoal_aqui", "changeme", "todo", "none"}
)


# -----------------------------------------------------------------------------
# Modelos de resultado
# -----------------------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class DependencyStatus:
    """Resultado da checagem de um pacote Python."""

    module: str
    distribution: str
    installed: bool
    version: str | None = None
    error: str | None = None


@dataclass(slots=True)
class EnvironmentReport:
    """Agrega o resultado completo do bootstrap do ambiente."""

    lake_root: Path
    created_dirs: list[Path] = field(default_factory=list)
    existing_dirs: list[Path] = field(default_factory=list)
    missing_required: list[DependencyStatus] = field(default_factory=list)
    missing_optional: list[DependencyStatus] = field(default_factory=list)
    failed_smoke_tests: list[str] = field(default_factory=list)
    missing_env_vars: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def is_blocking_failure(self) -> bool:
        """True quando o ambiente nao esta apto a rodar a ingestao."""
        return bool(self.missing_required or self.failed_smoke_tests)

    @property
    def has_pending_items(self) -> bool:
        """True quando ha pendencias nao bloqueantes (credenciais/opcionais)."""
        return bool(self.missing_env_vars or self.missing_optional or self.warnings)


# -----------------------------------------------------------------------------
# Logging estruturado (simula CloudWatch: stdout + arquivo)
# -----------------------------------------------------------------------------
def configure_logging(log_dir: Path, level: str = "INFO") -> Logger:
    """Configura o logger da aplicacao com saida em console e arquivo.

    Args:
        log_dir: diretorio onde o arquivo de log sera gravado.
        level: nivel de log textual (DEBUG, INFO, WARNING, ERROR).

    Returns:
        Logger configurado e pronto para uso.
    """
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False

    if logger.handlers:  # idempotente: evita handlers duplicados
        return logger

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(funcName)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.FileHandler(
        log_dir / "setup_environment.log", mode="a", encoding="utf-8"
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger



# -----------------------------------------------------------------------------
# Etapa A - Estrutura do Data Lake local (espelho dos prefixos S3)
# -----------------------------------------------------------------------------
def resolve_lake_root(cli_root: str | None, project_root: Path = PROJECT_ROOT) -> Path:
    """Resolve a raiz do Data Lake (CLI > variavel de ambiente > default).

    Args:
        cli_root: valor recebido via argumento de linha de comando.
        project_root: diretorio base do projeto.

    Returns:
        Caminho absoluto da raiz do Data Lake.
    """
    raw_value = cli_root or os.getenv("DATA_LAKE_ROOT") or DEFAULT_LAKE_DIRNAME
    candidate = Path(raw_value).expanduser()
    return candidate if candidate.is_absolute() else (project_root / candidate).resolve()


def create_directory_tree(
    base_path: Path,
    relative_paths: Sequence[str],
    logger: Logger,
    add_gitkeep: bool = True,
) -> tuple[list[Path], list[Path]]:
    """Cria (idempotentemente) uma arvore de diretorios sob `base_path`.

    Args:
        base_path: diretorio raiz onde a arvore sera criada.
        relative_paths: caminhos relativos no formato "camada/dominio".
        logger: logger para rastreabilidade.
        add_gitkeep: cria um .gitkeep para versionar a pasta vazia no Git.

    Returns:
        Tupla (diretorios_criados, diretorios_ja_existentes).

    Raises:
        OSError: se a criacao de algum diretorio falhar (permissao, disco, etc.).
    """
    created: list[Path] = []
    existing: list[Path] = []

    for relative in relative_paths:
        target = base_path / relative
        try:
            if target.is_dir():
                existing.append(target)
                logger.debug("Diretorio ja existente: %s", target)
            else:
                target.mkdir(parents=True, exist_ok=True)
                created.append(target)
                logger.info("Diretorio criado: %s", target)

            if add_gitkeep:
                gitkeep = target / ".gitkeep"
                if not gitkeep.exists():
                    gitkeep.touch()
                    logger.debug("Marcador .gitkeep criado em %s", target)
        except OSError as exc:
            logger.error("Falha ao criar diretorio %s: %s", target, exc)
            raise

    return created, existing


def write_lake_manifest(lake_root: Path, logger: Logger) -> Path:
    """Escreve um README na raiz do lake documentando o mapeamento local -> S3.

    Args:
        lake_root: raiz do Data Lake local.
        logger: logger para rastreabilidade.

    Returns:
        Caminho do arquivo de manifesto gravado.
    """
    manifest = lake_root / "README.md"
    bucket_raw = os.getenv("S3_BUCKET_RAW", "agro-intel-raw")
    bucket_processed = os.getenv("S3_BUCKET_PROCESSED", "agro-intel-processed")
    content = (
        "# Data Lake local (simulacao AWS S3)\n\n"
        "Gerado automaticamente por `setup_environment.py`.\n\n"
        "| Caminho local | Equivalente na AWS |\n"
        "| --- | --- |\n"
        f"| `raw/finance/` | `s3://{bucket_raw}/finance/` |\n"
        f"| `raw/climate_chirps/` | `s3://{bucket_raw}/climate_chirps/` |\n"
        f"| `raw/climate_era5/` | `s3://{bucket_raw}/climate_era5/` |\n"
        f"| `processed/finance/` | `s3://{bucket_processed}/finance/` |\n"
        f"| `processed/climate/` | `s3://{bucket_processed}/climate/` |\n\n"
        "Camada `raw`: bytes originais imutaveis (CSV/JSON, GeoTIFF, NetCDF).\n"
        "Camada `processed`: dados limpos e particionados em Parquet.\n"
    )
    try:
        manifest.write_text(content, encoding="utf-8")
        logger.info("Manifesto do lake atualizado: %s", manifest)
    except OSError as exc:  # nao bloqueante
        logger.warning("Nao foi possivel gravar o manifesto %s: %s", manifest, exc)
    return manifest



# -----------------------------------------------------------------------------
# Etapa B - Validacao de dependencias
# -----------------------------------------------------------------------------
def check_dependency(module_name: str, distribution: str) -> DependencyStatus:
    """Verifica se um pacote pode ser importado e retorna sua versao.

    Args:
        module_name: nome do modulo a importar (ex.: "netCDF4").
        distribution: nome da distribuicao no PyPI (ex.: "netCDF4").

    Returns:
        DependencyStatus com o resultado da checagem (nunca lanca excecao).
    """
    try:
        module = importlib.import_module(module_name)
    except Exception as exc:  # ImportError, OSError (DLL/GDAL), etc.
        return DependencyStatus(
            module=module_name,
            distribution=distribution,
            installed=False,
            error=f"{type(exc).__name__}: {exc}",
        )

    version = getattr(module, "__version__", None)
    if version is None:
        try:
            version = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            version = "desconhecida"

    return DependencyStatus(
        module=module_name,
        distribution=distribution,
        installed=True,
        version=str(version),
    )


def validate_dependencies(
    packages: dict[str, str], logger: Logger, label: str
) -> list[DependencyStatus]:
    """Valida um conjunto de pacotes e loga o resultado individual.

    Args:
        packages: mapeamento modulo -> distribuicao.
        logger: logger para rastreabilidade.
        label: rotulo do grupo ("obrigatorias" / "opcionais").

    Returns:
        Lista apenas com os pacotes ausentes/com falha de import.
    """
    logger.info("Validando dependencias %s (%d pacotes)...", label, len(packages))
    missing: list[DependencyStatus] = []

    for module_name, distribution in packages.items():
        status = check_dependency(module_name, distribution)
        if status.installed:
            logger.info("  [OK]    %-12s v%s", status.module, status.version)
        else:
            missing.append(status)
            level = logger.error if label == "obrigatorias" else logger.warning
            level("  [FALHA] %-12s -> %s", status.module, status.error)

    return missing


def run_smoke_tests(logger: Logger) -> list[str]:
    """Executa testes funcionais rapidos nas bibliotecas criticas.

    Valida bindings nativos (GDAL, HDF5/NetCDF, Arrow) e o accessor `.rio`,
    que sao as fontes mais comuns de falha em ambientes Windows.

    Args:
        logger: logger para rastreabilidade.

    Returns:
        Lista de descricoes dos testes que falharam (vazia = tudo ok).
    """
    failures: list[str] = []

    # 1. GDAL via rasterio (CHIRPS GeoTIFF)
    try:
        import rasterio
        from rasterio.crs import CRS

        crs = CRS.from_epsg(4326)
        logger.info(
            "  [OK]    rasterio/GDAL %s | proj4 do EPSG:4326 resolvido (%s)",
            rasterio.__gdal_version__,
            crs.linear_units or "degree",
        )
    except Exception as exc:
        failures.append(f"rasterio/GDAL indisponivel: {type(exc).__name__}: {exc}")
        logger.error("  [FALHA] rasterio/GDAL: %s", exc)

    # 2. xarray + rioxarray + NetCDF round-trip (ERA5-Land)
    try:
        import numpy as np
        import rioxarray  # noqa: F401  (registra o accessor .rio)
        import xarray as xr

        data = xr.DataArray(
            np.arange(9, dtype="float32").reshape(3, 3),
            dims=("y", "x"),
            coords={"y": [-1.0, 0.0, 1.0], "x": [-1.0, 0.0, 1.0]},
            name="tp",
        )
        data = data.rio.write_crs("EPSG:4326")

        with tempfile.TemporaryDirectory(prefix="agro_intel_smoke_") as tmp:
            nc_path = Path(tmp) / "smoke.nc"
            data.to_dataset().to_netcdf(nc_path, engine="netcdf4")
            with xr.open_dataset(nc_path, engine="netcdf4") as reopened:
                assert "tp" in reopened.data_vars

        import netCDF4

        logger.info(
            "  [OK]    xarray/rioxarray/netCDF4 | libnetcdf %s | round-trip .nc ok",
            netCDF4.__netcdf4libversion__,
        )
    except Exception as exc:
        failures.append(f"stack NetCDF (xarray/rioxarray/netCDF4): {exc}")
        logger.error("  [FALHA] stack NetCDF: %s", exc)

    # 3. pandas + pyarrow round-trip (camada processed em Parquet)
    try:
        import pandas as pd

        frame = pd.DataFrame(
            {"date": pd.to_datetime(["2024-01-02", "2024-01-03"]), "close": [1.0, 2.0]}
        )
        with tempfile.TemporaryDirectory(prefix="agro_intel_smoke_") as tmp:
            parquet_path = Path(tmp) / "smoke.parquet"
            frame.to_parquet(parquet_path, engine="pyarrow", index=False)
            assert len(pd.read_parquet(parquet_path)) == 2
        logger.info("  [OK]    pandas/pyarrow | round-trip .parquet ok")
    except Exception as exc:
        failures.append(f"stack Parquet (pandas/pyarrow): {exc}")
        logger.error("  [FALHA] stack Parquet: %s", exc)

    # 4. Clientes de ingestao (apenas instanciacao, sem rede)
    try:
        import cdsapi
        import yfinance as yf

        assert hasattr(cdsapi, "Client") and hasattr(yf, "Ticker")
        logger.info("  [OK]    clientes de ingestao yfinance/cdsapi disponiveis")
    except Exception as exc:
        failures.append(f"clientes de ingestao (yfinance/cdsapi): {exc}")
        logger.error("  [FALHA] clientes de ingestao: %s", exc)

    return failures


# -----------------------------------------------------------------------------
# Etapa C - Credenciais locais (.env) e configuracao do cdsapi
# -----------------------------------------------------------------------------
def load_environment_file(project_root: Path, logger: Logger) -> Path | None:
    """Carrega o arquivo .env para `os.environ`, se existir.

    Args:
        project_root: diretorio onde o .env e procurado.
        logger: logger para rastreabilidade.

    Returns:
        Caminho do .env carregado ou None quando ausente/indisponivel.
    """
    env_path = project_root / ".env"
    if not env_path.is_file():
        logger.warning(
            "Arquivo .env nao encontrado em %s (copie de .env.example)", env_path
        )
        return None

    try:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_path, override=False)
        logger.info("Credenciais locais carregadas de %s", env_path)
        return env_path
    except ImportError:
        logger.warning("python-dotenv nao instalado: .env nao foi carregado")
        return None


def is_configured(value: str | None) -> bool:
    """Indica se uma variavel de ambiente possui valor real (nao placeholder)."""
    return value is not None and value.strip().lower() not in PLACEHOLDER_VALUES


def validate_env_vars(logger: Logger) -> tuple[list[str], list[str]]:
    """Valida as variaveis de ambiente exigidas pela ingestao.

    Args:
        logger: logger para rastreabilidade.

    Returns:
        Tupla (variaveis_faltantes, avisos).
    """
    missing: list[str] = []
    warnings: list[str] = []

    for name in REQUIRED_ENV_VARS:
        value = os.getenv(name)
        if is_configured(value):
            display = "***" if "KEY" in name or "PASSWORD" in name else value
            logger.info("  [OK]    %-18s = %s", name, display)
        else:
            missing.append(name)
            logger.warning("  [PEND]  %-18s nao configurada", name)

    backend = (os.getenv("STORAGE_BACKEND") or "local").strip().lower()
    if backend == "s3":
        aws_missing = [n for n in AWS_ENV_VARS if not is_configured(os.getenv(n))]
        if aws_missing:
            missing.extend(aws_missing)
            logger.error(
                "STORAGE_BACKEND=s3 exige as variaveis AWS: %s", ", ".join(aws_missing)
            )
    else:
        logger.info(
            "STORAGE_BACKEND=%s -> escrevendo no Data Lake local "
            "(troque para 's3' apos o deploy)",
            backend,
        )
        if not is_configured(os.getenv("AWS_ACCESS_KEY_ID")):
            warnings.append(
                "Credenciais AWS ausentes (esperado no modo local; obrigatorias "
                "quando STORAGE_BACKEND=s3)."
            )

    return missing, warnings


def write_cdsapirc(logger: Logger, force: bool = False) -> Path | None:
    """Gera o arquivo `~/.cdsapirc` a partir das variaveis do .env.

    Args:
        logger: logger para rastreabilidade.
        force: sobrescreve o arquivo caso ja exista.

    Returns:
        Caminho do arquivo gravado, ou None se nada foi feito.
    """
    url = os.getenv("CDSAPI_URL")
    key = os.getenv("CDSAPI_KEY")
    if not (is_configured(url) and is_configured(key)):
        logger.warning(
            "CDSAPI_URL/CDSAPI_KEY nao configurados: ~/.cdsapirc nao foi gerado"
        )
        return None

    target = Path.home() / ".cdsapirc"
    if target.exists() and not force:
        logger.info("~/.cdsapirc ja existe em %s (use --write-cdsapirc)", target)
        return target

    try:
        target.write_text(f"url: {url}\nkey: {key}\n", encoding="utf-8")
        logger.info("Arquivo de credencial do Copernicus gravado em %s", target)
        return target
    except OSError as exc:
        logger.error("Falha ao gravar %s: %s", target, exc)
        return None


# -----------------------------------------------------------------------------
# Etapa D - Checagens opcionais de rede (--network-check)
# -----------------------------------------------------------------------------
def check_network_sources(logger: Logger) -> list[str]:
    """Testa conectividade real com Yahoo Finance e CHIRPS.

    Args:
        logger: logger para rastreabilidade.

    Returns:
        Lista de avisos para as fontes inacessiveis (nunca bloqueia).
    """
    warnings: list[str] = []

    try:
        import yfinance as yf

        sample = yf.Ticker("KC=F").history(period="5d", interval="1d")
        if sample.empty:
            warnings.append("yfinance respondeu vazio para KC=F (rate limit?)")
            logger.warning("  [AVISO] yfinance: nenhum candle retornado para KC=F")
        else:
            logger.info(
                "  [OK]    yfinance KC=F | %d candles | ultimo fechamento %.2f",
                len(sample),
                float(sample["Close"].iloc[-1]),
            )
    except Exception as exc:
        warnings.append(f"yfinance inacessivel: {exc}")
        logger.warning("  [AVISO] yfinance indisponivel: %s", exc)

    try:
        import requests

        base_url = os.getenv(
            "CHIRPS_BASE_URL",
            "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05",
        )
        response = requests.head(f"{base_url}/2024/", timeout=30, allow_redirects=True)
        logger.info("  [OK]    CHIRPS %s -> HTTP %s", base_url, response.status_code)
        if response.status_code >= 400:
            warnings.append(f"CHIRPS retornou HTTP {response.status_code}")
    except Exception as exc:
        warnings.append(f"CHIRPS inacessivel: {exc}")
        logger.warning("  [AVISO] CHIRPS indisponivel: %s", exc)

    return warnings


# -----------------------------------------------------------------------------
# Etapa E - Sumario e orquestracao
# -----------------------------------------------------------------------------
def log_runtime_context(logger: Logger, lake_root: Path) -> None:
    """Registra o contexto de execucao (util para auditoria/observabilidade)."""
    logger.info("=" * 78)
    logger.info("Bootstrap do ambiente - Inteligencia Climatica e Financeira (Agro)")
    logger.info("=" * 78)
    logger.info("Python           : %s (%s)", platform.python_version(), sys.executable)
    logger.info("Sistema          : %s %s", platform.system(), platform.release())
    logger.info("Projeto          : %s", PROJECT_ROOT)
    logger.info("Data Lake        : %s", lake_root)
    logger.info(
        "Virtualenv ativo : %s",
        os.getenv("VIRTUAL_ENV") or "NAO DETECTADO (recomendado ativar o .venv)",
    )


def log_summary(report: EnvironmentReport, logger: Logger) -> None:
    """Imprime o sumario final do bootstrap no log."""
    logger.info("-" * 78)
    logger.info("SUMARIO")
    logger.info("-" * 78)
    logger.info(
        "Diretorios: %d criados | %d ja existentes",
        len(report.created_dirs),
        len(report.existing_dirs),
    )

    if report.missing_required:
        logger.error(
            "Dependencias obrigatorias ausentes: %s",
            ", ".join(s.distribution for s in report.missing_required),
        )
        logger.error("Acao: pip install -r requirements.txt")
    else:
        logger.info("Dependencias obrigatorias: todas OK")

    for failure in report.failed_smoke_tests:
        logger.error("Smoke test falhou -> %s", failure)

    if report.missing_optional:
        logger.warning(
            "Dependencias opcionais ausentes (necessarias nas proximas etapas): %s",
            ", ".join(s.distribution for s in report.missing_optional),
        )

    if report.missing_env_vars:
        logger.warning(
            "Variaveis de ambiente pendentes no .env: %s",
            ", ".join(report.missing_env_vars),
        )

    for warning in report.warnings:
        logger.warning("Aviso: %s", warning)

    if report.is_blocking_failure:
        logger.error("STATUS: AMBIENTE INCOMPLETO - corrija os itens acima.")
    elif report.has_pending_items:
        logger.warning(
            "STATUS: DATA LAKE PRONTO com pendencias nao bloqueantes. "
            "Preencha o .env antes da ingestao do ERA5."
        )
    else:
        logger.info("STATUS: AMBIENTE 100%% PRONTO - siga para a Etapa 2.")
    logger.info("-" * 78)


def build_arg_parser() -> argparse.ArgumentParser:
    """Cria o parser de argumentos da CLI."""
    parser = argparse.ArgumentParser(
        prog="setup_environment.py",
        description=(
            "Cria a estrutura do Data Lake local (simulacao S3) e valida as "
            "dependencias/credenciais da plataforma de inteligencia agro."
        ),
    )
    parser.add_argument(
        "--root",
        default=None,
        help="Raiz do Data Lake (default: DATA_LAKE_ROOT do .env ou ./data_lake).",
    )
    parser.add_argument(
        "--skip-deps",
        action="store_true",
        help="Pula a validacao de dependencias e os smoke tests.",
    )
    parser.add_argument(
        "--network-check",
        action="store_true",
        help="Testa conectividade real com Yahoo Finance e CHIRPS.",
    )
    parser.add_argument(
        "--write-cdsapirc",
        action="store_true",
        help="Gera/sobrescreve ~/.cdsapirc com CDSAPI_URL e CDSAPI_KEY do .env.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Habilita log em nivel DEBUG.",
    )
    return parser


def bootstrap_environment(args: argparse.Namespace) -> EnvironmentReport:
    """Executa o bootstrap completo do ambiente local.

    Args:
        args: argumentos ja parseados da CLI.

    Returns:
        EnvironmentReport consolidado.

    Raises:
        OSError: se a criacao das pastas do Data Lake falhar.
    """
    log_dir = PROJECT_ROOT / (os.getenv("LOG_DIR") or "logs")
    level = "DEBUG" if args.verbose else (os.getenv("LOG_LEVEL") or "INFO")
    logger = configure_logging(log_dir=log_dir, level=level)

    load_environment_file(PROJECT_ROOT, logger)
    if args.verbose:  # o .env pode redefinir LOG_LEVEL apos o carregamento
        logger.setLevel(logging.DEBUG)

    lake_root = resolve_lake_root(args.root)
    log_runtime_context(logger, lake_root)
    report = EnvironmentReport(lake_root=lake_root)

    logger.info("[1/4] Criando estrutura do Data Lake local...")
    created, existing = create_directory_tree(lake_root, DATA_LAKE_STRUCTURE, logger)
    report.created_dirs.extend(created)
    report.existing_dirs.extend(existing)

    support_created, support_existing = create_directory_tree(
        PROJECT_ROOT, SUPPORT_DIRECTORIES, logger, add_gitkeep=False
    )
    report.created_dirs.extend(support_created)
    report.existing_dirs.extend(support_existing)
    write_lake_manifest(lake_root, logger)

    if args.skip_deps:
        logger.warning("[2/4] Validacao de dependencias ignorada (--skip-deps)")
    else:
        logger.info("[2/4] Validando dependencias...")
        report.missing_required = validate_dependencies(
            REQUIRED_PACKAGES, logger, "obrigatorias"
        )
        report.missing_optional = validate_dependencies(
            OPTIONAL_PACKAGES, logger, "opcionais"
        )
        if report.missing_required:
            logger.warning("Smoke tests ignorados: dependencias obrigatorias ausentes")
        else:
            logger.info("Executando smoke tests funcionais (GDAL/NetCDF/Arrow)...")
            report.failed_smoke_tests = run_smoke_tests(logger)

    logger.info("[3/4] Validando credenciais locais (.env)...")
    missing_env, env_warnings = validate_env_vars(logger)
    report.missing_env_vars = missing_env
    report.warnings.extend(env_warnings)

    if args.write_cdsapirc:
        write_cdsapirc(logger, force=True)

    if args.network_check:
        logger.info("[4/4] Testando conectividade com as fontes externas...")
        report.warnings.extend(check_network_sources(logger))
    else:
        logger.info("[4/4] Checagem de rede ignorada (use --network-check)")

    log_summary(report, logger)
    return report


def main(argv: Sequence[str] | None = None) -> int:
    """Ponto de entrada do script.

    Args:
        argv: lista de argumentos (default: sys.argv[1:]).

    Returns:
        Codigo de saida do processo (0 = ok, 1 = falha, 2 = pendencias).
    """
    args = build_arg_parser().parse_args(argv)
    try:
        report = bootstrap_environment(args)
    except OSError as exc:
        logging.getLogger(LOGGER_NAME).critical(
            "Falha de I/O ao preparar o ambiente: %s", exc
        )
        return 1
    except Exception as exc:  # rede de seguranca: nunca falhar silenciosamente
        logging.getLogger(LOGGER_NAME).critical(
            "Erro inesperado no bootstrap: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        return 1

    if report.is_blocking_failure:
        return 1
    if report.has_pending_items:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

