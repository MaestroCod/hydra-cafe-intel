"""ETAPA 3 - Ingestao ERA5-Land (reanalise horaria) -> camada raw.

Pipeline:

    cdsapi.retrieve(area=N,W,S,E)  ->  arquivo .nc temporario local
    ->  validacao com xarray       ->  StorageBackend  ->  remocao do temporario

Destino:

    raw/climate_era5/dt=YYYY-MM-DD/era5_land_brazil.nc

O recorte espacial e feito no SERVIDOR (parametro `area` do CDS), o que reduz
drasticamente o volume transferido: apenas o retangulo do Brasil e baixado.

AVISO IMPORTANTE SOBRE VARIAVEIS
    `maximum_2m_temperature_since_previous_post_processing` e
    `minimum_2m_temperature_since_previous_post_processing` NAO existem no
    dataset `reanalysis-era5-land` (pertencem ao `reanalysis-era5-single-levels`).
    Como o ERA5-Land e horario, tmax/tmin diarios sao derivados das 24 horas de
    `2m_temperature`. Este modulo resolve automaticamente esses nomes para
    `2m_temperature`, registrando um WARNING.

Uso:
    python -m src.ingestion.era5                      # hoje - ERA5_LAG_DAYS
    python -m src.ingestion.era5 --date 2026-08-10
    python -m src.ingestion.era5 --start 2026-08-01 --end 2026-08-03
    python -m src.ingestion.era5 --dry-run            # mostra o payload do CDS
"""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Final, Literal
from uuid import uuid4

from src.config import (
    RAW_ERA5_PREFIX,
    ConfigError,
    Settings,
    configure_logging,
    get_logger,
    get_settings,
)
from src.ingestion.chirps import iter_dates  # reuso do gerador de datas
from src.ingestion.retry import retry_call
from src.storage import StorageBackend, StorageError, get_storage

logger = get_logger("ingestion.era5")

SOURCE_NAME: Final[str] = "copernicus_cds_era5_land"
OUTPUT_FILENAME: Final[str] = "era5_land_brazil.nc"
NETCDF_CONTENT_TYPE: Final[str] = "application/x-netcdf"
#: ERA5-Land e horario: 24 passos por dia.
ALL_HOURS: Final[tuple[str, ...]] = tuple(f"{hour:02d}:00" for hour in range(24))

#: Variaveis validas do `reanalysis-era5-land` (conforme form.json do CDS).
ERA5_LAND_KNOWN_VARIABLES: Final[frozenset[str]] = frozenset(
    {
        # Temperatura
        "2m_dewpoint_temperature",
        "2m_temperature",
        "skin_temperature",
        "soil_temperature_level_1",
        "soil_temperature_level_2",
        "soil_temperature_level_3",
        "soil_temperature_level_4",
        # Agua no solo
        "skin_reservoir_content",
        "volumetric_soil_water_layer_1",
        "volumetric_soil_water_layer_2",
        "volumetric_soil_water_layer_3",
        "volumetric_soil_water_layer_4",
        # Evaporacao e escoamento
        "evaporation_from_bare_soil",
        "evaporation_from_open_water_surfaces_excluding_oceans",
        "evaporation_from_the_top_of_canopy",
        "evaporation_from_vegetation_transpiration",
        "potential_evaporation",
        "runoff",
        "snow_evaporation",
        "sub_surface_runoff",
        "surface_runoff",
        "total_evaporation",
        "total_precipitation",
        # Radiacao e fluxos
        "forecast_albedo",
        "surface_latent_heat_flux",
        "surface_net_solar_radiation",
        "surface_net_thermal_radiation",
        "surface_sensible_heat_flux",
        "surface_solar_radiation_downwards",
        "surface_thermal_radiation_downwards",
        # Vento, pressao e vegetacao
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "surface_pressure",
        "leaf_area_index_high_vegetation",
        "leaf_area_index_low_vegetation",
    }
)

#: Nomes do ERA5 single-levels que precisam ser derivados no ERA5-Land.
ERA5_LAND_VARIABLE_ALIASES: Final[dict[str, str]] = {
    "maximum_2m_temperature_since_previous_post_processing": "2m_temperature",
    "minimum_2m_temperature_since_previous_post_processing": "2m_temperature",
    "2m_temperature_max": "2m_temperature",
    "2m_temperature_min": "2m_temperature",
}

IngestionStatus = Literal["written", "skipped", "failed"]


class Era5IngestionError(RuntimeError):
    """Falha irrecuperavel na ingestao de um arquivo ERA5-Land."""


class Era5LicenceError(Era5IngestionError):
    """A licenca do dataset ainda nao foi aceita na conta do Copernicus CDS."""


@dataclass(frozen=True, slots=True)
class NetCdfStats:
    """Metadados extraidos do NetCDF baixado, usados para QA e auditoria.

    Attributes:
        variables: nomes das variaveis de dados encontradas no arquivo.
        dimensions: mapeamento dimensao -> tamanho.
        time_steps: numero de passos temporais (esperado 24 para 1 dia).
        bounds: extensao (min_lon, min_lat, max_lon, max_lat) do arquivo.
        size_bytes: tamanho do arquivo em bytes.
    """

    variables: tuple[str, ...]
    dimensions: dict[str, int]
    time_steps: int
    bounds: tuple[float, float, float, float] | None
    size_bytes: int


@dataclass(frozen=True, slots=True)
class Era5IngestionResult:
    """Resultado da ingestao de uma data do ERA5-Land."""

    target_date: date
    status: IngestionStatus
    key: str | None = None
    uri: str | None = None
    size_bytes: int = 0
    variables: tuple[str, ...] = ()
    stats: NetCdfStats | None = None
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        """True quando o objeto foi gravado ou ja existia no lake."""
        return self.status in ("written", "skipped")


@dataclass(frozen=True, slots=True)
class Era5IngestionRun:
    """Consolidado de um batch de datas do ERA5-Land."""

    run_id: str
    results: tuple[Era5IngestionResult, ...] = field(default_factory=tuple)

    @property
    def failures(self) -> tuple[Era5IngestionResult, ...]:
        """Datas que falharam por erro tecnico."""
        return tuple(r for r in self.results if r.status == "failed")

    @property
    def total_bytes(self) -> int:
        """Soma dos bytes gravados no batch."""
        return sum(r.size_bytes for r in self.results)


# -----------------------------------------------------------------------------
# Variaveis, request e chaves
# -----------------------------------------------------------------------------
def resolve_variables(
    requested: Iterable[str], dataset: str = "reanalysis-era5-land"
) -> tuple[str, ...]:
    """Normaliza a lista de variaveis para nomes aceitos pelo dataset.

    Aplica os aliases de `ERA5_LAND_VARIABLE_ALIASES` (tmax/tmin -> 2m_temperature)
    e remove duplicatas preservando a ordem. Variaveis desconhecidas geram
    WARNING mas sao mantidas, permitindo usar datasets diferentes sem alterar
    este modulo.

    Args:
        requested: nomes de variaveis solicitados (ex.: do .env).
        dataset: dataset alvo do CDS (os aliases so se aplicam ao ERA5-Land).

    Returns:
        Tupla de variaveis unicas e ordenadas conforme a solicitacao.

    Raises:
        Era5IngestionError: se a lista resultante ficar vazia.
    """
    resolved: dict[str, None] = {}
    is_era5_land = dataset == "reanalysis-era5-land"

    for raw_name in requested:
        name = raw_name.strip()
        if not name:
            continue
        if is_era5_land and name in ERA5_LAND_VARIABLE_ALIASES:
            target = ERA5_LAND_VARIABLE_ALIASES[name]
            logger.warning(
                "Variavel %s nao existe em %s; usando %s horario (tmax/tmin serao "
                "derivados das 24 horas na camada processed)",
                name,
                dataset,
                target,
            )
            resolved.setdefault(target, None)
            continue
        if is_era5_land and name not in ERA5_LAND_KNOWN_VARIABLES:
            logger.warning(
                "Variavel %s nao consta na lista conhecida de %s; sera enviada ao "
                "CDS como esta",
                name,
                dataset,
            )
        resolved.setdefault(name, None)

    if not resolved:
        raise Era5IngestionError("Nenhuma variavel valida informada para o ERA5")

    variables = tuple(resolved)
    logger.info(
        "Variaveis resolvidas | solicitadas=%d | efetivas=%d | %s",
        len(list(requested)) if isinstance(requested, (list, tuple)) else -1,
        len(variables),
        ", ".join(variables),
    )
    return variables


def build_request(
    target_date: date,
    variables: Sequence[str],
    area: tuple[float, float, float, float],
    data_format: str = "netcdf",
    download_format: str = "unarchived",
    hours: Sequence[str] = ALL_HOURS,
) -> dict[str, Any]:
    """Monta o dicionario de requisicao do CDS API.

    Args:
        target_date: data desejada (1 dia por arquivo).
        variables: variaveis ja resolvidas.
        area: recorte no formato do CDS (North, West, South, East).
        data_format: "netcdf" ou "grib".
        download_format: "unarchived" ou "zip".
        hours: horas a requisitar (default: as 24 horas).

    Returns:
        Dicionario pronto para `cdsapi.Client.retrieve`.

    Example:
        >>> req = build_request(date(2026, 8, 10), ["2m_temperature"],
        ...                     (5.27, -73.98, -33.75, -28.85))
        >>> req["day"], req["area"]
        (['10'], [5.27, -73.98, -33.75, -28.85])
    """
    return {
        "variable": list(variables),
        "year": [f"{target_date.year:04d}"],
        "month": [f"{target_date.month:02d}"],
        "day": [f"{target_date.day:02d}"],
        "time": list(hours),
        "area": list(area),
        "data_format": data_format,
        "download_format": download_format,
    }


def build_object_key(
    target_date: date,
    prefix: str = RAW_ERA5_PREFIX,
    filename: str = OUTPUT_FILENAME,
) -> str:
    """Monta a chave particionada do objeto na camada raw (1 dia por arquivo).

    Example:
        >>> build_object_key(date(2026, 8, 10))
        'raw/climate_era5/dt=2026-08-10/era5_land_brazil.nc'
    """
    return StorageBackend.join_key(prefix, f"dt={target_date.isoformat()}", filename)


def build_month_request(
    year: int,
    month: int,
    variables: Sequence[str],
    area: tuple[float, float, float, float],
    data_format: str = "netcdf",
    download_format: str = "unarchived",
    hours: Sequence[str] = ALL_HOURS,
) -> dict[str, Any]:
    """Monta a requisicao mensal do CDS (todos os dias do mes de uma vez).

    O backfill de 1 ano usa 12 requisicoes mensais em vez de 365 diarias,
    reduzindo drasticamente o tempo na fila do Copernicus CDS.

    Args:
        year: ano (ex.: 2025).
        month: mes 1-12.
        variables: variaveis resolvidas.
        area: recorte no formato do CDS (North, West, South, East).
        data_format: "netcdf" ou "grib".
        download_format: "unarchived" ou "zip".
        hours: horas a requisitar (default: as 24 horas).

    Returns:
        Dicionario pronto para `cdsapi.Client.retrieve`.
    """
    import calendar

    dias = [f"{dia:02d}" for dia in range(1, calendar.monthrange(year, month)[1] + 1)]
    return {
        "variable": list(variables),
        "year": [f"{year:04d}"],
        "month": [f"{month:02d}"],
        "day": dias,
        "time": list(hours),
        "area": list(area),
        "data_format": data_format,
        "download_format": download_format,
    }


def build_month_key(
    year: int,
    month: int,
    prefix: str = RAW_ERA5_PREFIX,
    filename: str = OUTPUT_FILENAME,
) -> str:
    """Monta a chave do arquivo mensal na camada raw.

    Example:
        >>> build_month_key(2025, 8)
        'raw/climate_era5/month=2025-08/era5_land_brazil.nc'
    """
    return StorageBackend.join_key(prefix, f"month={year:04d}-{month:02d}", filename)


def iter_months(start: date, end: date) -> Iterator[tuple[int, int]]:
    """Itera pelos meses (ano, mes) entre `start` e `end` (inclusive)."""
    cursor = date(start.year, start.month, 1)
    fim = date(end.year, end.month, 1)
    while cursor <= fim:
        yield cursor.year, cursor.month
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)


def default_target_date(settings: Settings) -> date:
    """Data default da ingestao: hoje (UTC) menos a latencia do ERA5-Land."""
    return datetime.now(tz=UTC).date() - timedelta(days=settings.era5_lag_days)


# -----------------------------------------------------------------------------
# Cliente CDS e download
# -----------------------------------------------------------------------------
def create_client(settings: Settings, quiet: bool = True) -> Any:
    """Cria o cliente `cdsapi.Client` com as credenciais do .env.

    Args:
        settings: configuracao com `cdsapi_url` e `cdsapi_key`.
        quiet: silencia o log interno do cdsapi (usamos nosso logger).

    Returns:
        Instancia de `cdsapi.Client`.

    Raises:
        Era5IngestionError: se o cdsapi nao estiver instalado ou as credenciais
            estiverem ausentes/invalidas.
    """
    try:
        settings.require("cdsapi_url", "cdsapi_key")
    except ConfigError as exc:
        raise Era5IngestionError(
            "Credenciais do Copernicus ausentes: preencha CDSAPI_URL e CDSAPI_KEY "
            "no .env (https://cds.climate.copernicus.eu/profile)"
        ) from exc

    # O cdsapi 0.7+ aceita a chave do novo portal CDS como UUID puro (sem "UID:"):
    # quando nao ha ":", ele instancia o LegacyClient (ecmwf-datastores) que
    # autentica via Bearer no endpoint /retrieve/v1/processes/.../execution.
    try:
        import cdsapi
    except ImportError as exc:  # pragma: no cover
        raise Era5IngestionError("cdsapi nao instalado") from exc

    try:
        return cdsapi.Client(
            url=settings.cdsapi_url,
            key=settings.cdsapi_key,
            quiet=quiet,
            progress=False,
            wait_until_complete=True,
        )
    except Exception as exc:
        raise Era5IngestionError(f"Falha ao criar o cliente CDS: {exc}") from exc


def download_to_temp(
    client: Any,
    dataset: str,
    request: dict[str, Any],
    destination: Path,
    max_retries: int = 3,
) -> Path:
    """Executa o `retrieve` do CDS gravando em um arquivo temporario local.

    O cdsapi so escreve em disco (nao expoe um stream em memoria), por isso o
    arquivo temporario e obrigatorio aqui; ele e removido apos o upload.

    Args:
        client: cliente cdsapi.
        dataset: nome do dataset (ex.: "reanalysis-era5-land").
        request: payload da requisicao.
        destination: caminho do arquivo de saida.
        max_retries: tentativas em caso de falha transitoria da fila do CDS.

    Returns:
        Caminho do arquivo baixado.

    Raises:
        Era5IngestionError: se o download falhar ou o arquivo vier vazio.
        Era5LicenceError: se a licenca do dataset nao estiver aceita.
    """
    import requests

    def _retrieve() -> Path:
        try:
            client.retrieve(dataset, request, str(destination))
        except requests.HTTPError as exc:
            corpo = str(getattr(exc.response, "text", "") or exc)
            if "licence" in corpo.lower():
                raise Era5LicenceError(
                    "Licenca do dataset nao aceita na conta do Copernicus. "
                    "Acesse e aceite os termos em: "
                    "https://cds.climate.copernicus.eu/datasets/"
                    + dataset
                    + "?tab=download#manage-licences"
                ) from exc
            raise
        if not destination.is_file() or destination.stat().st_size == 0:
            raise Era5IngestionError(
                f"CDS retornou arquivo vazio para {request.get('day')}"
            )
        return destination

    path = retry_call(
        _retrieve,
        description=f"retrieve {dataset} {request.get('year')}-{request.get('month')}",
        attempts=max_retries,
        backoff_seconds=5.0,
        exceptions=(Exception,),
        # Licenca nao aceita nao muda com retentativas: aborta na hora.
        giveup=lambda exc: isinstance(exc, Era5LicenceError),
        logger=logger,
    )
    logger.info(
        "Download CDS concluido | arquivo=%s | bytes=%d",
        path.name,
        path.stat().st_size,
    )
    return path


def inspect_netcdf(path: Path) -> NetCdfStats:
    """Valida e extrai metadados do NetCDF baixado.

    Args:
        path: caminho do arquivo `.nc`.

    Returns:
        NetCdfStats com variaveis, dimensoes, passos temporais e bounds.

    Raises:
        Era5IngestionError: se o arquivo nao puder ser aberto ou nao tiver
            variaveis de dados.
    """
    try:
        import xarray as xr

        with xr.open_dataset(path) as dataset:
            variables = tuple(str(name) for name in dataset.data_vars)
            dimensions = {str(k): int(v) for k, v in dataset.sizes.items()}
            time_dim = next(
                (name for name in ("valid_time", "time") if name in dataset.sizes), None
            )
            time_steps = int(dataset.sizes[time_dim]) if time_dim else 0

            lon_name = next(
                (name for name in ("longitude", "lon", "x") if name in dataset.coords),
                None,
            )
            lat_name = next(
                (name for name in ("latitude", "lat", "y") if name in dataset.coords),
                None,
            )
            bounds: tuple[float, float, float, float] | None = None
            if lon_name and lat_name:
                bounds = (
                    float(dataset[lon_name].min()),
                    float(dataset[lat_name].min()),
                    float(dataset[lon_name].max()),
                    float(dataset[lat_name].max()),
                )
    except Era5IngestionError:
        raise
    except Exception as exc:
        raise Era5IngestionError(
            f"NetCDF invalido ({type(exc).__name__}): {exc}"
        ) from exc

    if not variables:
        raise Era5IngestionError(f"NetCDF sem variaveis de dados: {path.name}")

    stats = NetCdfStats(
        variables=variables,
        dimensions=dimensions,
        time_steps=time_steps,
        bounds=bounds,
        size_bytes=path.stat().st_size,
    )
    logger.info(
        "NetCDF validado | variaveis=%s | dims=%s | passos_tempo=%d | bounds=%s",
        ",".join(stats.variables),
        stats.dimensions,
        stats.time_steps,
        tuple(round(v, 2) for v in stats.bounds) if stats.bounds else None,
    )
    return stats


# -----------------------------------------------------------------------------
# Orquestracao
# -----------------------------------------------------------------------------
def ingest_date(
    target_date: date,
    storage: StorageBackend,
    settings: Settings,
    run_id: str,
    client: Any | None = None,
    variables: Sequence[str] | None = None,
    overwrite: bool = True,
    dry_run: bool = False,
) -> Era5IngestionResult:
    """Ingere um dia do ERA5-Land: requisita, valida, envia e limpa o temporario.

    Nunca lanca excecao: erros sao devolvidos no resultado para nao interromper
    o batch.

    Args:
        target_date: data desejada.
        storage: backend de destino.
        settings: configuracao da plataforma.
        run_id: identificador da execucao.
        client: cliente cdsapi (injetavel para testes); None cria um novo.
        variables: variaveis a requisitar; None usa as do .env.
        overwrite: se False e a chave existir, marca status "skipped".
        dry_run: monta o request e loga o payload sem chamar o CDS.

    Returns:
        Era5IngestionResult com status, chave, URI e metadados do NetCDF.
    """
    key = build_object_key(target_date)
    resolved: tuple[str, ...] = ()
    workdir: Path | None = None

    try:
        if not overwrite and storage.exists(key):
            logger.info("Ingestao ignorada (objeto ja existe) | key=%s", key)
            return Era5IngestionResult(
                target_date=target_date,
                status="skipped",
                key=key,
                uri=storage.uri(key),
            )

        resolved = resolve_variables(
            variables if variables is not None else settings.era5_variables,
            dataset=settings.era5_dataset,
        )
        request = build_request(
            target_date,
            variables=resolved,
            area=settings.aoi_bbox_cds,
            data_format=settings.era5_data_format,
            download_format=settings.era5_download_format,
        )

        if dry_run:
            logger.info(
                "[DRY-RUN] Requisicao CDS nao enviada | dataset=%s | key=%s | "
                "payload=%s",
                settings.era5_dataset,
                key,
                json.dumps(request, ensure_ascii=False),
            )
            return Era5IngestionResult(
                target_date=target_date,
                status="written",
                key=key,
                uri=storage.uri(key),
                variables=resolved,
            )

        cds_client = client or create_client(settings)
        workdir = Path(tempfile.mkdtemp(prefix="era5_"))
        temp_file = workdir / f"era5_land_{target_date.isoformat()}.nc"

        download_to_temp(
            cds_client,
            dataset=settings.era5_dataset,
            request=request,
            destination=temp_file,
            max_retries=settings.era5_max_retries,
        )
        stats = inspect_netcdf(temp_file)

        metadata = {
            "source": SOURCE_NAME,
            "dataset": settings.era5_dataset,
            "reference_date": target_date.isoformat(),
            "variables": ",".join(resolved),
            "requested_variables": ",".join(settings.era5_variables),
            "time_steps": str(stats.time_steps),
            "aoi_name": settings.aoi_name,
            "aoi_bbox_cds": ",".join(str(v) for v in settings.aoi_bbox_cds),
            "ingested_at": datetime.now(tz=UTC).isoformat(),
            "run_id": run_id,
        }
        payload = temp_file.read_bytes()
        stored = storage.write_bytes(
            key, payload, content_type=NETCDF_CONTENT_TYPE, metadata=metadata
        )
        logger.info(
            "Ingestao ERA5 concluida | dt=%s | variaveis=%d -> %s",
            target_date.isoformat(),
            len(resolved),
            stored.uri,
        )
        return Era5IngestionResult(
            target_date=target_date,
            status="written",
            key=stored.key,
            uri=stored.uri,
            size_bytes=stored.size_bytes,
            variables=resolved,
            stats=stats,
        )

    except (Era5IngestionError, StorageError) as exc:
        logger.error("Ingestao ERA5 falhou | dt=%s | %s", target_date, exc)
        return Era5IngestionResult(
            target_date=target_date,
            status="failed",
            variables=resolved,
            error=str(exc),
        )
    except Exception as exc:  # rede de seguranca do batch
        logger.exception("Erro inesperado na ingestao ERA5 | dt=%s", target_date)
        return Era5IngestionResult(
            target_date=target_date,
            status="failed",
            variables=resolved,
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        if workdir is not None:
            cleanup_workdir(workdir)


def cleanup_workdir(workdir: Path) -> bool:
    """Remove o diretorio temporario usado pelo download do CDS.

    Args:
        workdir: diretorio a remover.

    Returns:
        True se removido; False se a remocao falhou (apenas loga o aviso).
    """
    try:
        shutil.rmtree(workdir, ignore_errors=False)
        logger.debug("Temporario removido | path=%s", workdir)
        return True
    except OSError as exc:
        logger.warning("Nao foi possivel remover o temporario %s: %s", workdir, exc)
        return False


def ingest_dates(
    dates: Sequence[date] | None = None,
    storage: StorageBackend | None = None,
    settings: Settings | None = None,
    client: Any | None = None,
    variables: Sequence[str] | None = None,
    overwrite: bool = True,
    dry_run: bool = False,
) -> Era5IngestionRun:
    """Ingere uma lista de datas do ERA5-Land reutilizando o mesmo cliente CDS.

    Args:
        dates: datas a processar; se None usa `default_target_date`.
        storage: backend de destino; se None resolve via factory.
        settings: configuracao; se None usa `get_settings()`.
        client: cliente cdsapi (injetavel em testes).
        variables: variaveis a requisitar; None usa as do .env.
        overwrite: sobrescreve objetos existentes.
        dry_run: nao chama o CDS nem grava no storage.

    Returns:
        Era5IngestionRun com um resultado por data.
    """
    cfg = settings or get_settings()
    backend = storage or get_storage(settings=cfg)
    targets = tuple(dates) if dates else (default_target_date(cfg),)
    run_id = uuid4().hex[:12]

    logger.info(
        "Batch ERA5 iniciado | run_id=%s | backend=%s | dataset=%s | datas=%d "
        "(%s..%s) | area_cds=%s",
        run_id,
        backend.name,
        cfg.era5_dataset,
        len(targets),
        targets[0].isoformat(),
        targets[-1].isoformat(),
        cfg.aoi_bbox_cds,
    )

    shared_client = client
    if shared_client is None and not dry_run:
        try:
            shared_client = create_client(cfg)
        except Era5IngestionError as exc:
            logger.error("Cliente CDS indisponivel: %s", exc)
            return Era5IngestionRun(
                run_id=run_id,
                results=tuple(
                    Era5IngestionResult(
                        target_date=target, status="failed", error=str(exc)
                    )
                    for target in targets
                ),
            )

    results = tuple(
        ingest_date(
            target_date=target,
            storage=backend,
            settings=cfg,
            run_id=run_id,
            client=shared_client,
            variables=variables,
            overwrite=overwrite,
            dry_run=dry_run,
        )
        for target in targets
    )

    run = Era5IngestionRun(run_id=run_id, results=results)
    log_run_summary(run)
    return run


def ingest_month(
    year: int,
    month: int,
    storage: StorageBackend,
    settings: Settings,
    run_id: str,
    client: Any | None = None,
    variables: Sequence[str] | None = None,
    overwrite: bool = True,
    dry_run: bool = False,
) -> Era5IngestionResult:
    """Ingere um mes inteiro do ERA5-Land em uma unica requisicao ao CDS.

    O arquivo mensal e gravado em `raw/climate_era5/month=YYYY-MM/`; a camada
    processed le esse arquivo e fatia dia a dia (ver `zonal_stats`).

    Args:
        year: ano do mes.
        month: mes (1-12).
        storage: backend de destino.
        settings: configuracao.
        run_id: identificador da execucao.
        client: cliente cdsapi (injetavel em testes).
        variables: variaveis a requisitar; None usa as do .env.
        overwrite: sobrescreve o arquivo existente.
        dry_run: monta o request sem chamar o CDS.

    Returns:
        Era5IngestionResult com status e metadados do NetCDF mensal.
    """
    cfg = settings or get_settings()
    key = build_month_key(year, month)
    resolved: tuple[str, ...] = ()
    workdir: Path | None = None

    try:
        if not overwrite and storage.exists(key):
            logger.info("Mes ja existente (skipped) | key=%s", key)
            return Era5IngestionResult(
                target_date=date(year, month, 1),
                status="skipped",
                key=key,
                uri=storage.uri(key),
            )

        resolved = resolve_variables(
            variables if variables is not None else cfg.era5_variables,
            dataset=cfg.era5_dataset,
        )
        request = build_month_request(
            year,
            month,
            variables=resolved,
            area=cfg.aoi_bbox_cds,
            data_format=cfg.era5_data_format,
            download_format=cfg.era5_download_format,
        )

        if dry_run:
            logger.info(
                "[DRY-RUN] Mes nao enviado | %04d-%02d | payload=%s",
                year,
                month,
                json.dumps(request, ensure_ascii=False),
            )
            return Era5IngestionResult(
                target_date=date(year, month, 1),
                status="written",
                key=key,
                uri=storage.uri(key),
                variables=resolved,
            )

        cds_client = client or create_client(cfg)
        workdir = Path(tempfile.mkdtemp(prefix="era5_"))
        temp_file = workdir / f"era5_land_{year:04d}-{month:02d}.nc"

        download_to_temp(
            cds_client,
            dataset=cfg.era5_dataset,
            request=request,
            destination=temp_file,
            max_retries=cfg.era5_max_retries,
        )
        stats = inspect_netcdf(temp_file)

        metadata = {
            "source": SOURCE_NAME,
            "dataset": cfg.era5_dataset,
            "period": f"{year:04d}-{month:02d}",
            "variables": ",".join(resolved),
            "time_steps": str(stats.time_steps),
            "aoi_name": cfg.aoi_name,
            "ingested_at": datetime.now(tz=UTC).isoformat(),
            "run_id": run_id,
        }
        payload = temp_file.read_bytes()
        stored = storage.write_bytes(
            key, payload, content_type=NETCDF_CONTENT_TYPE, metadata=metadata
        )
        logger.info(
            "Ingestao ERA5 mensal concluida | %04d-%02d | passos=%d -> %s",
            year,
            month,
            stats.time_steps,
            stored.uri,
        )
        return Era5IngestionResult(
            target_date=date(year, month, 1),
            status="written",
            key=stored.key,
            uri=stored.uri,
            size_bytes=stored.size_bytes,
            variables=resolved,
            stats=stats,
        )

    except (Era5IngestionError, StorageError) as exc:
        logger.error("Ingestao ERA5 mensal falhou | %04d-%02d | %s", year, month, exc)
        return Era5IngestionResult(
            target_date=date(year, month, 1),
            status="failed",
            variables=resolved,
            error=str(exc),
        )
    except Exception as exc:  # rede de seguranca do batch
        logger.exception("Erro inesperado | %04d-%02d", year, month)
        return Era5IngestionResult(
            target_date=date(year, month, 1),
            status="failed",
            variables=resolved,
            error=f"{type(exc).__name__}: {exc}",
        )
    finally:
        if workdir is not None:
            cleanup_workdir(workdir)


def ingest_backfill(
    lookback_days: int | None = None,
    storage: StorageBackend | None = None,
    settings: Settings | None = None,
    client: Any | None = None,
    variables: Sequence[str] | None = None,
    overwrite: bool = True,
    dry_run: bool = False,
) -> Era5IngestionRun:
    """Faz o backfill mensal do ERA5-Land (padrao: 1 ano = 12 requisicoes).

    Args:
        lookback_days: quantidade de dias de historico (default: `era5_lookback_days`).
        storage: backend de destino.
        settings: configuracao.
        client: cliente cdsapi (injetavel em testes).
        variables: variaveis a requisitar.
        overwrite: sobrescreve meses existentes.
        dry_run: nao chama o CDS.

    Returns:
        Era5IngestionRun com um resultado por mes.
    """
    cfg = settings or get_settings()
    backend = storage or get_storage(settings=cfg)
    dias = lookback_days or cfg.era5_lookback_days
    fim = default_target_date(cfg)
    inicio = fim - timedelta(days=dias - 1)
    run_id = uuid4().hex[:12]

    logger.info(
        "Backfill ERA5 mensal | run_id=%s | janela=%s..%s | meses=%d",
        run_id,
        inicio.isoformat(),
        fim.isoformat(),
        sum(1 for _ in iter_months(inicio, fim)),
    )

    shared_client = client
    if shared_client is None and not dry_run:
        shared_client = create_client(cfg)

    resultados = tuple(
        ingest_month(
            year=ano,
            month=mes,
            storage=backend,
            settings=cfg,
            run_id=run_id,
            client=shared_client,
            variables=variables,
            overwrite=overwrite,
            dry_run=dry_run,
        )
        for ano, mes in iter_months(inicio, fim)
    )
    run = Era5IngestionRun(run_id=run_id, results=resultados)
    log_run_summary(run)
    return run


def log_run_summary(run: Era5IngestionRun) -> None:
    """Registra o sumario do batch ERA5 no log estruturado."""
    logger.info("-" * 78)
    logger.info("SUMARIO DA INGESTAO ERA5-LAND | run_id=%s", run.run_id)
    for result in run.results:
        logger.info(
            "  %s | %-8s | %10d B | %s",
            result.target_date.isoformat(),
            result.status,
            result.size_bytes,
            result.uri or result.error or "-",
        )
    logger.info(
        "Total: %d datas | %d gravadas | %d falhas | %d bytes",
        len(run.results),
        sum(1 for r in run.results if r.status == "written"),
        len(run.failures),
        run.total_bytes,
    )
    logger.info("-" * 78)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    """Cria o parser de argumentos da ingestao ERA5-Land."""
    parser = argparse.ArgumentParser(
        prog="python -m src.ingestion.era5",
        description=(
            "Requisita o ERA5-Land no Copernicus CDS com recorte no servidor "
            "(area=N,W,S,E) e grava o NetCDF em raw/climate_era5/dt=YYYY-MM-DD/."
        ),
    )
    parser.add_argument("--date", default=None, help="Data unica (YYYY-MM-DD).")
    parser.add_argument("--start", default=None, help="Inicio do intervalo.")
    parser.add_argument("--end", default=None, help="Fim do intervalo (inclusive).")
    parser.add_argument(
        "--month",
        default=None,
        help="Mes unico (YYYY-MM) via requisicao mensal do CDS.",
    )
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=None,
        help="Historico em dias (ex.: 365 = 1 ano) baixado por mes. "
        "Sobrepoe ERA5_LOOKBACK_DAYS do .env.",
    )
    parser.add_argument(
        "--variables",
        default=None,
        help="Lista separada por virgulas (default: ERA5_VARIABLES do .env).",
    )
    parser.add_argument(
        "--dataset",
        default=None,
        help="Dataset do CDS (default: ERA5_DATASET, reanalysis-era5-land).",
    )
    parser.add_argument(
        "--backend",
        default=None,
        choices=("local", "s3"),
        help="Forca o backend de storage (default: STORAGE_BACKEND do .env).",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Nao regrava datas que ja existem no lake.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra o payload do CDS sem enviar a requisicao (nao consome fila).",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Log em DEBUG.")
    return parser


def resolve_dates(args: argparse.Namespace, settings: Settings) -> tuple[date, ...]:
    """Resolve as datas a processar a partir dos argumentos da CLI.

    Args:
        args: argumentos parseados (--date, --start, --end).
        settings: configuracao (para a data default).

    Returns:
        Tupla de datas ordenadas.

    Raises:
        ValueError: se as datas forem invalidas ou o intervalo estiver invertido.
    """
    if args.date:
        return (date.fromisoformat(args.date),)
    if args.start or args.end:
        start = date.fromisoformat(args.start) if args.start else None
        end = date.fromisoformat(args.end) if args.end else None
        if start is None or end is None:
            raise ValueError("--start e --end devem ser informados juntos")
        return tuple(iter_dates(start, end))
    return (default_target_date(settings),)


def main(argv: Sequence[str] | None = None) -> int:
    """Ponto de entrada da CLI de ingestao ERA5-Land.

    Args:
        argv: argumentos de linha de comando (default: sys.argv[1:]).

    Returns:
        0 = tudo gravado; 1 = falha tecnica em pelo menos uma data.
    """
    args = build_arg_parser().parse_args(argv)
    settings = get_settings()
    if args.dataset:
        settings = replace(settings, era5_dataset=args.dataset)
    configure_logging(level="DEBUG" if args.verbose else None, settings=settings)

    try:
        variables = (
            tuple(v.strip() for v in args.variables.split(",") if v.strip())
            if args.variables
            else None
        )
        storage = get_storage(backend=args.backend, settings=settings)

        if args.month:
            ano, mes = (int(parte) for parte in args.month.split("-"))
            run_id = uuid4().hex[:12]
            run = Era5IngestionRun(
                run_id=run_id,
                results=(
                    ingest_month(
                        year=ano,
                        month=mes,
                        storage=storage,
                        settings=settings,
                        run_id=run_id,
                        variables=variables,
                        overwrite=not args.no_overwrite,
                        dry_run=args.dry_run,
                    ),
                ),
            )
            log_run_summary(run)
        elif args.backfill_days is not None or settings.era5_lookback_days:
            run = ingest_backfill(
                lookback_days=args.backfill_days,
                storage=storage,
                settings=settings,
                variables=variables,
                overwrite=not args.no_overwrite,
                dry_run=args.dry_run,
            )
        else:
            targets = resolve_dates(args, settings)
            run = ingest_dates(
                dates=targets,
                storage=storage,
                settings=settings,
                variables=variables,
                overwrite=not args.no_overwrite,
                dry_run=args.dry_run,
            )
    except ValueError as exc:
        logger.error("Argumento invalido: %s", exc)
        return 1
    except StorageError as exc:
        logger.critical("Falha na camada de storage: %s", exc)
        return 1
    except Exception as exc:  # pragma: no cover - rede de seguranca
        logger.critical(
            "Erro inesperado na ingestao ERA5: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        return 1

    return 1 if run.failures else 0


if __name__ == "__main__":
    raise SystemExit(main())


