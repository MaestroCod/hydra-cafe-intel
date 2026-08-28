"""ETAPA 3 - Ingestao CHIRPS (precipitacao diaria) -> camada raw.

Pipeline 100% em memoria (nenhum arquivo temporario em disco):

    HTTP streaming (.tif.gz)  ->  gzip em memoria  ->  rioxarray/rasterio
    ->  clip_box(bbox Brasil) ->  GeoTIFF LZW em memoria  ->  StorageBackend

Destino:

    raw/climate_chirps/dt=YYYY-MM-DD/chirps_brazil.tif

Observacoes de fonte:
    - Produto final (`CHIRPS_BASE_URL`) tem latencia de ~1 a 2 meses.
    - Produto preliminar (`CHIRPS_PRELIM_BASE_URL`, flag `--prelim`) e quase
      tempo real, porem sujeito a revisao.
    - Grade global 0.05 deg, nodata = -9999, unidade = mm/dia.

Uso:
    python -m src.ingestion.chirps                          # hoje - CHIRPS_LAG_DAYS
    python -m src.ingestion.chirps --date 2026-07-15
    python -m src.ingestion.chirps --start 2026-07-01 --end 2026-07-05
    python -m src.ingestion.chirps --date 2026-08-01 --prelim
"""

from __future__ import annotations

import argparse
import gzip
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Final, Literal
from uuid import uuid4

from src.config import (
    RAW_CHIRPS_PREFIX,
    Settings,
    configure_logging,
    get_logger,
    get_settings,
)
from src.ingestion.retry import retry_call
from src.storage import StorageBackend, StorageError, get_storage

logger = get_logger("ingestion.chirps")

SOURCE_NAME: Final[str] = "chirps_v2.0_global_daily_p05"
OUTPUT_FILENAME: Final[str] = "chirps_brazil.tif"
GEOTIFF_CONTENT_TYPE: Final[str] = "image/tiff"
#: Valor de ausencia de dado do CHIRPS (mar/oceano e falhas).
CHIRPS_NODATA: Final[float] = -9999.0
#: Nome do arquivo remoto: chirps-v2.0.2026.07.15.tif.gz
REMOTE_FILENAME_TEMPLATE: Final[str] = "chirps-v2.0.{year}.{month:02d}.{day:02d}.tif.gz"

IngestionStatus = Literal["written", "not_available", "skipped", "failed"]


class ChirpsIngestionError(RuntimeError):
    """Falha irrecuperavel na ingestao de um raster CHIRPS."""


class ChirpsNotAvailableError(ChirpsIngestionError):
    """O arquivo da data solicitada ainda nao existe no servidor (HTTP 404)."""


@dataclass(frozen=True, slots=True)
class RasterStats:
    """Estatisticas do raster recortado, usadas para QA e metadados.

    Attributes:
        width: numero de colunas apos o recorte.
        height: numero de linhas apos o recorte.
        bounds: extensao (min_lon, min_lat, max_lon, max_lat).
        crs: sistema de referencia (ex.: "EPSG:4326").
        valid_pixels: pixels com dado valido (diferentes de nodata).
        min_mm: precipitacao minima valida (mm).
        max_mm: precipitacao maxima valida (mm).
        mean_mm: precipitacao media valida (mm).
    """

    width: int
    height: int
    bounds: tuple[float, float, float, float]
    crs: str
    valid_pixels: int
    min_mm: float
    max_mm: float
    mean_mm: float


@dataclass(frozen=True, slots=True)
class ChirpsIngestionResult:
    """Resultado da ingestao de uma data do CHIRPS."""

    target_date: date
    status: IngestionStatus
    key: str | None = None
    uri: str | None = None
    size_bytes: int = 0
    source_url: str | None = None
    stats: RasterStats | None = None
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        """True quando o objeto foi gravado ou ja existia no lake."""
        return self.status in ("written", "skipped")


@dataclass(frozen=True, slots=True)
class ChirpsIngestionRun:
    """Consolidado de um batch de datas do CHIRPS."""

    run_id: str
    results: tuple[ChirpsIngestionResult, ...] = field(default_factory=tuple)

    @property
    def failures(self) -> tuple[ChirpsIngestionResult, ...]:
        """Datas que falharam por erro tecnico."""
        return tuple(r for r in self.results if r.status == "failed")

    @property
    def unavailable(self) -> tuple[ChirpsIngestionResult, ...]:
        """Datas ainda nao publicadas pelo provedor."""
        return tuple(r for r in self.results if r.status == "not_available")

    @property
    def total_bytes(self) -> int:
        """Soma dos bytes gravados no batch."""
        return sum(r.size_bytes for r in self.results)


# -----------------------------------------------------------------------------
# Helpers de URL, chave e datas
# -----------------------------------------------------------------------------
def build_source_url(
    target_date: date, base_url: str, prelim: bool = False, prelim_base_url: str = ""
) -> str:
    """Monta a URL do GeoTIFF comprimido do CHIRPS.

    Args:
        target_date: data do raster diario.
        base_url: URL base do produto final.
        prelim: se True usa o produto preliminar (quase tempo real).
        prelim_base_url: URL base do produto preliminar.

    Returns:
        URL absoluta do arquivo `.tif.gz`.

    Example:
        >>> build_source_url(date(2026, 7, 15), "https://x/p05")
        'https://x/p05/2026/chirps-v2.0.2026.07.15.tif.gz'
    """
    root = (prelim_base_url or base_url) if prelim else base_url
    filename = REMOTE_FILENAME_TEMPLATE.format(
        year=target_date.year, month=target_date.month, day=target_date.day
    )
    return f"{root.rstrip('/')}/{target_date.year}/{filename}"


def build_object_key(
    target_date: date,
    prefix: str = RAW_CHIRPS_PREFIX,
    filename: str = OUTPUT_FILENAME,
) -> str:
    """Monta a chave particionada do objeto na camada raw.

    Example:
        >>> build_object_key(date(2026, 7, 15))
        'raw/climate_chirps/dt=2026-07-15/chirps_brazil.tif'
    """
    return StorageBackend.join_key(prefix, f"dt={target_date.isoformat()}", filename)


def default_target_date(settings: Settings) -> date:
    """Data default da ingestao: hoje (UTC) menos a latencia do produto."""
    return datetime.now(tz=UTC).date() - timedelta(days=settings.chirps_lag_days)


def iter_dates(start: date, end: date) -> Iterator[date]:
    """Itera dia a dia de `start` ate `end` (inclusive).

    Raises:
        ValueError: se `end` for anterior a `start`.
    """
    if end < start:
        raise ValueError(f"Intervalo invalido: {start} > {end}")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


# -----------------------------------------------------------------------------
# Download (HTTP streaming) e descompressao em memoria
# -----------------------------------------------------------------------------
def download_streamed(
    url: str,
    timeout_seconds: int = 180,
    chunk_size_bytes: int = 1024 * 1024,
    max_retries: int = 3,
) -> bytes:
    """Baixa um arquivo por streaming, acumulando os chunks em memoria.

    O streaming evita que a biblioteca `requests` materialize a resposta inteira
    de uma vez; os chunks sao concatenados em um `BytesIO` (o CHIRPS diario
    comprimido tem ~2-10 MB, seguro para memoria).

    Args:
        url: URL do arquivo remoto.
        timeout_seconds: timeout de conexao/leitura.
        chunk_size_bytes: tamanho de cada chunk lido.
        max_retries: tentativas em caso de erro transitorio de rede.

    Returns:
        Conteudo binario completo do arquivo.

    Raises:
        ChirpsNotAvailableError: se o servidor responder 404 (data inexistente).
        ChirpsIngestionError: para qualquer outra falha de rede/HTTP.
    """
    import requests

    def _fetch() -> bytes:
        with requests.get(url, stream=True, timeout=timeout_seconds) as response:
            if response.status_code == 404:
                raise ChirpsNotAvailableError(f"Arquivo inexistente (404): {url}")
            response.raise_for_status()
            buffer = bytearray()
            for chunk in response.iter_content(chunk_size=chunk_size_bytes):
                if chunk:
                    buffer.extend(chunk)
        if not buffer:
            raise ChirpsIngestionError(f"Resposta vazia para {url}")
        return bytes(buffer)

    payload = retry_call(
        _fetch,
        description=f"download CHIRPS {url.rsplit('/', 1)[-1]}",
        attempts=max_retries,
        exceptions=(Exception,),
        giveup=lambda exc: isinstance(exc, ChirpsNotAvailableError),
        logger=logger,
    )
    logger.info(
        "Download concluido | url=%s | bytes_comprimidos=%d", url, len(payload)
    )
    return payload


def decompress_gzip(payload: bytes) -> bytes:
    """Descompacta bytes GZIP em memoria.

    Args:
        payload: conteudo do arquivo `.gz`.

    Returns:
        Conteudo descompactado (GeoTIFF global).

    Raises:
        ChirpsIngestionError: se o conteudo nao for um GZIP valido.
    """
    try:
        raw = gzip.decompress(payload)
    except (gzip.BadGzipFile, EOFError, OSError) as exc:
        raise ChirpsIngestionError(f"Falha ao descompactar GZIP: {exc}") from exc
    logger.debug("GZIP descompactado em memoria | bytes=%d", len(raw))
    return raw


# -----------------------------------------------------------------------------
# Recorte espacial + exportacao GeoTIFF LZW (tudo em memoria)
# -----------------------------------------------------------------------------
def clip_to_bbox(
    geotiff_bytes: bytes,
    bbox: tuple[float, float, float, float],
    nodata: float = CHIRPS_NODATA,
    tags: dict[str, str] | None = None,
) -> tuple[bytes, RasterStats]:
    """Recorta o raster global para a bounding box e devolve GeoTIFF LZW.

    Usa `rasterio.io.MemoryFile` na leitura e na escrita: em nenhum momento o
    dado toca o disco local, o que mantem o modulo pronto para rodar em Lambda
    ou container com filesystem somente leitura.

    Args:
        geotiff_bytes: GeoTIFF global descompactado.
        bbox: recorte no formato (min_lon, min_lat, max_lon, max_lat).
        nodata: valor de ausencia de dado a preservar no arquivo de saida.
        tags: metadados GeoTIFF adicionais (linhagem/auditoria).

    Returns:
        Tupla (bytes do GeoTIFF recortado e comprimido, estatisticas do raster).

    Raises:
        ChirpsIngestionError: se o raster nao puder ser lido, o recorte ficar
            vazio ou a escrita falhar.
    """
    import numpy as np
    import rioxarray
    import xarray as xr
    from rasterio.io import MemoryFile

    min_lon, min_lat, max_lon, max_lat = bbox

    try:
        with MemoryFile(geotiff_bytes) as source_memfile, source_memfile.open() as dataset:
            raster = rioxarray.open_rasterio(dataset, masked=False)
            if not isinstance(raster, xr.DataArray):
                raise ChirpsIngestionError(
                    "Esperado um raster de banda unica (DataArray); recebido "
                    f"{type(raster).__name__}"
                )
            raster = raster.rio.write_nodata(nodata, inplace=False)
            clipped = raster.rio.clip_box(
                minx=min_lon, miny=min_lat, maxx=max_lon, maxy=max_lat
            )
            clipped.load()

        array = np.asarray(clipped.values)
        if array.ndim == 2:  # garante o eixo de banda
            array = array[np.newaxis, :, :]
        if array.size == 0:
            raise ChirpsIngestionError(
                f"Recorte vazio para bbox={bbox}: verifique a ordem dos valores"
            )

        transform = clipped.rio.transform()
        crs = clipped.rio.crs
        bands, height, width = array.shape
        valid = array[array != nodata]

        profile = {
            "driver": "GTiff",
            "height": height,
            "width": width,
            "count": bands,
            "dtype": array.dtype.name,
            "crs": crs,
            "transform": transform,
            "nodata": nodata,
            "compress": "LZW",
            "tiled": True,
            "blockxsize": 256,
            "blockysize": 256,
        }

        with MemoryFile() as target_memfile:
            with target_memfile.open(**profile) as destination:
                destination.write(array)
                destination.update_tags(**(tags or {}))
                destination.set_band_description(1, "precipitation_mm_day")
            payload = target_memfile.read()

        stats = RasterStats(
            width=width,
            height=height,
            bounds=tuple(float(v) for v in clipped.rio.bounds()),  # type: ignore[arg-type]
            crs=str(crs),
            valid_pixels=int(valid.size),
            min_mm=float(valid.min()) if valid.size else float("nan"),
            max_mm=float(valid.max()) if valid.size else float("nan"),
            mean_mm=float(valid.mean()) if valid.size else float("nan"),
        )
    except ChirpsIngestionError:
        raise
    except Exception as exc:  # rasterio/GDAL/xarray
        raise ChirpsIngestionError(
            f"Falha no recorte espacial ({type(exc).__name__}): {exc}"
        ) from exc

    logger.info(
        "Recorte concluido | %dx%d px | bounds=%s | validos=%d | mm(min/med/max)="
        "%.2f/%.2f/%.2f | bytes_lzw=%d",
        stats.width,
        stats.height,
        tuple(round(v, 3) for v in stats.bounds),
        stats.valid_pixels,
        stats.min_mm,
        stats.mean_mm,
        stats.max_mm,
        len(payload),
    )
    return payload, stats


# -----------------------------------------------------------------------------
# Orquestracao
# -----------------------------------------------------------------------------
def ingest_date(
    target_date: date,
    storage: StorageBackend,
    settings: Settings,
    run_id: str,
    prelim: bool = False,
    overwrite: bool = True,
    dry_run: bool = False,
) -> ChirpsIngestionResult:
    """Ingere o raster CHIRPS de uma data: baixa, recorta e persiste.

    Nunca lanca excecao: erros sao devolvidos no resultado para que um batch
    de varias datas continue processando as demais.

    Args:
        target_date: data do raster diario.
        storage: backend de destino (local ou S3).
        settings: configuracao da plataforma.
        run_id: identificador da execucao (linhagem).
        prelim: usa o produto preliminar do CHIRPS.
        overwrite: se False e a chave existir, marca status "skipped".
        dry_run: executa tudo, exceto a gravacao no storage.

    Returns:
        ChirpsIngestionResult com status, chave, URI e estatisticas do raster.
    """
    key = build_object_key(target_date)
    url = build_source_url(
        target_date,
        base_url=settings.chirps_base_url,
        prelim=prelim,
        prelim_base_url=settings.chirps_prelim_base_url,
    )

    try:
        if not overwrite and storage.exists(key):
            logger.info("Ingestao ignorada (objeto ja existe) | key=%s", key)
            return ChirpsIngestionResult(
                target_date=target_date,
                status="skipped",
                key=key,
                uri=storage.uri(key),
                source_url=url,
            )

        compressed = download_streamed(
            url,
            timeout_seconds=settings.chirps_timeout_seconds,
            chunk_size_bytes=settings.chirps_chunk_size_bytes,
            max_retries=settings.chirps_max_retries,
        )
        global_tif = decompress_gzip(compressed)

        ingested_at = datetime.now(tz=UTC).isoformat()
        tags = {
            "source": SOURCE_NAME,
            "source_url": url,
            "product": "prelim" if prelim else "final",
            "reference_date": target_date.isoformat(),
            "aoi_name": settings.aoi_name,
            "aoi_bbox": ",".join(str(v) for v in settings.aoi_bbox_wgs84),
            "units": "mm/day",
            "ingested_at": ingested_at,
            "run_id": run_id,
        }
        payload, stats = clip_to_bbox(
            global_tif, bbox=settings.aoi_bbox_wgs84, tags=tags
        )

        if dry_run:
            logger.info(
                "[DRY-RUN] Gravacao simulada | key=%s | bytes=%d", key, len(payload)
            )
            return ChirpsIngestionResult(
                target_date=target_date,
                status="written",
                key=key,
                uri=storage.uri(key),
                size_bytes=len(payload),
                source_url=url,
                stats=stats,
            )

        metadata = {
            **tags,
            "width": str(stats.width),
            "height": str(stats.height),
            "valid_pixels": str(stats.valid_pixels),
            "max_mm": f"{stats.max_mm:.3f}",
        }
        stored = storage.write_bytes(
            key, payload, content_type=GEOTIFF_CONTENT_TYPE, metadata=metadata
        )
        logger.info(
            "Ingestao CHIRPS concluida | dt=%s | produto=%s -> %s",
            target_date.isoformat(),
            "prelim" if prelim else "final",
            stored.uri,
        )
        return ChirpsIngestionResult(
            target_date=target_date,
            status="written",
            key=stored.key,
            uri=stored.uri,
            size_bytes=stored.size_bytes,
            source_url=url,
            stats=stats,
        )

    except ChirpsNotAvailableError as exc:
        logger.warning(
            "Raster ainda nao publicado | dt=%s | %s", target_date.isoformat(), exc
        )
        return ChirpsIngestionResult(
            target_date=target_date,
            status="not_available",
            source_url=url,
            error=str(exc),
        )
    except (ChirpsIngestionError, StorageError) as exc:
        logger.error("Ingestao CHIRPS falhou | dt=%s | %s", target_date, exc)
        return ChirpsIngestionResult(
            target_date=target_date, status="failed", source_url=url, error=str(exc)
        )
    except Exception as exc:  # rede de seguranca do batch
        logger.exception("Erro inesperado na ingestao CHIRPS | dt=%s", target_date)
        return ChirpsIngestionResult(
            target_date=target_date,
            status="failed",
            source_url=url,
            error=f"{type(exc).__name__}: {exc}",
        )


def ingest_dates(
    dates: Sequence[date] | None = None,
    storage: StorageBackend | None = None,
    settings: Settings | None = None,
    prelim: bool = False,
    overwrite: bool = True,
    dry_run: bool = False,
) -> ChirpsIngestionRun:
    """Ingere uma lista de datas do CHIRPS e consolida o resultado.

    Args:
        dates: datas a processar; se None usa `default_target_date`.
        storage: backend de destino; se None resolve via factory.
        settings: configuracao; se None usa `get_settings()`.
        prelim: usa o produto preliminar.
        overwrite: sobrescreve objetos existentes.
        dry_run: nao grava no storage.

    Returns:
        ChirpsIngestionRun com um resultado por data.
    """
    cfg = settings or get_settings()
    backend = storage or get_storage(settings=cfg)
    targets = tuple(dates) if dates else (default_target_date(cfg),)
    run_id = uuid4().hex[:12]

    logger.info(
        "Batch CHIRPS iniciado | run_id=%s | backend=%s | datas=%d (%s..%s) | "
        "produto=%s | bbox=%s",
        run_id,
        backend.name,
        len(targets),
        targets[0].isoformat(),
        targets[-1].isoformat(),
        "prelim" if prelim else "final",
        cfg.aoi_bbox_wgs84,
    )

    results = tuple(
        ingest_date(
            target_date=target,
            storage=backend,
            settings=cfg,
            run_id=run_id,
            prelim=prelim,
            overwrite=overwrite,
            dry_run=dry_run,
        )
        for target in targets
    )

    run = ChirpsIngestionRun(run_id=run_id, results=results)
    log_run_summary(run)
    return run


def log_run_summary(run: ChirpsIngestionRun) -> None:
    """Registra o sumario do batch CHIRPS no log estruturado."""
    logger.info("-" * 78)
    logger.info("SUMARIO DA INGESTAO CHIRPS | run_id=%s", run.run_id)
    for result in run.results:
        logger.info(
            "  %s | %-13s | %8d B | %s",
            result.target_date.isoformat(),
            result.status,
            result.size_bytes,
            result.uri or result.error or "-",
        )
    logger.info(
        "Total: %d datas | %d gravadas | %d indisponiveis | %d falhas | %d bytes",
        len(run.results),
        sum(1 for r in run.results if r.status == "written"),
        len(run.unavailable),
        len(run.failures),
        run.total_bytes,
    )
    logger.info("-" * 78)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    """Cria o parser de argumentos da ingestao CHIRPS."""
    parser = argparse.ArgumentParser(
        prog="python -m src.ingestion.chirps",
        description=(
            "Baixa o CHIRPS diario, recorta para a bounding box do Brasil e grava "
            "GeoTIFF LZW em raw/climate_chirps/dt=YYYY-MM-DD/."
        ),
    )
    parser.add_argument("--date", default=None, help="Data unica (YYYY-MM-DD).")
    parser.add_argument("--start", default=None, help="Inicio do intervalo.")
    parser.add_argument("--end", default=None, help="Fim do intervalo (inclusive).")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="Historico em dias ate a data default (ex.: 365 = 1 ano). "
        "Sobrepoe CHIRPS_LOOKBACK_DAYS do .env.",
    )
    parser.add_argument(
        "--prelim",
        action="store_true",
        help="Usa o produto preliminar (quase tempo real, sujeito a revisao).",
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
        "--dry-run", action="store_true", help="Processa sem gravar no storage."
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Log em DEBUG.")
    return parser


def resolve_dates(args: argparse.Namespace, settings: Settings) -> tuple[date, ...]:
    """Resolve as datas a processar a partir dos argumentos da CLI.

    Ordem de precedencia: --date > --start/--end > --lookback-days > data default
    (hoje - lag do produto).

    Args:
        args: argumentos parseados.
        settings: configuracao (lag e lookback do .env).

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
    if args.lookback_days is not None or settings.chirps_lookback_days:
        dias = args.lookback_days or settings.chirps_lookback_days
        fim = default_target_date(settings)
        inicio = fim - timedelta(days=dias - 1)
        return tuple(iter_dates(inicio, fim))
    return (default_target_date(settings),)


def main(argv: Sequence[str] | None = None) -> int:
    """Ponto de entrada da CLI de ingestao CHIRPS.

    Args:
        argv: argumentos de linha de comando (default: sys.argv[1:]).

    Returns:
        0 = tudo gravado; 1 = falha tecnica; 2 = alguma data indisponivel.
    """
    args = build_arg_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(level="DEBUG" if args.verbose else None, settings=settings)

    try:
        targets = resolve_dates(args, settings)
        storage = get_storage(backend=args.backend, settings=settings)
        run = ingest_dates(
            dates=targets,
            storage=storage,
            settings=settings,
            prelim=args.prelim,
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
            "Erro inesperado na ingestao CHIRPS: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        return 1

    if run.failures:
        return 1
    if run.unavailable:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
