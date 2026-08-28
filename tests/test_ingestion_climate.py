"""Testes da Etapa 3: ingestao climatica (CHIRPS e ERA5-Land).

Nenhuma chamada de rede real: o download HTTP do CHIRPS e o cliente cdsapi sao
substituidos por dublês (monkeypatch), e os artefatos (GeoTIFF global gzipado e
NetCDF horario) sao gerados sinteticamente com rasterio/xarray.

Execucao:
    .\\.venv\\Scripts\\python.exe -m pytest tests -q
"""

from __future__ import annotations

import gzip
from collections.abc import Iterator
from dataclasses import replace
from datetime import date
from pathlib import Path
from typing import Any, Self

import numpy as np
import pytest
import rasterio
import xarray as xr
from rasterio.transform import from_origin

from src.config import Settings
from src.ingestion import chirps, era5
from src.processing import geometry
from src.processing.zonal_stats import era5_zonal_hourly
from src.storage import LocalStorage

BBOX_BRAZIL = (-73.98, -33.75, -28.85, 5.27)
CDS_AREA_BRAZIL = (5.27, -73.98, -33.75, -28.85)


# -----------------------------------------------------------------------------
# Fixtures / geradores de artefatos sinteticos
# -----------------------------------------------------------------------------
@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    """Settings isolado apontando o lake para um diretorio temporario."""
    return Settings(
        data_lake_root=str(tmp_path / "lake"),
        aoi_bbox=BBOX_BRAZIL,
        aoi_name="brasil",
        cdsapi_url="https://cds.example/api",
        cdsapi_key="chave-de-teste",
        chirps_max_retries=2,
        era5_max_retries=2,
    )


@pytest.fixture()
def storage(settings: Settings) -> LocalStorage:
    """Backend local apontando para o lake temporario."""
    return LocalStorage(root=settings.lake_root_path)


def build_global_geotiff(resolution: float = 1.0) -> bytes:
    """Gera um GeoTIFF global sintetico (lon -180..180, lat -50..50).

    Args:
        resolution: tamanho do pixel em graus.

    Returns:
        Bytes do GeoTIFF (mesma convencao do CHIRPS: nodata -9999).
    """
    width = int(360 / resolution)
    height = int(100 / resolution)
    data = np.arange(width * height, dtype="float32").reshape(1, height, width) % 50.0
    data[0, 0, 0] = chirps.CHIRPS_NODATA  # simula pixel de oceano

    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 1,
        "dtype": "float32",
        "crs": "EPSG:4326",
        "transform": from_origin(-180.0, 50.0, resolution, resolution),
        "nodata": chirps.CHIRPS_NODATA,
    }
    with rasterio.io.MemoryFile() as memfile:
        with memfile.open(**profile) as dataset:
            dataset.write(data)
        return memfile.read()


def build_era5_netcdf(target: Path, hours: int = 24) -> Path:
    """Gera um NetCDF sintetico no formato devolvido pelo CDS para o Brasil."""
    lats = np.arange(5.2, -33.8, -1.0, dtype="float32")
    lons = np.arange(-73.9, -28.8, 1.0, dtype="float32")
    times = np.array(
        [np.datetime64("2026-08-10T00:00:00") + np.timedelta64(h, "h") for h in range(hours)]
    )
    shape = (hours, lats.size, lons.size)
    dataset = xr.Dataset(
        {
            "t2m": (("valid_time", "latitude", "longitude"), np.full(shape, 300.0, "float32")),
            "tp": (("valid_time", "latitude", "longitude"), np.full(shape, 0.001, "float32")),
            "pev": (("valid_time", "latitude", "longitude"), np.full(shape, -0.002, "float32")),
            "swvl1": (("valid_time", "latitude", "longitude"), np.full(shape, 0.3, "float32")),
        },
        coords={"valid_time": times, "latitude": lats, "longitude": lons},
    )
    dataset.to_netcdf(target, engine="netcdf4")
    return target


class FakeHttpResponse:
    """Dublê minimo de `requests.Response` com suporte a streaming."""

    def __init__(self, payload: bytes, status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int = 1024) -> Iterator[bytes]:
        for start in range(0, len(self.payload), chunk_size):
            yield self.payload[start : start + chunk_size]


# -----------------------------------------------------------------------------
# CHIRPS - helpers puros
# -----------------------------------------------------------------------------
def test_build_source_url_final_and_prelim() -> None:
    final_url = chirps.build_source_url(date(2026, 7, 15), "https://x/p05")
    assert final_url == "https://x/p05/2026/chirps-v2.0.2026.07.15.tif.gz"

    prelim_url = chirps.build_source_url(
        date(2026, 8, 1),
        "https://x/p05",
        prelim=True,
        prelim_base_url="https://x/prelim",
    )
    assert prelim_url == "https://x/prelim/2026/chirps-v2.0.2026.08.01.tif.gz"


def test_chirps_object_key_uses_hive_partition() -> None:
    assert (
        chirps.build_object_key(date(2026, 7, 15))
        == "raw/climate_chirps/dt=2026-07-15/chirps_brazil.tif"
    )


def test_iter_dates_inclusive_and_validates_order() -> None:
    days = list(chirps.iter_dates(date(2026, 7, 1), date(2026, 7, 3)))
    assert days == [date(2026, 7, 1), date(2026, 7, 2), date(2026, 7, 3)]
    with pytest.raises(ValueError, match="Intervalo invalido"):
        list(chirps.iter_dates(date(2026, 7, 3), date(2026, 7, 1)))


# -----------------------------------------------------------------------------
# CHIRPS - download, gzip e recorte
# -----------------------------------------------------------------------------
def test_download_streamed_accumulates_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = b"x" * 5000
    monkeypatch.setattr(
        "requests.get", lambda *_, **__: FakeHttpResponse(payload), raising=True
    )
    assert (
        chirps.download_streamed("https://x/f.tif.gz", chunk_size_bytes=1024) == payload
    )


def test_download_streamed_404_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"n": 0}

    def _get(*_: object, **__: object) -> FakeHttpResponse:
        calls["n"] += 1
        return FakeHttpResponse(b"", status_code=404)

    monkeypatch.setattr("requests.get", _get, raising=True)
    with pytest.raises(chirps.ChirpsNotAvailableError):
        chirps.download_streamed("https://x/f.tif.gz", max_retries=3)
    assert calls["n"] == 1  # giveup impede retentativas em 404


def test_download_streamed_retries_transient_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = {"n": 0}

    def _get(*_: object, **__: object) -> FakeHttpResponse:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ConnectionError("reset por par")
        return FakeHttpResponse(b"ok")

    monkeypatch.setattr("requests.get", _get, raising=True)
    monkeypatch.setattr("time.sleep", lambda *_: None)
    assert chirps.download_streamed("https://x/f.tif.gz", max_retries=3) == b"ok"
    assert calls["n"] == 2


def test_decompress_gzip_roundtrip_and_invalid_payload() -> None:
    assert chirps.decompress_gzip(gzip.compress(b"conteudo")) == b"conteudo"
    with pytest.raises(chirps.ChirpsIngestionError, match="GZIP"):
        chirps.decompress_gzip(b"nao-eh-gzip")


def test_clip_to_bbox_produces_lzw_geotiff_within_bounds() -> None:
    payload, stats = chirps.clip_to_bbox(
        build_global_geotiff(), bbox=BBOX_BRAZIL, tags={"source": "teste"}
    )

    assert stats.crs == "EPSG:4326"
    assert stats.width < 360 and stats.height < 100  # houve recorte real
    min_lon, min_lat, max_lon, max_lat = stats.bounds
    assert min_lon >= -75.0 and max_lon <= -28.0
    assert min_lat >= -34.5 and max_lat <= 6.0

    with rasterio.io.MemoryFile(payload) as memfile, memfile.open() as dataset:
        assert dataset.driver == "GTiff"
        assert dataset.compression is not None
        assert dataset.compression.name.lower() == "lzw"
        assert dataset.nodata == chirps.CHIRPS_NODATA
        assert dataset.crs.to_epsg() == 4326
        assert dataset.tags()["source"] == "teste"
        assert dataset.descriptions[0] == "precipitation_mm_day"


def test_clip_to_bbox_rejects_bbox_outside_raster() -> None:
    with pytest.raises(chirps.ChirpsIngestionError):
        chirps.clip_to_bbox(build_global_geotiff(), bbox=(100.0, 60.0, 120.0, 80.0))


# -----------------------------------------------------------------------------
# CHIRPS - ingestao ponta a ponta (rede mockada)
# -----------------------------------------------------------------------------
def _patch_chirps_download(monkeypatch: pytest.MonkeyPatch) -> None:
    """Aponta `requests.get` para um GeoTIFF global gzipado sintetico."""
    monkeypatch.setattr(
        "requests.get",
        lambda *_, **__: FakeHttpResponse(gzip.compress(build_global_geotiff())),
        raising=True,
    )


def test_ingest_date_writes_geotiff_to_lake(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, storage: LocalStorage
) -> None:
    _patch_chirps_download(monkeypatch)

    result = chirps.ingest_date(
        target_date=date(2026, 7, 15),
        storage=storage,
        settings=settings,
        run_id="run-teste",
    )

    assert result.status == "written"
    assert result.key == "raw/climate_chirps/dt=2026-07-15/chirps_brazil.tif"
    assert result.size_bytes > 0
    assert result.stats is not None and result.stats.valid_pixels > 0

    stored = storage.read_bytes(result.key)
    with rasterio.io.MemoryFile(stored) as memfile, memfile.open() as dataset:
        tags = dataset.tags()
        assert tags["reference_date"] == "2026-07-15"
        assert tags["product"] == "final"
        assert tags["run_id"] == "run-teste"
        assert tags["units"] == "mm/day"

    disk_dir = settings.lake_root_path / "raw" / "climate_chirps" / "dt=2026-07-15"
    assert (disk_dir / "chirps_brazil.tif").is_file()


def test_ingest_date_marks_not_available_on_404(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, storage: LocalStorage
) -> None:
    monkeypatch.setattr(
        "requests.get",
        lambda *_, **__: FakeHttpResponse(b"", status_code=404),
        raising=True,
    )
    result = chirps.ingest_date(
        target_date=date(2026, 12, 31),
        storage=storage,
        settings=settings,
        run_id="run-teste",
    )
    assert result.status == "not_available"
    assert not storage.exists(chirps.build_object_key(date(2026, 12, 31)))


def test_ingest_dates_skips_existing_when_no_overwrite(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, storage: LocalStorage
) -> None:
    _patch_chirps_download(monkeypatch)
    target = date(2026, 7, 15)

    first = chirps.ingest_dates(dates=[target], storage=storage, settings=settings)
    second = chirps.ingest_dates(
        dates=[target], storage=storage, settings=settings, overwrite=False
    )

    assert first.results[0].status == "written"
    assert second.results[0].status == "skipped"
    assert not first.failures and not second.failures


def test_ingest_dates_dry_run_does_not_write(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, storage: LocalStorage
) -> None:
    _patch_chirps_download(monkeypatch)
    run = chirps.ingest_dates(
        dates=[date(2026, 7, 15)], storage=storage, settings=settings, dry_run=True
    )
    assert run.results[0].status == "written"
    assert storage.list_objects("raw/climate_chirps") == []


# -----------------------------------------------------------------------------
# ERA5-Land - variaveis, request e chave
# -----------------------------------------------------------------------------
def test_resolve_variables_maps_unsupported_tmax_tmin() -> None:
    resolved = era5.resolve_variables(
        [
            "2m_temperature",
            "maximum_2m_temperature_since_previous_post_processing",
            "minimum_2m_temperature_since_previous_post_processing",
            "total_precipitation",
            "potential_evaporation",
            "volumetric_soil_water_layer_1",
        ]
    )
    assert resolved == (
        "2m_temperature",
        "total_precipitation",
        "potential_evaporation",
        "volumetric_soil_water_layer_1",
    )
    assert all(name in era5.ERA5_LAND_KNOWN_VARIABLES for name in resolved)


def test_resolve_variables_keeps_unknown_for_other_datasets() -> None:
    resolved = era5.resolve_variables(
        ["maximum_2m_temperature_since_previous_post_processing"],
        dataset="reanalysis-era5-single-levels",
    )
    assert resolved == ("maximum_2m_temperature_since_previous_post_processing",)


def test_resolve_variables_rejects_empty_list() -> None:
    with pytest.raises(era5.Era5IngestionError):
        era5.resolve_variables(["", "   "])


def test_build_request_uses_cds_area_order() -> None:
    request = era5.build_request(
        date(2026, 8, 10), ["2m_temperature"], area=CDS_AREA_BRAZIL
    )
    assert request["area"] == [5.27, -73.98, -33.75, -28.85]  # N, W, S, E
    assert request["year"] == ["2026"]
    assert request["month"] == ["08"]
    assert request["day"] == ["10"]
    assert len(request["time"]) == 24
    assert request["data_format"] == "netcdf"
    assert request["download_format"] == "unarchived"


def test_era5_object_key_uses_hive_partition() -> None:
    assert (
        era5.build_object_key(date(2026, 8, 10))
        == "raw/climate_era5/dt=2026-08-10/era5_land_brazil.nc"
    )


def test_settings_bbox_conversion_wgs84_to_cds(settings: Settings) -> None:
    assert settings.aoi_bbox_wgs84 == BBOX_BRAZIL
    assert settings.aoi_bbox_cds == CDS_AREA_BRAZIL


# -----------------------------------------------------------------------------
# ERA5-Land - ingestao com cliente cdsapi mockado
# -----------------------------------------------------------------------------
class FakeCdsClient:
    """Dublê do `cdsapi.Client` que grava um NetCDF sintetico no destino."""

    def __init__(self, fail_times: int = 0) -> None:
        self.calls: list[tuple[str, dict[str, Any], str]] = []
        self.fail_times = fail_times

    def retrieve(self, name: str, request: dict[str, Any], target: str) -> None:
        self.calls.append((name, request, target))
        if len(self.calls) <= self.fail_times:
            raise RuntimeError("fila do CDS indisponivel")
        build_era5_netcdf(Path(target))


def test_era5_ingest_date_writes_netcdf_and_removes_temp(
    settings: Settings, storage: LocalStorage
) -> None:
    client = FakeCdsClient()

    result = era5.ingest_date(
        target_date=date(2026, 8, 10),
        storage=storage,
        settings=settings,
        run_id="run-era5",
        client=client,
    )

    assert result.status == "written"
    assert result.key == "raw/climate_era5/dt=2026-08-10/era5_land_brazil.nc"
    assert result.size_bytes > 0
    assert result.variables == (
        "2m_temperature",
        "total_precipitation",
        "potential_evaporation",
        "volumetric_soil_water_layer_1",
    )

    # A requisicao usou o recorte no servidor, no formato do CDS.
    dataset_name, request, target_path = client.calls[0]
    assert dataset_name == "reanalysis-era5-land"
    assert request["area"] == list(CDS_AREA_BRAZIL)

    # O arquivo temporario foi apagado (somente o objeto do lake permanece).
    assert not Path(target_path).exists()
    assert not Path(target_path).parent.exists()

    # O NetCDF gravado no lake e legivel e tem 24 passos horarios.
    reread = settings.lake_root_path / "roundtrip.nc"
    reread.write_bytes(storage.read_bytes(result.key))
    with xr.open_dataset(reread, engine="netcdf4") as reopened:
        assert reopened.sizes["valid_time"] == 24
        assert set(reopened.data_vars) >= {"t2m", "tp"}

    assert result.stats is not None
    assert result.stats.time_steps == 24
    assert result.stats.bounds is not None


def test_era5_download_retries_then_succeeds(
    monkeypatch: pytest.MonkeyPatch, settings: Settings, storage: LocalStorage
) -> None:
    monkeypatch.setattr("time.sleep", lambda *_: None)
    client = FakeCdsClient(fail_times=1)

    result = era5.ingest_date(
        target_date=date(2026, 8, 10),
        storage=storage,
        settings=settings,
        run_id="run-era5",
        client=client,
    )

    assert result.status == "written"
    assert len(client.calls) == 2  # 1 falha + 1 sucesso


def test_era5_ingest_date_fails_without_credentials(
    settings: Settings, storage: LocalStorage
) -> None:
    no_creds = replace(settings, cdsapi_key=None)
    result = era5.ingest_date(
        target_date=date(2026, 8, 10),
        storage=storage,
        settings=no_creds,
        run_id="run-era5",
    )
    assert result.status == "failed"
    assert result.error is not None and "CDSAPI_KEY" in result.error
    assert storage.list_objects("raw/climate_era5") == []


def test_era5_dry_run_does_not_call_cds(
    settings: Settings, storage: LocalStorage
) -> None:
    client = FakeCdsClient()
    run = era5.ingest_dates(
        dates=[date(2026, 8, 10)],
        storage=storage,
        settings=settings,
        client=client,
        dry_run=True,
    )
    assert run.results[0].status == "written"
    assert client.calls == []
    assert storage.list_objects("raw/climate_era5") == []


def test_era5_skips_existing_when_no_overwrite(
    settings: Settings, storage: LocalStorage
) -> None:
    client = FakeCdsClient()
    target = date(2026, 8, 10)

    first = era5.ingest_dates(
        dates=[target], storage=storage, settings=settings, client=client
    )
    second = era5.ingest_dates(
        dates=[target],
        storage=storage,
        settings=settings,
        client=client,
        overwrite=False,
    )

    assert first.results[0].status == "written"
    assert second.results[0].status == "skipped"
    assert len(client.calls) == 1  # a segunda execucao nao chamou o CDS


# -----------------------------------------------------------------------------
# ERA5-Land - backfill mensal (1 ano = 12 requisicoes)
# -----------------------------------------------------------------------------
def test_build_month_request_contem_todos_os_dias() -> None:
    request = era5.build_month_request(2026, 2, ["2m_temperature"], area=CDS_AREA_BRAZIL)
    assert request["day"] == [f"{d:02d}" for d in range(1, 29)]  # fevereiro/2026
    assert request["month"] == ["02"]
    assert len(request["time"]) == 24


def test_build_month_key_e_iter_months() -> None:
    assert (
        era5.build_month_key(2026, 7)
        == "raw/climate_era5/month=2026-07/era5_land_brazil.nc"
    )
    meses = list(
        era5.iter_months(date(2025, 11, 10), date(2026, 2, 5))
    )
    assert meses == [(2025, 11), (2025, 12), (2026, 1), (2026, 2)]


def test_era5_zonal_hourly_fatia_arquivo_mensal_por_dia(
    settings: Settings, storage: LocalStorage
) -> None:
    """Um NetCDF mensal (2 dias) gera linhas com `dt` de cada dia-alvo."""
    from tests.test_processing import build_era5_netcdf  # reuso do builder sintetico

    dias = [date(2026, 7, 16), date(2026, 7, 17)]
    storage.write_bytes(
        era5.build_month_key(2026, 7),
        build_era5_netcdf(start_date="2026-07-16", hours=48),
    )
    polos = geometry.load_polos(settings)

    horario = era5_zonal_hourly(dias, polos, storage)
    assert not horario.empty
    assert set(horario["dt"].unique()) == set(dias)
    # 2 dias x 2 polos de cafe (escopo) x 24 h.
    assert len(horario) == 2 * len(polos) * 24


def test_ingest_month_dry_run_monta_payload_mensal(
    settings: Settings, storage: LocalStorage
) -> None:
    resultado = era5.ingest_month(
        year=2026,
        month=7,
        storage=storage,
        settings=settings,
        run_id="run-mensal",
        dry_run=True,
    )
    assert resultado.status == "written"
    assert resultado.key == "raw/climate_era5/month=2026-07/era5_land_brazil.nc"
    assert storage.list_objects("raw/climate_era5") == []


def test_ingest_month_escreve_e_remove_temporario(
    settings: Settings, storage: LocalStorage
) -> None:
    from src.ingestion.era5 import Era5IngestionResult

    client = FakeCdsClient()

    resultado = era5.ingest_month(
        year=2026,
        month=7,
        storage=storage,
        settings=settings,
        run_id="run-mensal",
        client=client,
    )

    assert isinstance(resultado, Era5IngestionResult)
    assert resultado.status == "written"
    assert storage.exists("raw/climate_era5/month=2026-07/era5_land_brazil.nc")
    # O FakeCdsClient grava 1 dia; o request mensal pede 31 dias (aceito no fake).
    assert resultado.size_bytes > 0




