"""Testes da Etapa 4: geometria, estatistica zonal, balanco hidrico,
transformacao financeira e orquestracao do pipeline (tudo offline).

Nenhuma chamada de rede: os rasters CHIRPS, NetCDF ERA5 e Parquets financeiros
sao gerados sinteticamente e gravados em um Data Lake temporario via
`LocalStorage`, exercitando o mesmo caminho `get_storage()` da aplicacao real.

Execucao:
    .\\.venv\\Scripts\\python.exe -m pytest tests -q
"""

from __future__ import annotations

import io
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import rasterio
import xarray as xr
from rasterio.transform import from_origin

from src.analytics.gold import add_crop_stress_metrics, build_gold_analytics
from src.config import Settings
from src.ingestion.chirps import build_object_key as chirps_key
from src.ingestion.era5 import build_object_key as era5_key
from src.processing import geometry
from src.processing.finance_transform import (
    add_risk_metrics,
    apply_conversion,
    split_fx,
    transform_finance,
)
from src.processing.pipeline import (
    run_climate_pipeline,
    run_finance_pipeline,
    run_pipeline,
)
from src.processing.water_balance import daily_from_hourly, water_balance
from src.processing.zonal_stats import chirps_zonal_stats, era5_zonal_hourly
from src.storage import LocalStorage

BBOX = (-73.98, -33.75, -28.85, 5.27)
DATES = [date(2026, 7, 15), date(2026, 7, 16), date(2026, 7, 17)]


@pytest.fixture()
def settings(tmp_path: Path) -> Settings:
    """Settings isolado com lake temporario e 3 dias de CHIRPS."""
    return Settings(
        data_lake_root=str(tmp_path / "lake"),
        aoi_bbox=BBOX,
        aoi_name="brasil",
        polos_geojson_path=None,
        water_stress_window_days=7,
        water_stress_deficit_mm=-30.0,
    )


@pytest.fixture()
def storage(settings: Settings) -> LocalStorage:
    """Backend local apontando para o lake temporario."""
    return LocalStorage(root=settings.lake_root_path)


# -----------------------------------------------------------------------------
# Geracao de artefatos sinteticos
# -----------------------------------------------------------------------------
def build_chirps_geotiff(value: float = 10.0) -> bytes:
    """GeoTIFF global de teste: 5 px/grau, valor constante + nodata em 1 pixel."""
    width, height = 900, 200  # lon -180..0, lat -40..0
    dados = np.full((1, height, width), value, dtype="float32")
    dados[0, 0, 0] = -9999.0  # nodata
    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 1,
        "dtype": "float32",
        "crs": "EPSG:4326",
        "transform": from_origin(-180.0, 0.0, 0.2, 0.2),
        "nodata": -9999.0,
    }
    with rasterio.io.MemoryFile() as memfile:
        with memfile.open(**profile) as dataset:
            dataset.write(dados)
        return memfile.read()


def build_era5_netcdf(
    start_date: str = "2026-07-16", hours: int = 24
) -> bytes:
    """NetCDF horario de teste cobrindo o Brasil (0.2 graus).

    Args:
        start_date: dia coberto pelo NetCDF (1 arquivo = 1 dia).
        hours: numero de passos horarios (default 24).
    """
    lats = np.arange(6.0, -34.0, -0.2, dtype="float32")
    lons = np.arange(-74.0, -28.0, 0.2, dtype="float32")
    times = pd.date_range(start_date, periods=hours, freq="h")
    shape = (hours, lats.size, lons.size)

    acumulada = np.zeros(shape, dtype="float32")
    pev_acum = np.zeros(shape, dtype="float32")
    for h in range(hours):
        acumulada[h] = 0.001 * (h + 1)  # 1 mm/h acumulado em metros -> 24 mm/dia
        pev_acum[h] = 0.00006 * (h + 1)  # ~1,4 mm/dia de ETP

    # Temperatura com variacao diurna: Tmax ~301 K (28 C) as 15h e Tmin ~293 K (20 C) as 6h.
    temperatura = np.zeros(shape, dtype="float32")
    for h in range(hours):
        temperatura[h] = 297.0 + 4.0 * np.sin((h - 9) * np.pi / 12)

    dataset = xr.Dataset(
        {
            "t2m": (("valid_time", "latitude", "longitude"), temperatura),
            "tp": (("valid_time", "latitude", "longitude"), acumulada),
            "pev": (("valid_time", "latitude", "longitude"), -pev_acum),
            "swvl1": (
                ("valid_time", "latitude", "longitude"),
                np.full(shape, 0.32, "float32"),
            ),
        },
        coords={
            "valid_time": times,
            "latitude": lats,
            "longitude": lons,
        },
    )
    return dataset.to_netcdf(engine="h5netcdf")


def build_finance_parquet(
    ticker: str, closes: list[float], fx: bool = False
) -> pd.DataFrame:
    """DataFrame no formato da camada raw (coluna `date`, como o yfinance)."""
    index = pd.date_range("2026-07-10", periods=len(closes), freq="B")
    return pd.DataFrame(
        {
            "date": index,
            "open": closes,
            "high": closes,
            "low": closes,
            "close": closes,
            "adj_close": closes,
            "volume": 1000,
            "ticker": ticker,
            "commodity": "fx_usd_brl" if fx else "unknown",
            "exchange": "FX" if fx else "TEST",
            "currency": "BRL" if fx else "USD",
            "interval": "1d",
            "source": "yahoo_finance",
            "ingested_at": datetime.now(tz=UTC),
            "run_id": "teste",
        }
    )


def _parquet_bytes(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, engine="pyarrow", index=False)
    return buffer.getvalue()


def seed_raw_lake(
    storage: LocalStorage,
    chirps_value: float = 10.0,
    include_era5: bool = True,
    include_finance: bool = True,
) -> None:
    """Popula a camada raw do lake temporario com artefatos sinteticos."""
    for dia in DATES:
        storage.write_bytes(chirps_key(dia), build_chirps_geotiff(chirps_value))

    if include_era5:
        for dia in DATES:
            storage.write_bytes(
                era5_key(dia), build_era5_netcdf(start_date=dia.isoformat())
            )

    if include_finance:
        specs = {
            "KC=F": (300.0, 320.0, 310.0, 330.0, 305.0, 295.0, 340.0, 350.0, 345.0, 335.0),
            "ZC=F": (500.0, 510.0, 505.0, 520.0, 515.0, 508.0, 530.0, 540.0, 535.0, 525.0),
            "ZS=F": (
                1000.0, 1020.0, 1010.0, 1030.0, 1025.0, 1015.0, 1050.0, 1060.0, 1055.0, 1040.0,
            ),
        }
        for ticker, closes in specs.items():
            frame = build_finance_parquet(ticker, list(closes))
            safe = ticker.replace("=", "_")
            chave = f"raw/finance/ticker_safe={safe}/dt=2026-08-24/{safe}_1d.parquet"
            storage.write_bytes(chave, _parquet_bytes(frame))

        fx = build_finance_parquet("BRL=X", [5.0] * 10, fx=True)
        storage.write_bytes(
            "raw/finance/ticker_safe=BRL_X/dt=2026-08-25/BRL_X_1d.parquet",
            _parquet_bytes(fx),
        )


# -----------------------------------------------------------------------------
# Geometria
# -----------------------------------------------------------------------------
def test_geometry_polos_definition_and_bbox() -> None:
    assert len(geometry.POLO_DEFINITIONS) == 4
    nomes = {p.nome for p in geometry.POLO_DEFINITIONS}
    assert nomes == {"Sul_de_Minas", "Cerrado_Mineiro", "Sorriso_MT", "Oeste_PR"}

    gdf = geometry.polos_from_bboxes()
    assert len(gdf) == 4
    assert gdf.crs.to_string() == "EPSG:4326"
    assert all(gdf["area_km2"] > 10000)
    assert set(gdf["polo_produtor"]) == nomes


def test_load_polos_fallback_sem_geojson(settings: Settings) -> None:
    gdf = geometry.load_polos(settings)
    # Escopo do projeto: apenas cafe (2 polos de Minas Gerais).
    assert len(gdf) == 2
    assert set(gdf["polo_produtor"]) == {"Sul_de_Minas", "Cerrado_Mineiro"}
    assert (gdf["geometry_source"] == "bbox_interna").all()


def test_load_polos_usa_geojson_externo(settings: Settings, tmp_path: Path) -> None:
    origem = geometry.polos_from_bboxes()
    caminho = tmp_path / "polos.geojson"
    origem.to_file(caminho, driver="GeoJSON")

    gdf = geometry.load_polos(settings, geojson_path=caminho)
    # O escopo (cafe) filtra o vetorial externo para os 2 polos ativos.
    assert len(gdf) == 2
    assert set(gdf["polo_produtor"]) == {"Sul_de_Minas", "Cerrado_Mineiro"}
    assert (gdf["geometry_source"] == f"geojson:{caminho.name}").all()


def test_polos_dataframe_sem_geometria(settings: Settings) -> None:
    frame = geometry.polos_dataframe(settings)
    assert "geometry" not in frame.columns
    assert {"polo_produtor", "commodity", "uf", "area_km2"}.issubset(frame.columns)


# -----------------------------------------------------------------------------
# Zonal CHIRPS + ERA5
# -----------------------------------------------------------------------------
def test_chirps_zonal_stats_valores_constantes(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage, chirps_value=12.0)
    polos = geometry.load_polos(settings)

    zonal = chirps_zonal_stats(DATES, polos, storage)
    assert len(zonal) == len(DATES) * len(polos)
    assert (zonal["pixels_validos"] > 0).all()
    # Raster constante -> media ponderada == valor do pixel.
    assert np.allclose(zonal["precipitacao_media_ponderada_mm"], 12.0)
    assert (zonal["chirps_disponivel"]).all()


def test_chirps_zonal_stats_resiliente_a_data_ausente(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage, include_era5=False, include_finance=False)
    polos = geometry.load_polos(settings)
    com_falha = DATES + [date(2026, 8, 1)]  # 01/08 nao existe na raw

    zonal = chirps_zonal_stats(com_falha, polos, storage)
    assert len(zonal) == len(com_falha) * len(polos)
    ausente = zonal[zonal["dt"] == date(2026, 8, 1)]
    assert not ausente["chirps_disponivel"].any()
    assert ausente["pixels_validos"].eq(0).all()


def test_era5_zonal_hourly_converte_unidades_e_media_por_polo(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage, include_finance=False)
    polos = geometry.load_polos(settings)

    horario = era5_zonal_hourly(DATES, polos, storage)
    assert not horario.empty
    assert {"timestamp", "polo_produtor", "temperatura_2m_c"}.issubset(horario.columns)
    # t2m com ciclo diurno sintetico: max ~28 C, min ~20 C.
    assert np.allclose(horario["temperatura_2m_c"].max(), 28.0, atol=0.5)
    assert np.allclose(horario["temperatura_2m_c"].min(), 20.0, atol=0.5)
    # tp em METROS: 0.024 m acumulado no ultimo passo -> 24 mm.
    assert np.allclose(horario["precipitacao_mm"].iloc[-1], 24.0, atol=1e-2)
    # pev negativo -> absoluto em mm: ~1.44 mm no ultimo passo.
    assert np.allclose(
        horario["evapotranspiracao_potencial_mm"].iloc[-1], 1.44, atol=1e-2
    )
    # swvl1 identidade.
    assert np.allclose(horario["umidade_solo_camada1_m3m3"], 0.32, atol=1e-5)


def test_era5_zonal_hourly_vazio_sem_netcdf(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage, include_era5=False, include_finance=False)
    polos = geometry.load_polos(settings)
    assert era5_zonal_hourly(DATES, polos, storage).empty


# -----------------------------------------------------------------------------
# Balanco hidrico
# -----------------------------------------------------------------------------
def test_daily_from_hourly_consolida_tmax_tmin_e_totais(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage, include_finance=False)
    polos = geometry.load_polos(settings)

    horario = era5_zonal_hourly(DATES, polos, storage)
    diario = daily_from_hourly(
        horario, polos=geometry.polos_dataframe(settings)
    )

    assert len(diario) == len(DATES) * len(polos)
    assert np.allclose(diario["temp_max_c"], 28.0, atol=0.5)
    assert np.allclose(diario["temp_min_c"], 20.0, atol=0.5)
    # Total diario de tp = 0.024 m acumulado -> 24 mm.
    assert np.allclose(diario["precipitacao_era5_mm"], 24.0, atol=0.1)
    assert np.allclose(diario["etp_era5_mm"], 1.44, atol=0.1)
    assert np.allclose(diario["umidade_solo_m3m3"], 0.32, atol=1e-5)
    assert (diario["horas_disponiveis"] == 24).all()


def test_water_balance_deficit_e_alerta_7d(
    settings: Settings, storage: LocalStorage
) -> None:
    # Raster constante 2 mm/dia + ETP 1.2 mm/dia -> deficit = +0.8 (sem alerta).
    seed_raw_lake(storage, chirps_value=2.0)
    polos_gdf = geometry.load_polos(settings)
    polos_df = geometry.polos_dataframe(settings)

    zonal = chirps_zonal_stats(DATES, polos_gdf, storage)
    horario = era5_zonal_hourly(DATES, polos_gdf, storage)
    diario = daily_from_hourly(horario, polos=polos_df)
    balanco = water_balance(zonal, diario, polos_df, settings)

    assert "deficit_hidrico_mm" in balanco.columns
    # chuva CHIRPS 2.0 mm - ETP ERA5 1.44 mm = +0.56 (sem alerta).
    assert np.allclose(balanco["deficit_hidrico_mm"], 0.56, atol=1e-2)
    assert not balanco["alerta_estresse_hidrico"].any()
    assert (balanco["etp_fonte"] == "era5_pev").all()


def test_water_balance_aciona_alerta_sem_era5_usando_hargreaves(
    settings: Settings, storage: LocalStorage
) -> None:
    # CHIRPS seco (0.1 mm) por 14 dias e SEM NetCDF ERA5 (so temperatura do
    # daily_from_hourly nao existira) -> ETP por Hargreaves com as temperaturas
    # sinteticas do ERA5 puxa o deficit acumulado para baixo do limiar.
    dias = [date(2026, 7, 1) + timedelta(days=i) for i in range(14)]
    for dia in dias:
        storage.write_bytes(chirps_key(dia), build_chirps_geotiff(0.1))
        storage.write_bytes(era5_key(dia), build_era5_netcdf(start_date=dia.isoformat()))
    polos_gdf = geometry.load_polos(settings)
    polos_df = geometry.polos_dataframe(settings)

    zonal = chirps_zonal_stats(dias, polos_gdf, storage)
    horario = era5_zonal_hourly(dias, polos_gdf, storage)
    diario = daily_from_hourly(horario, polos=polos_df)
    # Remove a ETP do ERA5 para forcar o caminho Hargreaves.
    diario = diario.drop(columns=["etp_era5_mm"])

    # Limiar de -15 mm: Hargreaves acumula ~-19 mm/7d -> alerta ligado.
    settings_limiar = replace(settings, water_stress_deficit_mm=-15.0)
    balanco = water_balance(zonal, diario, polos_df, settings_limiar)

    assert (balanco["etp_fonte"] == "hargreaves").all()
    assert balanco["etp_mm"].notna().all()
    assert balanco["etp_mm"].gt(0).all()
    # Com 7+ dias na janela o alerta liga (deficit acumulado < -15 mm).
    assert balanco["alerta_estresse_hidrico"].tail(7 * len(polos_gdf)).all()


def test_water_balance_janela_incompleta_nao_alerta(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage, chirps_value=0.1)
    polos_gdf = geometry.load_polos(settings)
    polos_df = geometry.polos_dataframe(settings)
    zonal = chirps_zonal_stats([DATES[0]], polos_gdf, storage)  # 1 dia apenas
    balanco = water_balance(zonal, None, polos_df, settings, require_full_window=True)
    assert not balanco["alerta_estresse_hidrico"].any()
    assert (balanco["janela_completa"] == False).all()


# -----------------------------------------------------------------------------
# Financeiro
# -----------------------------------------------------------------------------
def test_apply_conversion_kc_usd_para_brl_saca() -> None:
    closes = [300.0, 320.0]
    commodity = pd.DataFrame(
        {
            "dt": pd.to_datetime(["2026-07-10", "2026-07-13"]),
            "close": closes,
            "ticker": ["KC=F", "KC=F"],
            "commodity": ["coffee_arabica"] * 2,
            "exchange": ["ICE"] * 2,
            "currency": ["USD"] * 2,
        }
    )
    cambio = pd.DataFrame(
        {
            "dt": pd.to_datetime(["2026-07-09", "2026-07-12"]),
            "usd_brl": [5.0, 5.0],
        }
    )

    convertido = apply_conversion(commodity, cambio)
    # 300 cents/lb -> 3.00 USD/lb -> 3.00 * 132.2774 = 396.83 USD/saca -> * 5 = 1984.16 BRL/saca
    assert np.allclose(convertido["preco_brl_saca"], [1984.16, 2116.44], atol=0.5)
    assert np.allclose(convertido["usd_brl_utilizado"], 5.0)


def test_apply_conversion_sem_cambio_resulta_nan() -> None:
    commodity = pd.DataFrame(
        {
            "dt": pd.to_datetime(["2026-07-10"]),
            "close": [300.0],
            "ticker": ["KC=F"],
            "commodity": ["coffee_arabica"],
            "exchange": ["ICE"],
            "currency": ["USD"],
        }
    )
    convertido = apply_conversion(commodity, pd.DataFrame())
    assert np.isnan(convertido["preco_brl_saca"].iloc[0])
    assert np.isnan(convertido["usd_brl_utilizado"].iloc[0])


def test_split_fx_separa_cambio_e_commodities() -> None:
    frame = pd.concat(
        [
            build_finance_parquet("KC=F", [300.0, 310.0]),
            build_finance_parquet("BRL=X", [5.0, 5.1], fx=True),
        ],
        ignore_index=True,
    )
    commodities, cambio = split_fx(frame)
    assert set(commodities["ticker"]) == {"KC=F"}
    assert set(cambio["usd_brl"]) == {5.0, 5.1}


def test_add_risk_metrics_retorno_e_volatilidade_21d() -> None:
    # 30 dias uteis com oscilacao crescente -> vol movel de 21 pregocs > 0.
    preco = pd.DataFrame(
        {
            "dt": pd.date_range("2026-07-10", periods=30, freq="B"),
            "ticker": ["KC=F"] * 30,
            "preco_brl_saca": [100.0 + 2.0 * np.sin(i / 3) for i in range(30)],
        }
    )
    risco = add_risk_metrics(preco, window_days=21, min_periods=5)
    assert np.isnan(risco["retorno_diario_pct"].iloc[0])
    assert np.isclose(float(risco["retorno_diario_pct"].iloc[1]), float(preco["preco_brl_saca"].pct_change().iloc[1] * 100), atol=1e-9)
    # Vol com janela cheia (a partir do 22o dia com 21 observacoes validas).
    assert float(risco["volatilidade_21d_diaria"].iloc[21]) > 0
    # Anualizada = diaria * sqrt(252).
    validas = risco["volatilidade_21d_diaria"].notna()
    assert np.allclose(
        risco.loc[validas, "volatilidade_21d_anualizada"],
        risco.loc[validas, "volatilidade_21d_diaria"] * (252 ** 0.5),
    )
    # Com < min_periods observacoes a volatilidade e NaN.
    assert np.isnan(risco["volatilidade_21d_diaria"].iloc[1])


def test_transform_finance_ponta_a_ponta(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage)
    transformado = transform_finance(storage, settings=settings)
    assert not transformado.empty
    assert {
        "preco_brl_saca",
        "retorno_diario_pct",
        "volatilidade_21d_anualizada",
    }.issubset(transformado.columns)
    # KC=F: 300 cents/lb -> 1984.16 BRL/saca (cambio 5.0).
    primeira = transformado[transformado["ticker"] == "KC=F"].sort_values("dt").iloc[0]
    assert np.isclose(primeira["preco_brl_saca"], 1984.16, atol=0.5)
    # BRL=X nao entra no resultado de commodities.
    assert not (transformado["ticker"] == "BRL=X").any()


# -----------------------------------------------------------------------------
# Pipeline e Gold
# -----------------------------------------------------------------------------
def test_run_pipeline_ponta_a_ponta_escreve_processed(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage)

    resultado = run_pipeline(storage=storage, settings=settings)

    assert len(resultado.climate_keys) == len(DATES)
    assert len(resultado.finance_keys) == 3  # KC_F, ZC_F, ZS_F
    assert resultado.finance_rows > 0

    for chave in resultado.climate_keys:
        assert storage.exists(chave)
        assert chave.startswith("processed/climate/")
        assert "/dt=2026-07-" in chave
    for chave in resultado.finance_keys:
        assert storage.exists(chave)
        assert chave.startswith("processed/finance/")
        assert "ticker_safe=" in chave


def test_run_pipeline_resiliente_sem_finance(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage, include_finance=False)  # CHIRPS + ERA5, sem finanças

    balanco, resultado = run_climate_pipeline(storage=storage, settings=settings)
    assert len(resultado.climate_keys) == len(DATES)
    assert not balanco.empty
    assert balanco["chirps_disponivel"].all()
    # Com ERA5 (temperatura) mas sem financas, o ETP usa o pev do ERA5.
    assert (balanco["etp_fonte"] == "era5_pev").all()
    assert balanco["dados_completos"].all()


def test_gold_gera_parquet_de_correlacoes(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage)
    run_pipeline(storage=storage, settings=settings)

    gold = build_gold_analytics(storage=storage)
    assert gold.gold_key == "gold/analytics_crop_market.parquet"
    assert storage.exists(gold.gold_key)
    assert gold.gold_rows == 6  # 3 commodities x 2 estresses x 2 mercados (n>=5)
    assert all(c.commodity in {"coffee_arabica", "corn", "soybean"} for c in gold.correlacoes)


def test_gold_add_stress_metrics_calcula_anomalia(
    settings: Settings, storage: LocalStorage
) -> None:
    seed_raw_lake(storage)
    polos_gdf = geometry.load_polos(settings)
    polos_df = geometry.polos_dataframe(settings)
    zonal = chirps_zonal_stats(DATES, polos_gdf, storage)
    balanco = water_balance(zonal, None, polos_df, settings)

    com_estresse = add_crop_stress_metrics(balanco)
    assert {"deficit_acumulado_14d_mm", "anomalia_chuva_30d_mm", "crop_stress_index"}.issubset(
        com_estresse.columns
    )
    # Raster constante -> anomalia = 0.
    assert np.allclose(com_estresse["anomalia_chuva_30d_mm"], 0.0, atol=1e-6)


def test_pipeline_sem_raw_nao_levanta_excecao(
    settings: Settings, storage: LocalStorage
) -> None:
    # Com write_outputs=False o pipeline processa placeholders em memoria.
    balanco, clima = run_climate_pipeline(
        storage=storage, settings=settings, write_outputs=False
    )
    assert clima.climate_keys == ()
    assert len(balanco) == 4 * len(geometry.load_polos(settings))  # fallback x polos

    financas, resultado = run_finance_pipeline(
        storage=storage, settings=settings, write_outputs=False
    )
    assert financas.empty
    assert resultado.finance_keys == ()





