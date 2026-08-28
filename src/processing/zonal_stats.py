"""ETAPA 4 - Estatistica zonal dos rasters da camada Raw por polo produtor.

Le CHIRPS (GeoTIFF) e ERA5-Land (NetCDF) exclusivamente via `get_storage()`,
portanto funciona identicamente no disco local e no S3.

Ponderacao por area
    Em EPSG:4326 a area de um pixel varia com o cosseno da latitude. Todas as
    medias espaciais aplicam peso `cos(lat)`, evitando vies em polos com grande
    extensao norte-sul. A coluna `*_mean_simples` mantem a media aritmetica
    para comparacao/auditoria.

Resiliencia
    Rasters ausentes, chaves inexistentes, geometrias fora da extensao do raster
    e dias sem cobertura valida NAO levantam excecao: geram log de aviso e
    linhas com `NaN` + `pixels_validos = 0`, mantendo a serie temporal continua.
"""

from __future__ import annotations

import io
import tempfile
from collections.abc import Sequence
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final

from src.config import RAW_CHIRPS_PREFIX, RAW_ERA5_PREFIX, get_logger
from src.storage import ObjectNotFoundError, StorageBackend, StorageError

if TYPE_CHECKING:  # pragma: no cover
    import geopandas as gpd
    import pandas as pd

logger = get_logger("processing.zonal_stats")

CHIRPS_NODATA: Final[float] = -9999.0
CHIRPS_FILENAME: Final[str] = "chirps_brazil.tif"
ERA5_FILENAME: Final[str] = "era5_land_brazil.nc"

#: Mapeia a variavel curta do ERA5 para (nome_padronizado, fator, acumulada).
#: Fatores convertem para unidades agronomicas: K->degC, m->mm.
ERA5_VARIABLE_MAP: Final[dict[str, tuple[str, str, bool]]] = {
    "t2m": ("temperatura_2m_c", "kelvin_to_celsius", False),
    "tp": ("precipitacao_mm", "m_to_mm", True),
    "pev": ("evapotranspiracao_potencial_mm", "m_to_mm_abs", True),
    "e": ("evaporacao_total_mm", "m_to_mm_abs", True),
    "swvl1": ("umidade_solo_camada1_m3m3", "identity", False),
    "ssrd": ("radiacao_solar_j_m2", "identity", True),
    "sp": ("pressao_superficie_pa", "identity", False),
}


class ZonalStatsError(RuntimeError):
    """Falha irrecuperavel no calculo da estatistica zonal."""


# -----------------------------------------------------------------------------
# Helpers de leitura via storage
# -----------------------------------------------------------------------------
def chirps_key(target_date: date, filename: str = CHIRPS_FILENAME) -> str:
    """Chave do GeoTIFF CHIRPS de uma data na camada raw."""
    return StorageBackend.join_key(
        RAW_CHIRPS_PREFIX, f"dt={target_date.isoformat()}", filename
    )


def era5_key(target_date: date, filename: str = ERA5_FILENAME) -> str:
    """Chave do NetCDF ERA5-Land de uma data na camada raw."""
    return StorageBackend.join_key(
        RAW_ERA5_PREFIX, f"dt={target_date.isoformat()}", filename
    )


def read_raw_object(storage: StorageBackend, key: str) -> bytes | None:
    """Le um objeto da camada raw devolvendo None quando ausente.

    Args:
        storage: backend de leitura.
        key: chave logica do objeto.

    Returns:
        Bytes do objeto ou None se a chave nao existir.

    Raises:
        ZonalStatsError: em falhas de storage diferentes de "nao encontrado".
    """
    try:
        return storage.read_bytes(key)
    except ObjectNotFoundError:
        logger.warning("Objeto ausente na camada raw | key=%s", key)
        return None
    except StorageError as exc:
        raise ZonalStatsError(f"Falha ao ler {key}: {exc}") from exc


def _apply_factor(values: Any, factor: str) -> Any:
    """Converte unidades brutas do ERA5 para unidades agronomicas."""
    if factor == "kelvin_to_celsius":
        return values - 273.15
    if factor == "m_to_mm":
        return values * 1000.0
    if factor == "m_to_mm_abs":
        return abs(values * 1000.0)
    return values


# -----------------------------------------------------------------------------
# CHIRPS - estatistica zonal sobre GeoTIFF
# -----------------------------------------------------------------------------
def raster_zonal_stats(
    raster_bytes: bytes,
    polos: gpd.GeoDataFrame,
    nodata: float = CHIRPS_NODATA,
    all_touched: bool = True,
) -> pd.DataFrame:
    """Calcula estatistica zonal de um GeoTIFF para cada geometria.

    Args:
        raster_bytes: conteudo do GeoTIFF (lido do storage).
        polos: GeoDataFrame em EPSG:4326 com os polos produtores.
        nodata: valor de ausencia de dado a ignorar.
        all_touched: inclui pixels tocados pela borda do polígono (recomendado
            para polos pequenos frente a resolucao de 0.05 graus).

    Returns:
        DataFrame com uma linha por polo e colunas de estatisticas.

    Raises:
        ZonalStatsError: se o raster nao puder ser aberto.
    """
    import numpy as np
    import pandas as pd_mod
    import rasterio
    from rasterio.mask import mask as rio_mask

    registros: list[dict[str, Any]] = []

    try:
        with rasterio.io.MemoryFile(raster_bytes) as memfile, memfile.open() as dataset:
            raster_nodata = dataset.nodata if dataset.nodata is not None else nodata
            raster_bounds = dataset.bounds

            for _, polo in polos.iterrows():
                nome = str(polo["polo_produtor"])
                base = {
                    "polo_produtor": nome,
                    "commodity": polo.get("commodity", "unknown"),
                    "uf": polo.get("uf", "unknown"),
                }
                geometry = polo["geometry"]

                if not _intersects_bounds(geometry, raster_bounds):
                    logger.warning(
                        "Polo %s fora da extensao do raster | bounds_raster=%s",
                        nome,
                        tuple(round(v, 2) for v in raster_bounds),
                    )
                    registros.append({**base, **_empty_raster_stats()})
                    continue

                try:
                    recorte, transform = rio_mask(
                        dataset,
                        [geometry.__geo_interface__],
                        crop=True,
                        all_touched=all_touched,
                        filled=True,
                        nodata=raster_nodata,
                    )
                except Exception as exc:
                    logger.warning("Recorte falhou para %s: %s", nome, exc)
                    registros.append({**base, **_empty_raster_stats()})
                    continue

                banda = np.asarray(recorte[0], dtype="float64")
                validos = np.isfinite(banda) & (banda != raster_nodata)
                pesos = _cos_latitude_weights(banda.shape, transform)

                if not validos.any():
                    logger.warning("Nenhum pixel valido para o polo %s", nome)
                    registros.append({**base, **_empty_raster_stats()})
                    continue

                valores = banda[validos]
                peso_valido = pesos[validos]
                media_ponderada = float(
                    np.average(valores, weights=peso_valido)
                    if peso_valido.sum() > 0
                    else valores.mean()
                )
                registros.append(
                    {
                        **base,
                        "valor_medio": float(valores.mean()),
                        "valor_medio_ponderado": media_ponderada,
                        "valor_soma": float(valores.sum()),
                        "valor_soma_ponderada": float(np.sum(valores * peso_valido)),
                        "valor_min": float(valores.min()),
                        "valor_max": float(valores.max()),
                        "valor_p90": float(np.percentile(valores, 90)),
                        "pixels_validos": int(valores.size),
                        "cobertura_pct": round(
                            100.0 * valores.size / max(banda.size, 1), 2
                        ),
                    }
                )
    except ZonalStatsError:
        raise
    except Exception as exc:
        raise ZonalStatsError(
            f"Falha ao processar o raster ({type(exc).__name__}): {exc}"
        ) from exc

    return pd_mod.DataFrame(registros)


def _empty_raster_stats() -> dict[str, Any]:
    """Estatisticas neutras para polos/datas sem dado valido."""
    nan = float("nan")
    return {
        "valor_medio": nan,
        "valor_medio_ponderado": nan,
        "valor_soma": nan,
        "valor_soma_ponderada": nan,
        "valor_min": nan,
        "valor_max": nan,
        "valor_p90": nan,
        "pixels_validos": 0,
        "cobertura_pct": 0.0,
    }


def _intersects_bounds(geometry: Any, bounds: Any) -> bool:
    """Indica se a geometria intersecta a extensao do raster."""
    minx, miny, maxx, maxy = geometry.bounds
    return not (
        maxx < bounds.left
        or minx > bounds.right
        or maxy < bounds.bottom
        or miny > bounds.top
    )


def _cos_latitude_weights(shape: tuple[int, int], transform: Any) -> Any:
    """Gera a matriz de pesos `cos(lat)` para uma janela recortada.

    Args:
        shape: (linhas, colunas) do recorte.
        transform: transform afim da janela.

    Returns:
        Array 2D de pesos proporcionais a area real de cada pixel.
    """
    import numpy as np

    linhas = np.arange(shape[0]) + 0.5
    latitudes = transform.f + transform.e * linhas  # e < 0 (norte -> sul)
    pesos_linha = np.cos(np.radians(np.clip(latitudes, -89.9, 89.9)))
    return np.repeat(pesos_linha[:, np.newaxis], shape[1], axis=1)


def chirps_zonal_stats(
    dates: Sequence[date],
    polos: gpd.GeoDataFrame,
    storage: StorageBackend,
) -> pd.DataFrame:
    """Estatistica zonal diaria do CHIRPS por polo produtor.

    Args:
        dates: datas a processar.
        polos: GeoDataFrame dos polos (EPSG:4326).
        storage: backend de leitura da camada raw.

    Returns:
        DataFrame indexado logicamente por (`dt`, `polo_produtor`) com colunas:
        `precipitacao_media_mm`, `precipitacao_media_ponderada_mm`,
        `precipitacao_soma_mm`, `precipitacao_max_mm`, `precipitacao_p90_mm`,
        `pixels_validos`, `cobertura_pct`, `chirps_disponivel`.
    """
    import pandas as pd_mod

    frames: list[pd.DataFrame] = []

    for target_date in dates:
        key = chirps_key(target_date)
        payload = read_raw_object(storage, key)

        if payload is None:
            frames.append(_placeholder_chirps(target_date, polos))
            continue

        try:
            stats = raster_zonal_stats(payload, polos)
        except ZonalStatsError as exc:
            logger.error("CHIRPS invalido em %s: %s", target_date, exc)
            frames.append(_placeholder_chirps(target_date, polos))
            continue

        stats = stats.rename(
            columns={
                "valor_medio": "precipitacao_media_mm",
                "valor_medio_ponderado": "precipitacao_media_ponderada_mm",
                "valor_soma": "precipitacao_soma_mm",
                "valor_soma_ponderada": "precipitacao_soma_ponderada_mm",
                "valor_min": "precipitacao_min_mm",
                "valor_max": "precipitacao_max_mm",
                "valor_p90": "precipitacao_p90_mm",
            }
        )
        stats.insert(0, "dt", target_date)
        stats["chirps_disponivel"] = stats["pixels_validos"] > 0
        stats["chirps_key"] = key
        frames.append(stats)

        logger.info(
            "Zonal CHIRPS | dt=%s | polos=%d | chuva_media_ponderada=%s mm",
            target_date.isoformat(),
            len(stats),
            stats["precipitacao_media_ponderada_mm"].round(2).tolist(),
        )

    if not frames:
        return pd_mod.DataFrame()

    resultado = pd_mod.concat(frames, ignore_index=True)
    resultado["dt"] = pd_mod.to_datetime(resultado["dt"]).dt.date
    return resultado.sort_values(["dt", "polo_produtor"]).reset_index(drop=True)


def _placeholder_chirps(target_date: date, polos: gpd.GeoDataFrame) -> pd.DataFrame:
    """Linhas neutras para uma data sem raster CHIRPS disponivel."""
    import pandas as pd_mod

    nan = float("nan")
    registros = [
        {
            "dt": target_date,
            "polo_produtor": str(polo["polo_produtor"]),
            "commodity": polo.get("commodity", "unknown"),
            "uf": polo.get("uf", "unknown"),
            "precipitacao_media_mm": nan,
            "precipitacao_media_ponderada_mm": nan,
            "precipitacao_soma_mm": nan,
            "precipitacao_soma_ponderada_mm": nan,
            "precipitacao_min_mm": nan,
            "precipitacao_max_mm": nan,
            "precipitacao_p90_mm": nan,
            "pixels_validos": 0,
            "cobertura_pct": 0.0,
            "chirps_disponivel": False,
            "chirps_key": chirps_key(target_date),
        }
        for _, polo in polos.iterrows()
    ]
    return pd_mod.DataFrame(registros)


# -----------------------------------------------------------------------------
# ERA5-Land - estatistica zonal sobre NetCDF horario
# -----------------------------------------------------------------------------
def open_netcdf_bytes(payload: bytes) -> Any:
    """Abre um NetCDF a partir de bytes, sem gravar no disco quando possivel.

    Tenta o engine `h5netcdf` sobre um `BytesIO` (NetCDF4/HDF5). Se falhar
    (ex.: arquivo NetCDF3 classico), grava um temporario e usa o engine
    `netcdf4`, removendo o arquivo em seguida.

    Args:
        payload: conteudo binario do arquivo `.nc`.

    Returns:
        `xarray.Dataset` carregado em memoria (`.load()` aplicado).

    Raises:
        ZonalStatsError: se nenhum engine conseguir abrir o conteudo.
    """
    import xarray as xr

    try:
        with xr.open_dataset(io.BytesIO(payload), engine="h5netcdf") as dataset:
            return dataset.load()
    except Exception as exc_h5:
        logger.debug("h5netcdf falhou (%s); tentando via arquivo temporario", exc_h5)

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            suffix=".nc", prefix="era5_zonal_", delete=False
        ) as tmp:
            tmp.write(payload)
            tmp_path = Path(tmp.name)
        with xr.open_dataset(tmp_path, engine="netcdf4") as dataset:
            return dataset.load()
    except Exception as exc:
        raise ZonalStatsError(f"NetCDF ilegivel: {type(exc).__name__}: {exc}") from exc
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)


def _coord_names(dataset: Any) -> tuple[str, str, str]:
    """Descobre os nomes das coordenadas de tempo, latitude e longitude.

    Raises:
        ZonalStatsError: se alguma coordenada essencial estiver ausente.
    """
    tempo = next(
        (n for n in ("valid_time", "time", "forecast_time") if n in dataset.coords), None
    )
    lat = next((n for n in ("latitude", "lat", "y") if n in dataset.coords), None)
    lon = next((n for n in ("longitude", "lon", "x") if n in dataset.coords), None)
    if not (tempo and lat and lon):
        raise ZonalStatsError(
            f"NetCDF sem coordenadas esperadas (tempo/lat/lon): {list(dataset.coords)}"
        )
    return tempo, lat, lon


def _subset_polo(dataset: Any, polo: Any, lat_name: str, lon_name: str) -> Any:
    """Recorta o Dataset para a bounding box do polo, respeitando a ordem do eixo."""
    min_lon, min_lat, max_lon, max_lat = polo["geometry"].bounds
    lat_values = dataset[lat_name].values
    lat_desc = bool(lat_values.size > 1 and lat_values[0] > lat_values[-1])
    lat_slice = slice(max_lat, min_lat) if lat_desc else slice(min_lat, max_lat)
    return dataset.sel({lat_name: lat_slice, lon_name: slice(min_lon, max_lon)})


def _month_key_for(target_date: date, prefix: str = RAW_ERA5_PREFIX) -> str:
    """Chave do arquivo mensal que contem a data (month=YYYY-MM)."""
    return StorageBackend.join_key(
        prefix,
        f"month={target_date.year:04d}-{target_date.month:02d}",
        "era5_land_brazil.nc",
    )


def era5_zonal_hourly(
    dates: Sequence[date],
    polos: gpd.GeoDataFrame,
    storage: StorageBackend,
) -> pd.DataFrame:
    """Serie horaria media (ponderada por cos(lat)) do ERA5-Land por polo.

    Le tanto o layout diario (`raw/climate_era5/dt=YYYY-MM-DD/`) quanto o
    mensal (`raw/climate_era5/month=YYYY-MM/`, usado no backfill de 1 ano),
    fatiando o NetCDF mensal por dia. Cada variavel e convertida para unidade
    agronomica conforme `ERA5_VARIABLE_MAP` e reduzida espacialmente dentro da
    bbox do polo. A consolidacao temporal (Tmax/Tmin/Tmean e totais diarios)
    fica a cargo do modulo `water_balance`.

    Args:
        dates: datas a processar.
        polos: GeoDataFrame dos polos.
        storage: backend de leitura da camada raw.

    Returns:
        DataFrame com colunas `dt`, `timestamp`, `polo_produtor` e uma coluna
        por variavel padronizada. Vazio quando nenhuma data possui NetCDF.
    """
    import pandas as pd_mod

    registros: list[dict[str, Any]] = []
    datas_por_mes: dict[tuple[int, int], list[date]] = {}
    for target in dates:
        datas_por_mes.setdefault((target.year, target.month), []).append(target)

    for (_ano, _mes), datas_mes in sorted(datas_por_mes.items()):
        chave_mensal = _month_key_for(datas_mes[0])
        payload = read_raw_object(storage, chave_mensal)

        if payload is not None:
            try:
                dataset = open_netcdf_bytes(payload)
                tempo_name, lat_name, lon_name = _coord_names(dataset)
            except ZonalStatsError as exc:
                logger.error("ERA5 mensal invalido: %s", exc)
                dataset = None
            if dataset is not None:
                _process_month_dataset(
                    dataset,
                    datas_mes,
                    polos,
                    lat_name,
                    lon_name,
                    tempo_name,
                    registros,
                )
                dataset.close()
                continue

        # Sem arquivo mensal: tenta o layout diario de cada data.
        for target_date in datas_mes:
            payload_dia = read_raw_object(storage, era5_key(target_date))
            if payload_dia is None:
                logger.warning(
                    "ERA5 ausente em %s; balanco hidrico usara fallback",
                    target_date.isoformat(),
                )
                continue
            try:
                dataset = open_netcdf_bytes(payload_dia)
                tempo_name, lat_name, lon_name = _coord_names(dataset)
            except ZonalStatsError as exc:
                logger.error("ERA5 invalido em %s: %s", target_date, exc)
                continue
            _process_month_dataset(
                dataset,
                [target_date],
                polos,
                lat_name,
                lon_name,
                tempo_name,
                registros,
            )
            dataset.close()

    if not registros:
        return pd_mod.DataFrame()

    resultado = pd_mod.DataFrame(registros)
    resultado["dt"] = pd_mod.to_datetime(resultado["dt"]).dt.date
    return resultado.sort_values(["dt", "polo_produtor", "timestamp"]).reset_index(
        drop=True
    )


def _process_month_dataset(
    dataset: Any,
    datas: Sequence[date],
    polos: gpd.GeoDataFrame,
    lat_name: str,
    lon_name: str,
    tempo_name: str,
    registros: list[dict[str, Any]],
) -> int:
    """Calcula as series horarias de cada data-alvo dentro de um NetCDF.

    O NetCDF pode ser diario ou mensal; datas fora da cobertura sao puladas com
    aviso. Os registros sao anexados a `registros` (mesmo esquema de colunas).

    Args:
        dataset: Dataset aberto (h5netcdf/netcdf4).
        datas: datas-alvo dentro da cobertura do arquivo.
        polos: GeoDataFrame dos polos.
        lat_name/lon_name/tempo_name: nomes das coordenadas.
        registros: lista acumuladora de dicts.

    Returns:
        Quantidade de passos temporais processados.
    """
    import numpy as np
    import pandas as pd_mod

    valores_tempo = pd_mod.to_datetime(dataset[tempo_name].values).normalize()
    alvo = pd_mod.to_datetime([d.isoformat() for d in datas]).normalize()
    indices = np.where(np.isin(valores_tempo, alvo))[0]

    if len(indices) == 0:
        logger.warning(
            "Nenhuma data-alvo dentro do NetCDF | cobertura=%s..%s | alvo=%s..%s",
            valores_tempo.min().date(),
            valores_tempo.max().date(),
            alvo.min().date(),
            alvo.max().date(),
        )
        return 0

    passos = 0
    for _, polo in polos.iterrows():
        nome = str(polo["polo_produtor"])
        recorte = _subset_polo(dataset, polo, lat_name, lon_name)
        if recorte[lat_name].size == 0 or recorte[lon_name].size == 0:
            logger.warning(
                "Polo %s sem celulas ERA5 na bbox | alvo=%s..%s",
                nome,
                datas[0].isoformat(),
                datas[-1].isoformat(),
            )
            continue

        pesos = _cos_weights_xarray(recorte, lat_name)
        colunas: dict[str, Any] = {}
        for bruto, (padronizado, fator, _acumulada) in ERA5_VARIABLE_MAP.items():
            if bruto not in recorte.data_vars:
                continue
            media = (
                recorte[bruto]
                .weighted(pesos)
                .mean(dim=(lat_name, lon_name), skipna=True)
            )
            colunas[padronizado] = _apply_factor(media.values, fator)

        if not colunas:
            logger.warning(
                "NetCDF sem variaveis mapeadas | vars=%s", list(recorte.data_vars)
            )
            continue

        for indice in indices:
            dia = valores_tempo[indice].date()
            timestamp = pd_mod.to_datetime(dataset[tempo_name].values[indice])
            registros.append(
                {
                    "dt": dia,
                    "timestamp": timestamp,
                    "polo_produtor": nome,
                    "commodity": polo.get("commodity", "unknown"),
                    "uf": polo.get("uf", "unknown"),
                    "celulas_era5": int(
                        recorte[lat_name].size * recorte[lon_name].size
                    ),
                    **{
                        nome_col: float(valores[indice])
                        for nome_col, valores in colunas.items()
                    },
                }
            )
        passos += len(indices)

    logger.info(
        "Zonal ERA5 | %s..%s | polos=%d | passos=%d",
        datas[0].isoformat(),
        datas[-1].isoformat(),
        len(polos),
        passos,
    )
    return passos


def _cos_weights_xarray(dataset: Any, lat_name: str) -> Any:
    """Pesos `cos(lat)` como DataArray alinhado ao eixo de latitude."""
    import numpy as np

    return np.cos(np.radians(dataset[lat_name]))




