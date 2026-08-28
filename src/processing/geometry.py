"""ETAPA 4 - Geometrias dos polos produtores de referencia (WGS84).

Estrategia de resiliencia (sem dependencia de shapefiles externos):

    1. Se `POLOS_GEOJSON_PATH` estiver definido no .env e o arquivo existir,
       os polígonos oficiais sao carregados dele (ex.: malha do IBGE recortada).
    2. Caso contrario, sao usadas as bounding boxes representativas definidas
       em `POLO_DEFINITIONS`, garantindo execucao 100% offline.

Polos cobertos:
    Cafe arabica  -> Sul_de_Minas (MG), Cerrado_Mineiro (MG)
    Soja / Milho  -> Sorriso_MT (MT), Oeste_PR (PR)

Todas as geometrias sao devolvidas em EPSG:4326 (mesma grade do CHIRPS e do
ERA5-Land), evitando reprojecao na etapa de estatistica zonal.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Final

from src.config import Settings, get_logger, get_settings

if TYPE_CHECKING:  # pragma: no cover - evita importar geopandas em tempo de import
    import geopandas as gpd
    import pandas as pd

logger = get_logger("processing.geometry")

CRS_WGS84: Final[str] = "EPSG:4326"
#: CRS de area igual (Equal Earth / EASE-Grid 2.0) para calculo de area em km2.
CRS_EQUAL_AREA: Final[str] = "EPSG:6933"


class GeometryError(RuntimeError):
    """Falha ao construir ou carregar as geometrias dos polos produtores."""


@dataclass(frozen=True, slots=True)
class PoloDefinition:
    """Definicao de um polo produtor de referencia.

    Attributes:
        nome: identificador usado como chave em todo o pipeline.
        commodity: cultura predominante ("coffee_arabica", "soybean_corn").
        uf: unidade federativa principal.
        bbox: retangulo representativo (min_lon, min_lat, max_lon, max_lat).
        tickers: tickers financeiros correlacionados ao polo.
        descricao: contexto agronomico resumido.
    """

    nome: str
    commodity: str
    uf: str
    bbox: tuple[float, float, float, float]
    tickers: tuple[str, ...]
    descricao: str


#: Bounding boxes representativas (fallback offline, aproximadas).
POLO_DEFINITIONS: Final[tuple[PoloDefinition, ...]] = (
    PoloDefinition(
        nome="Sul_de_Minas",
        commodity="coffee_arabica",
        uf="MG",
        bbox=(-47.00, -22.90, -44.20, -20.60),
        tickers=("KC=F", "ICF=F"),
        descricao=(
            "Maior regiao produtora de cafe arabica do Brasil (Varginha, Tres "
            "Coracoes, Alfenas, Guaxupe); altitude 800-1200 m."
        ),
    ),
    PoloDefinition(
        nome="Cerrado_Mineiro",
        commodity="coffee_arabica",
        uf="MG",
        bbox=(-48.20, -20.00, -45.60, -17.30),
        tickers=("KC=F", "ICF=F"),
        descricao=(
            "Cafe arabica de Denominacao de Origem no Alto Paranaiba/Triangulo "
            "(Patrocinio, Monte Carmelo, Araguari); forte uso de irrigacao."
        ),
    ),
    PoloDefinition(
        nome="Sorriso_MT",
        commodity="soybean_corn",
        uf="MT",
        bbox=(-56.30, -13.30, -54.60, -11.60),
        tickers=("ZS=F", "ZC=F", "CCM=F", "SJC=F"),
        descricao=(
            "Maior municipio produtor de soja do Brasil, no medio-norte de MT; "
            "safrinha de milho apos a soja."
        ),
    ),
    PoloDefinition(
        nome="Oeste_PR",
        commodity="soybean_corn",
        uf="PR",
        bbox=(-54.30, -25.60, -52.20, -23.60),
        tickers=("ZS=F", "ZC=F", "CCM=F", "SJC=F"),
        descricao=(
            "Cascavel/Toledo/Marechal Candido Rondon; soja no verao e milho "
            "safrinha, historicamente sensivel a veranicos."
        ),
    ),
)

#: Indice por nome para acesso direto.
POLOS_BY_NAME: Final[dict[str, PoloDefinition]] = {
    polo.nome: polo for polo in POLO_DEFINITIONS
}


# -----------------------------------------------------------------------------
# Construcao das geometrias
# -----------------------------------------------------------------------------
def polos_from_bboxes(
    definitions: tuple[PoloDefinition, ...] = POLO_DEFINITIONS,
) -> gpd.GeoDataFrame:
    """Constroi o GeoDataFrame dos polos a partir das bounding boxes internas.

    Args:
        definitions: definicoes a materializar.

    Returns:
        GeoDataFrame em EPSG:4326 com colunas de atributos, `geometry` e
        `area_km2` (calculada em projecao de area igual).

    Raises:
        GeometryError: se geopandas/shapely nao estiverem disponiveis.
    """
    try:
        import geopandas as gpd_mod
        from shapely.geometry import box
    except ImportError as exc:  # pragma: no cover - validado na Etapa 1
        raise GeometryError(f"geopandas/shapely indisponiveis: {exc}") from exc

    records = [
        {
            "polo_produtor": polo.nome,
            "commodity": polo.commodity,
            "uf": polo.uf,
            "tickers": ",".join(polo.tickers),
            "descricao": polo.descricao,
            "min_lon": polo.bbox[0],
            "min_lat": polo.bbox[1],
            "max_lon": polo.bbox[2],
            "max_lat": polo.bbox[3],
            "geometry_source": "bbox_interna",
        }
        for polo in definitions
    ]
    geometries = [box(*polo.bbox) for polo in definitions]

    gdf = gpd_mod.GeoDataFrame(records, geometry=geometries, crs=CRS_WGS84)
    return _with_area(gdf)


def _with_area(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Acrescenta a coluna `area_km2` usando projecao de area igual."""
    try:
        gdf = gdf.copy()
        gdf["area_km2"] = (gdf.to_crs(CRS_EQUAL_AREA).area / 1_000_000).round(2)
    except Exception as exc:  # projecao indisponivel nao deve quebrar o pipeline
        logger.warning("Nao foi possivel calcular area_km2: %s", exc)
        gdf["area_km2"] = float("nan")
    return gdf


def polos_from_geojson(path: Path) -> gpd.GeoDataFrame:
    """Carrega os polos de um arquivo vetorial externo (GeoJSON/GPKG/SHP).

    O arquivo deve conter uma coluna `polo_produtor` (ou `nome`) e opcionalmente
    `commodity`/`uf`. Atributos ausentes sao completados com `POLO_DEFINITIONS`.

    Args:
        path: caminho do arquivo vetorial.

    Returns:
        GeoDataFrame em EPSG:4326.

    Raises:
        GeometryError: se o arquivo nao puder ser lido ou nao tiver a coluna de
            identificacao dos polos.
    """
    try:
        import geopandas as gpd_mod

        gdf = gpd_mod.read_file(path)
    except Exception as exc:
        raise GeometryError(f"Falha ao ler {path}: {exc}") from exc

    if "polo_produtor" not in gdf.columns:
        if "nome" in gdf.columns:
            gdf = gdf.rename(columns={"nome": "polo_produtor"})
        else:
            raise GeometryError(
                f"{path} deve conter a coluna 'polo_produtor' (ou 'nome')"
            )

    if gdf.crs is None:
        logger.warning("%s sem CRS declarado; assumindo %s", path, CRS_WGS84)
        gdf = gdf.set_crs(CRS_WGS84)
    elif gdf.crs.to_string() != CRS_WGS84:
        gdf = gdf.to_crs(CRS_WGS84)

    for column, default in (("commodity", "unknown"), ("uf", "unknown")):
        if column not in gdf.columns:
            gdf[column] = [
                getattr(POLOS_BY_NAME.get(str(nome)), column, default) or default
                for nome in gdf["polo_produtor"]
            ]

    gdf["geometry_source"] = f"geojson:{path.name}"
    bounds = gdf.geometry.bounds
    gdf["min_lon"], gdf["min_lat"] = bounds["minx"], bounds["miny"]
    gdf["max_lon"], gdf["max_lat"] = bounds["maxx"], bounds["maxy"]
    return _with_area(gdf)


def load_polos(
    settings: Settings | None = None,
    geojson_path: Path | str | None = None,
) -> gpd.GeoDataFrame:
    """Carrega os polos produtores com fallback automatico para bboxes.

    Args:
        settings: configuracao; se None usa `get_settings()`.
        geojson_path: caminho explicito de um vetorial; sobrepoe o .env.

    Returns:
        GeoDataFrame em EPSG:4326 com os polos produtores.

    Raises:
        GeometryError: se nem o arquivo externo nem o fallback funcionarem.
    """
    cfg = settings or get_settings()
    candidate = geojson_path or cfg.polos_geojson_path

    if candidate:
        path = Path(candidate).expanduser()
        if path.is_file():
            try:
                gdf = polos_from_geojson(path)
                gdf = _apply_scope(gdf, cfg)
                logger.info(
                    "Polos carregados do vetorial externo | path=%s | polos=%d",
                    path,
                    len(gdf),
                )
                return gdf
            except GeometryError as exc:
                logger.warning(
                    "Falha ao usar %s (%s); aplicando fallback de bounding boxes",
                    path,
                    exc,
                )
        else:
            logger.warning(
                "POLOS_GEOJSON_PATH=%s nao encontrado; usando bounding boxes internas",
                path,
            )

    gdf = _apply_scope(polos_from_bboxes(), cfg)
    logger.info(
        "Polos carregados via bounding boxes internas | polos=%d | area_total=%.0f km2",
        len(gdf),
        float(gdf["area_km2"].sum()),
    )
    return gdf


def _apply_scope(gdf: gpd.GeoDataFrame, settings: Settings) -> gpd.GeoDataFrame:
    """Restringe os polos aos ativos no escopo do projeto (ex.: cafe).

    Args:
        gdf: GeoDataFrame com todos os polos disponiveis.
        settings: configuracao com `scope_polos` (vazio = mantem todos).

    Returns:
        GeoDataFrame filtrado.
    """
    if not settings.scope_polos:
        return gdf
    filtrado = gdf[gdf["polo_produtor"].isin(settings.scope_polos)].reset_index(drop=True)
    if filtrado.empty:
        logger.warning(
            "Nenhum polo do escopo %s encontrado; mantendo todos",
            settings.scope_polos,
        )
        return gdf
    return filtrado


def polos_dataframe(settings: Settings | None = None) -> pd.DataFrame:
    """Retorna os atributos dos polos sem a coluna de geometria.

    Util para juntar metadados aos DataFrames de estatistica zonal e para
    persistir a dimensao dos polos na camada processed.

    Args:
        settings: configuracao; se None usa `get_settings()`.

    Returns:
        DataFrame com uma linha por polo produtor.
    """
    gdf = load_polos(settings)
    return gdf.drop(columns=["geometry"]).reset_index(drop=True)


def get_polo(nome: str, settings: Settings | None = None) -> gpd.GeoDataFrame:
    """Retorna um unico polo pelo nome.

    Args:
        nome: identificador do polo (ex.: "Sul_de_Minas").
        settings: configuracao; se None usa `get_settings()`.

    Returns:
        GeoDataFrame com uma linha.

    Raises:
        GeometryError: se o polo nao existir.
    """
    gdf = load_polos(settings)
    selected = gdf[gdf["polo_produtor"] == nome]
    if selected.empty:
        disponiveis = ", ".join(sorted(gdf["polo_produtor"]))
        raise GeometryError(f"Polo {nome!r} inexistente. Disponiveis: {disponiveis}")
    return selected.reset_index(drop=True)


def polos_bbox(settings: Settings | None = None) -> tuple[float, float, float, float]:
    """Bounding box que envolve todos os polos (min_lon, min_lat, max_lon, max_lat).

    Args:
        settings: configuracao; se None usa `get_settings()`.

    Returns:
        Extensao total dos polos, util para recortes preliminares de raster.
    """
    gdf = load_polos(settings)
    minx, miny, maxx, maxy = gdf.total_bounds
    return (float(minx), float(miny), float(maxx), float(maxy))


