"""ETAPA 5 - Painel interativo (Streamlit + Plotly) do projeto Hydra.

Visoes:
    1. Cards de indicadores (Preco BRL/saca, Crop Stress Index, Alerta).
    2. Mapa das cidades do Sul de Minas (Varginha, Tres Coracoes, Alfenas,
       Guaxupe) com a BBox do polo e o alerta de estresse.
    3. Serie temporal semanal: barra de Crop Stress Index + linha de preco
       (eixo duplo).
    4. Heatmap de correlacao de Pearson entre clima e mercado.

Execucao:
    streamlit run app.py
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from src.analytics.gold import build_gold_weekly_analytics
from src.config import configure_logging, get_settings
from src.storage import ObjectNotFoundError, get_storage

#: Municipios de referencia do polo Sul de Minas.
CITIES_SOUTH_MG = pd.DataFrame(
    {
        "cidade": ["Varginha", "Três Corações", "Alfenas", "Guaxupé"],
        "latitude": [-21.551, -21.697, -21.420, -21.305],
        "longitude": [-45.430, -45.254, -45.947, -46.712],
    }
)

STRESS_COLUMNS = [
    "crop_stress_index",
    "deficit_hidrico_semanal",
    "precipitacao_semanal_mm",
    "et0_semanal_mm",
]
MARKET_COLUMNS = [
    "retorno_semanal_pct",
    "volatilidade_21d_anualizada",
    "volatilidade_4w_semanal_anualizada",
    "preco_brl_saca",
]


@st.cache_data(show_spinner=False)
def load_gold_weekly() -> pd.DataFrame:
    """Carrega a Gold semanal do lake; gera o fallback sintetico se ausente."""
    settings = get_settings()
    storage = get_storage(settings=settings)
    chave = "gold/analytics_coffee_stress_weekly.parquet"
    try:
        return storage.read_parquet(chave)
    except ObjectNotFoundError:
        return build_gold_weekly_analytics(storage=storage)


def build_corr_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Matriz de correlacao de Pearson entre clima e mercado."""
    subset = df[STRESS_COLUMNS + MARKET_COLUMNS].apply(pd.to_numeric, errors="coerce")
    return subset.corr(method="pearson")


def page_config() -> None:
    st.set_page_config(
        page_title="Hydra · Inteligência Café",
        page_icon="☕",
        layout="wide",
        initial_sidebar_state="collapsed",
    )


def _float(value: object) -> float:
    """Converte para float tratando NaN de forma segura (KPI do Streamlit)."""
    import math

    if value is None or (isinstance(value, float) and math.isnan(value)):
        return float("nan")
    return float(value)


def render_metrics(df: pd.DataFrame) -> None:
    """Cards de indicadores da ultima semana (preco, vol, CSI, chuva).

    - Preco Cafe (R$/saca) com delta do retorno semanal.
    - Volatilidade movel 21d em % a.a., delta em p.p. vs. semana anterior
      (delta positivo = aumento de risco -> cor inversa).
    - Deficit hidrico (Crop Stress Index) em mm.
    - Chuva semanal em mm, com alerta quando < 10 mm.
    """
    ordenado = df.sort_values("data_semana").reset_index(drop=True)
    ultima = ordenado.iloc[-1]
    anterior = ordenado.iloc[-2] if len(ordenado) > 1 else None

    preco = _float(ultima["preco_brl_saca"])
    retorno = _float(ultima["retorno_semanal_pct"]) if "retorno_semanal_pct" in ultima else float("nan")

    vol = _float(ultima["volatilidade_21d_anualizada"]) if "volatilidade_21d_anualizada" in ultima else float("nan")
    vol_ant = (
        _float(anterior["volatilidade_21d_anualizada"]) if anterior is not None else float("nan")
    )
    delta_vol = vol - vol_ant if not (pd.isna(vol) or pd.isna(vol_ant)) else None

    csi = _float(ultima["crop_stress_index"])
    chuva = _float(ultima["precipitacao_semanal_mm"]) if "precipitacao_semanal_mm" in ultima else float("nan")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Preço Café (R$/saca)",
        f"R$ {preco:,.2f}" if not pd.isna(preco) else "—",
        delta=f"{retorno:+.2f}%" if not pd.isna(retorno) else None,
    )
    c2.metric(
        "Volatilidade Móvel (21d)",
        f"{vol:.1f}% a.a." if not pd.isna(vol) else "—",
        delta=f"{delta_vol:+.1f} p.p." if delta_vol is not None else None,
        delta_color="inverse",  # aumento de volatilidade = vermelho (risco)
    )
    c3.metric("Déficit Hídrico (CSI)", f"{csi:.1f} mm")
    c4.metric(
        "Chuva Semanal",
        f"{chuva:.1f} mm" if not pd.isna(chuva) else "—",
        delta="⚠️ < 10 mm" if not pd.isna(chuva) and chuva < 10.0 else None,
        delta_color="inverse",
    )


def _scatter_map(data: pd.DataFrame, **kwargs: object):
    """`px.scatter_map` (Plotly >=6) com fallback para `scatter_mapbox`."""
    if hasattr(px, "scatter_map"):
        return px.scatter_map(data, **kwargs)
    kwargs["mapbox_style"] = kwargs.pop("map_style", "open-street-map")
    return px.scatter_mapbox(data, **kwargs)


def _scattermap_trace(**kwargs: object):
    """`go.Scattermap` (Plotly >=6) com fallback para `Scattermapbox`."""
    if hasattr(go, "Scattermap"):
        return go.Scattermap(**kwargs)
    return go.Scattermapbox(**kwargs)


def render_map(df: pd.DataFrame) -> None:
    """Mapa dos municipios do Sul de Minas sobre a BBox do polo."""
    ultima = df.sort_values("data_semana").iloc[-1]
    alerta = bool(ultima["alerta_estresse"])

    cidades = CITIES_SOUTH_MG.copy()
    cidades["tamanho"] = 14
    fig = _scatter_map(
        cidades,
        lat="latitude",
        lon="longitude",
        text="cidade",
        size="tamanho",
        zoom=7.5,
        center={"lat": -21.5, "lon": -45.7},
        map_style="open-street-map",
        title=f"Polo Sul de Minas · Semana {ultima['data_semana'].date()}",
    )
    fig.add_trace(
        _scattermap_trace(
            lat=[-22.90, -22.90, -20.60, -20.60, -22.90],
            lon=[-47.00, -44.20, -44.20, -47.00, -47.00],
            mode="lines",
            line={"width": 2, "color": "red" if alerta else "green"},
            name="BBox do polo",
        )
    )
    fig.update_layout(height=430, margin={"l": 0, "r": 0, "t": 40, "b": 0})
    st.plotly_chart(fig, width="stretch")


def render_series(df: pd.DataFrame) -> None:
    """Eixo duplo: barra do Crop Stress Index + linha do preco BRL/saca."""
    df = df.sort_values("data_semana")
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Bar(
            x=df["data_semana"],
            y=df["crop_stress_index"],
            name="Crop Stress Index (mm)",
            marker_color=np.where(df["alerta_estresse"], "#d62728", "#2ca02c"),
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Scatter(
            x=df["data_semana"],
            y=df["preco_brl_saca"],
            name="Preço BRL/saca",
            line={"color": "#1f77b4", "width": 2.5},
        ),
        secondary_y=True,
    )
    fig.update_yaxes(title_text="Crop Stress Index (mm)", secondary_y=False)
    fig.update_yaxes(title_text="Preço BRL/saca", secondary_y=True)
    fig.update_layout(
        title="Estresse hídrico × Preço do café (semanal)",
        height=430,
        hovermode="x unified",
    )
    st.plotly_chart(fig, width="stretch")


def render_heatmap(corr: pd.DataFrame) -> None:
    """Heatmap de correlacao de Pearson entre clima e mercado."""
    clima = [c for c in STRESS_COLUMNS if c in corr.index]
    mercado = [c for c in MARKET_COLUMNS if c in corr.columns]
    fig = px.imshow(
        corr.loc[clima, mercado],
        text_auto=".2f",
        color_continuous_scale="RdBu_r",
        zmin=-1,
        zmax=1,
        title="Correlação: anomalia climática × mercado",
    )
    fig.update_layout(height=380)
    st.plotly_chart(fig, width="stretch")


def main() -> None:
    settings = get_settings()
    configure_logging(log_file="dashboard.log", settings=settings)
    page_config()

    st.title("☕ Hydra — Inteligência Climática e Financeira do Café Arábica")
    st.caption(
        "Polo **Sul de Minas (MG)** · Granularidade **semanal (1W)** · "
        "Fonte: CHIRPS + ERA5-Land + Yahoo Finance"
    )

    df = load_gold_weekly()
    if df.empty:
        st.error("Sem dados para exibir. Execute a Gold semanal primeiro.")
        st.stop()

    render_metrics(df)

    tab1, tab2, tab3 = st.tabs(["🗺️ Mapa", "📈 Clima × Preço", "🧮 Correlação"])
    with tab1:
        render_map(df)
    with tab2:
        render_series(df)
    with tab3:
        render_heatmap(build_corr_matrix(df))

    st.divider()
    st.caption(
        "Fallback de demonstração: dados sintéticos determinísticos (seed=42) "
        "quando a camada processed estiver vazia."
    )


if __name__ == "__main__":
    main()

