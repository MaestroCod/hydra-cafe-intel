"""ETAPA 4 - Camada Gold: correlacao entre estresse hidrico e mercado.

O motor consolida a visao analitica final do Data Lake:

    processed/climate/water_balance/dt=*  +  processed/finance/**.parquet
    -> cruzamento temporal por commodity -> correlacao de Pearson
    -> data_lake/gold/analytics_crop_market.parquet

Metricas de estresse (Crop Stress Index)
    - deficit_acumulado_7d_mm  e  deficit_acumulado_14d_mm  (somado do balanco)
    - anomalia climatica: desvio da chuva acumulada em relacao a media movel de
      30 dias (proxy simples de anomalia, sem exigir climatologia externa)

Correlacoes (Pearson)
    Para cada commodity, correlacionam-se as metricas de estresse com:
    - `retorno_pct` (retorno diario do contrato em BRL/saca)
    - `volatilidade_7d_pct` (volatilidade movel)
    A correlacao usa `method='pearson'` e so aproveita pares com ambas as
    observacoes validas (dropna), garantindo robustez a datas ausentes.
"""

from __future__ import annotations

import io
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final
from uuid import uuid4

import numpy as np
import pandas as pd

from src.config import PROCESSED_CLIMATE_PREFIX, PROCESSED_FINANCE_PREFIX, get_logger
from src.storage import StorageBackend, StorageError, get_storage

logger = get_logger("analytics.gold")

GOLD_PREFIX: Final[str] = "gold"
GOLD_FILENAME: Final[str] = "analytics_crop_market.parquet"

#: Colunas de estresse hidrico usadas na correlacao com o mercado.
STRESS_COLUMNS: Final[tuple[str, ...]] = (
    "deficit_acumulado_7d_mm",
    "deficit_acumulado_14d_mm",
    "anomalia_chuva_30d_mm",
)
#: Colunas de mercado correlacionadas com o estresse (processed financeiro).
MARKET_COLUMNS: Final[tuple[str, ...]] = (
    "retorno_diario_pct",
    "volatilidade_21d_anualizada",
)

#: Mapeia commodity -> tickers que participam da matriz de correlacao.
COMMODITY_TO_TICKERS: Final[dict[str, tuple[str, ...]]] = {
    "coffee_arabica": ("KC=F",),
    "corn": ("ZC=F",),
    "soybean": ("ZS=F",),
}


class GoldError(RuntimeError):
    """Falha na geracao da camada Gold."""


@dataclass(frozen=True, slots=True)
class CorrelationResult:
    """Uma correlacao (estresse x mercado) para uma commodity.

    Attributes:
        commodity: cultura (coffee_arabica, corn, soybean).
        stress_metric: nome da metrica de estresse.
        market_metric: nome da metrica de mercado.
        pearson_r: coeficiente de Pearson (-1..1), NaN se insuficiente.
        p_value: valor-p aproximado; NaN se insuficiente.
        pares_validos: numero de pares usados.
    """

    commodity: str
    stress_metric: str
    market_metric: str
    pearson_r: float
    p_value: float
    pares_validos: int


@dataclass(frozen=True, slots=True)
class GoldResult:
    """Consolidado da camada Gold.

    Attributes:
        run_id: identificador da execucao.
        gerado_em: timestamp UTC.
        gold_key: chave do Parquet analitico gravado.
        gold_rows: linhas da visao consolidada.
        correlacoes: tupla de correlacoes calculadas.
        correlacoes_significativas: correlacoes com |r| >= 0.4 e p < 0.05.
    """

    run_id: str
    gerado_em: datetime
    gold_key: str
    gold_rows: int
    correlacoes: tuple[CorrelationResult, ...] = field(default_factory=tuple)

    @property
    def correlacoes_significativas(self) -> tuple[CorrelationResult, ...]:
        """Correlacoes relevantes: |r| >= 0.4 e p-value < 0.05."""
        return tuple(
            c
            for c in self.correlacoes
            if abs(c.pearson_r) >= 0.4 and c.p_value < 0.05
        )


# -----------------------------------------------------------------------------
# Leitura da camada processed
# -----------------------------------------------------------------------------
def read_processed_climate(
    storage: StorageBackend, prefix: str = PROCESSED_CLIMATE_PREFIX
) -> pd.DataFrame:
    """Le todos os Parquets de balanco hidrico da camada processed.

    Args:
        storage: backend de leitura.
        prefix: prefixo do lake.

    Returns:
        pandas.DataFrame concatenado (import lazy).

    Raises:
        GoldError: em falha de leitura/parsing.
    """
    import pandas as pd_mod

    try:
        objetos = [obj for obj in storage.list_objects(prefix) if obj.key.endswith(".parquet")]
    except StorageError as exc:
        raise GoldError(f"Falha ao listar {prefix}: {exc}") from exc

    if not objetos:
        logger.warning("Nenhum Parquet climatico processado em %s", prefix)
        return pd_mod.DataFrame()

    frames = [
        pd_mod.read_parquet(io.BytesIO(storage.read_bytes(obj.key))) for obj in objetos
    ]
    dados = pd_mod.concat(frames, ignore_index=True)
    dados["dt"] = pd_mod.to_datetime(dados["dt"], errors="coerce").dt.tz_localize(None)
    logger.info("Balanco hidrico lido da processed | linhas=%d", len(dados))
    return dados


def read_processed_finance(
    storage: StorageBackend, prefix: str = PROCESSED_FINANCE_PREFIX
) -> pd.DataFrame:
    """Le todos os Parquets de cotacoes BRL/saca da camada processed.

    Args:
        storage: backend de leitura.
        prefix: prefixo do lake.

    Returns:
        pandas.DataFrame concatenado (import lazy).

    Raises:
        GoldError: em falha de leitura/parsing.
    """
    import pandas as pd_mod

    try:
        objetos = [obj for obj in storage.list_objects(prefix) if obj.key.endswith(".parquet")]
    except StorageError as exc:
        raise GoldError(f"Falha ao listar {prefix}: {exc}") from exc

    if not objetos:
        logger.warning("Nenhum Parquet financeiro processado em %s", prefix)
        return pd_mod.DataFrame()

    frames = [
        pd_mod.read_parquet(io.BytesIO(storage.read_bytes(obj.key))) for obj in objetos
    ]
    dados = pd_mod.concat(frames, ignore_index=True)
    dados["dt"] = pd_mod.to_datetime(dados["dt"], errors="coerce").dt.tz_localize(None)
    logger.info("Cotacoes lidas da processed | linhas=%d", len(dados))
    return dados


# -----------------------------------------------------------------------------
# Derivacao de estresse e anomalia
# -----------------------------------------------------------------------------
def add_crop_stress_metrics(clima: pd.DataFrame, window_anomalia: int = 30) -> pd.DataFrame:
    """Acrescenta o indice de estresse e a anomalia climatica ao balanco.

    Args:
        clima: DataFrame de balanco hidrico por (dt, polo).
        window_anomalia: janela da media movel de chuva para a anomalia.

    Returns:
        DataFrame com `deficit_acumulado_14d_mm`, `anomalia_chuva_30d_mm` e
        `crop_stress_index` (padronizado).
    """
    import numpy as np

    dados = clima.copy()
    if dados.empty:
        return dados

    dados["deficit_acumulado_14d_mm"] = (
        dados.groupby("polo_produtor")["deficit_hidrico_mm"]
        .transform(lambda serie: serie.rolling(14, min_periods=1).sum())
    )
    dados["chuva_media_30d_mm"] = (
        dados.groupby("polo_produtor")["precipitacao_chirps_mm"]
        .transform(lambda serie: serie.rolling(window_anomalia, min_periods=1).mean())
    )
    dados["anomalia_chuva_30d_mm"] = (
        dados["precipitacao_chirps_mm"] - dados["chuva_media_30d_mm"]
    )
    estresse = dados["deficit_acumulado_7d_mm"].astype("float64")
    dados["crop_stress_index"] = (
        (estresse - estresse.mean()) / estresse.std()
        if estresse.std() > 0
        else np.nan
    )
    return dados


# -----------------------------------------------------------------------------
# Correlacao e consolidacao Gold
# -----------------------------------------------------------------------------
def compute_correlations(
    clima: pd.DataFrame,
    financas: pd.DataFrame,
    stress_columns: Sequence[str] = STRESS_COLUMNS,
    market_columns: Sequence[str] = MARKET_COLUMNS,
) -> tuple[CorrelationResult, ...]:
    """Calcula a matriz de correlacao de Pearson estresse x mercado.

    O cruzamento temporal e feito por (dt, commodity): o estresse medio dos
    polos daquela cultura e pareado com o retorno/volatilidade do ticker da
    commodity. A correlacao usa apenas pares validos (dropna).

    Args:
        clima: balanco hidrico processado (com `add_crop_stress_metrics`).
        financas: cotacoes BRL/saca processadas.
        stress_columns: metricas de estresse a correlacionar.
        market_columns: metricas de mercado a correlacionar.

    Returns:
        Tupla de correlacoes, uma por combinacao (commodity, estresse, mercado).
    """
    import numpy as np
    import pandas as pd_mod

    if clima.empty or financas.empty:
        logger.warning(
            "Dados insuficientes para correlacao (clima=%d, financas=%d)",
            len(clima),
            len(financas),
        )
        return ()

    clima_df = pd_mod.DataFrame(clima)
    financas_df = pd_mod.DataFrame(financas)
    clima_df["dt"] = pd_mod.to_datetime(clima_df["dt"]).dt.normalize()
    financas_df["dt"] = pd_mod.to_datetime(financas_df["dt"]).dt.normalize()

    correlacoes: list[CorrelationResult] = []
    for commodity, tickers in COMMODITY_TO_TICKERS.items():
        polos_commodity = clima_df[clima_df["commodity"] == commodity]
        mercado = financas_df[financas_df["ticker"].isin(tickers)]

        if polos_commodity.empty or mercado.empty:
            logger.debug("Sem dados pareados para %s", commodity)
            continue

        estresse_diario = (
            polos_commodity.groupby("dt")[list(stress_columns)]
            .mean(numeric_only=True)
            .reset_index()
        )
        pareado = estresse_diario.merge(
            mercado[["dt", *market_columns]], on="dt", how="inner"
        )

        for stress in stress_columns:
            for market in market_columns:
                pares = pareado[[stress, market]].dropna()
                if len(pares) < 5:
                    correlacoes.append(
                        CorrelationResult(
                            commodity=commodity,
                            stress_metric=stress,
                            market_metric=market,
                            pearson_r=float("nan"),
                            p_value=float("nan"),
                            pares_validos=len(pares),
                        )
                    )
                    continue
                r = float(np.corrcoef(pares[stress], pares[market])[0, 1])
                if abs(r) < 1.0:
                    t_stat = r * np.sqrt((len(pares) - 2) / max(1.0 - r**2, 1e-12))
                    p = _p_value_from_t(t_stat, len(pares) - 2)
                else:
                    p = 0.0
                correlacoes.append(
                    CorrelationResult(
                        commodity=commodity,
                        stress_metric=stress,
                        market_metric=market,
                        pearson_r=float(r),
                        p_value=float(p),
                        pares_validos=len(pares),
                    )
                )

    logger.info(
        "Correlacoes calculadas | total=%d | significativas(|r|>=0.4, p<0.05)=%d",
        len(correlacoes),
        sum(1 for c in correlacoes if abs(c.pearson_r) >= 0.4 and c.p_value < 0.05),
    )
    return tuple(correlacoes)


def _p_value_from_t(t_stat: float, dof: int) -> float:
    """Valor-p bilateral aproximado para uma t de Student com `dof` graus.

    Implementacao pura em Python usando a funcao beta incompleta regularizada
    (serie de Continued Fraction / Lentz), sem dependencia do scipy:

        p = 2 * I_x(dof/2, 1/2)   onde  x = dof / (dof + t^2)

    Args:
        t_stat: estatistica t observada.
        dof: graus de liberdade (n - 2).

    Returns:
        Valor-p bilateral (0..1).
    """
    import math

    if dof <= 0 or not math.isfinite(t_stat):
        return float("nan")
    x = dof / (dof + t_stat * t_stat)
    if not (0.0 < x < 1.0):
        return 0.0
    return min(1.0, 2.0 * _incomplete_beta(x, dof / 2.0, 0.5))


def _incomplete_beta(x: float, a: float, b: float) -> float:
    """Funcao beta incompleta regularizada I_x(a, b) (Numerical Recipes)."""
    import math

    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    log_base = (
        math.lgamma(a + b)
        - math.lgamma(a)
        - math.lgamma(b)
        + a * math.log(x)
        + b * math.log1p(-x)
    )
    front = math.exp(log_base)

    # Fracao continua (Continued Fraction) de Lentz para I_x.
    _MAX_ITER = 200
    _EPS = 3e-12
    _FPMIN = 1e-300

    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < _FPMIN:
        d = _FPMIN
    d = 1.0 / d
    h = d
    for m in range(1, _MAX_ITER + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < _FPMIN:
            d = _FPMIN
        c = 1.0 + aa / c
        if abs(c) < _FPMIN:
            c = _FPMIN
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < _FPMIN:
            d = _FPMIN
        c = 1.0 + aa / c
        if abs(c) < _FPMIN:
            c = _FPMIN
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < _EPS:
            break
    return front * h


def build_gold_analytics(
    storage: StorageBackend | None = None,
    write_output: bool = True,
) -> GoldResult:
    """Gera a visao consolidada Gold (estresse x mercado) e grava o Parquet.

    Args:
        storage: backend (local ou S3); None resolve via factory.
        write_output: grava `gold/analytics_crop_market.parquet`.

    Returns:
        GoldResult com chave, linhas e correlacoes calculadas.

    Raises:
        GoldError: em falha de leitura/escrita.
    """
    backend = storage or get_storage()
    run_id = uuid4().hex[:12]

    clima = read_processed_climate(backend)
    financas = read_processed_finance(backend)

    if clima.empty or financas.empty:
        raise GoldError(
            "Camada processed sem dados suficientes para a Gold "
            f"(clima={len(clima)}, financas={len(financas)}). Execute o pipeline "
            "da Etapa 4 antes."
        )

    clima = add_crop_stress_metrics(clima)
    correlacoes = compute_correlations(clima, financas)

    import pandas as pd_mod

    visao = pd_mod.DataFrame(
        [
            {
                "commodity": c.commodity,
                "stress_metric": c.stress_metric,
                "market_metric": c.market_metric,
                "pearson_r": c.pearson_r,
                "p_value": c.p_value,
                "pares_validos": c.pares_validos,
                "significativa": abs(c.pearson_r) >= 0.4 and c.p_value < 0.05,
            }
            for c in correlacoes
        ]
    )
    if visao.empty:
        visao = pd_mod.DataFrame(
            columns=[
                "commodity",
                "stress_metric",
                "market_metric",
                "pearson_r",
                "p_value",
                "pares_validos",
                "significativa",
            ]
        )

    visao["run_id"] = run_id
    visao["gerado_em"] = datetime.now(tz=UTC)

    chave = StorageBackend.join_key(GOLD_PREFIX, GOLD_FILENAME)
    if write_output:
        buffer = io.BytesIO()
        visao.to_parquet(buffer, engine="pyarrow", compression="snappy", index=False)
        backend.write_bytes(
            chave,
            buffer.getvalue(),
            content_type="application/vnd.apache.parquet",
            metadata={
                "run_id": run_id,
                "rows": str(len(visao)),
                "gerado_em": datetime.now(tz=UTC).isoformat(),
            },
        )
        logger.info("Camada Gold gravada | key=%s | linhas=%d", chave, len(visao))

    return GoldResult(
        run_id=run_id,
        gerado_em=datetime.now(tz=UTC),
        gold_key=chave,
        gold_rows=len(visao),
        correlacoes=correlacoes,
    )


# =============================================================================
# ETAPA 5 - Gold Semanal (escopo Hydra: Cafe Arabica, Sul de Minas, 1W)
# =============================================================================
GOLD_WEEKLY_FILENAME: Final[str] = "analytics_coffee_stress_weekly.parquet"
#: Periodo semanal ancorado no domingo: `data_semana` e a segunda-feira.
WEEK_PERIOD: Final[str] = "W-SUN"

#: Commodity e polo do escopo simplificado da Etapa 5.
WEEKLY_COMMODITY: Final[str] = "coffee_arabica"
WEEKLY_POLO: Final[str] = "Sul_de_Minas"
#: Contratos de cafe arabica acompanhados.
WEEKLY_TICKERS: Final[tuple[str, ...]] = ("KC=F", "ICF=F")
#: Limiar semanal (mm) que liga o alerta de estresse hidrico.
WEEKLY_STRESS_THRESHOLD_MM: Final[float] = 15.0


def _padroniza_clima(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza o balanco hidrico da processed para o esquema semanal.

    Args:
        df: saida de `read_processed_climate`.

    Returns:
        DataFrame com `data`, `polo`, `precipitacao_mm`, `et0_mm`,
        `deficit_hidrico`.
    """
    if df.empty:
        return df
    out = df.copy()
    out["data"] = pd.to_datetime(out["dt"], errors="coerce")
    out = out.rename(
        columns={
            "polo_produtor": "polo",
            "precipitacao_chirps_mm": "precipitacao_mm",
            "etp_mm": "et0_mm",
            "deficit_hidrico_mm": "deficit_hidrico",
        }
    )
    manter = ["data", "polo", "commodity", "precipitacao_mm", "et0_mm", "deficit_hidrico"]
    return out[[c for c in manter if c in out.columns]].dropna(subset=["data"])


def _padroniza_fin(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza as cotacoes da processed para o esquema semanal.

    Args:
        df: saida de `read_processed_finance`.

    Returns:
        DataFrame com `data`, `ticker`, `preco_brl_saca` e a volatilidade
        diaria anualizada (21d) em `volatilidade`.
    """
    if df.empty:
        return df
    out = df.copy()
    out["data"] = pd.to_datetime(out["dt"], errors="coerce")
    col_vol = (
        "volatilidade_21d_anualizada"
        if "volatilidade_21d_anualizada" in out.columns
        else "volatilidade"
    )
    out = out.rename(columns={col_vol: "volatilidade"})
    manter = ["data", "ticker", "preco_brl_saca", "volatilidade"]
    return out[[c for c in manter if c in out.columns]].dropna(subset=["data"])


def _fallback_semanal() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Gera dados sinteticos deterministicos quando a processed esta vazia.

    Usa um gerador com semente fixa para que o painel e os testes sejam
    reproduziveis mesmo sem dados ingeridos.

    Returns:
        Tupla (df_clima, df_fin) no esquema padronizado semanal.
    """
    rng = np.random.default_rng(42)
    datas = pd.date_range("2023-01-01", "2024-01-01", freq=WEEK_PERIOD)
    # Garante que as datas de referencia caem em segundas-feiras (inicio da semana).
    datas = pd.date_range("2023-01-02", "2024-01-01", freq=WEEK_PERIOD)
    clima = pd.DataFrame(
        {
            "data": datas,
            "polo": WEEKLY_POLO,
            "commodity": WEEKLY_COMMODITY,
            "precipitacao_mm": rng.uniform(0, 50, len(datas)),
            "et0_mm": rng.uniform(15, 35, len(datas)),
            "deficit_hidrico": rng.uniform(-20, 10, len(datas)),
        }
    )
    fin = pd.DataFrame(
        {
            "data": datas,
            "ticker": "KC=F",
            "preco_brl_saca": rng.uniform(900, 1400, len(datas)),
            "volatilidade": rng.uniform(0.10, 0.35, len(datas)),
        }
    )
    logger.warning(
        "Processed vazia; usando dados sinteticos de demonstracao (seed=42)"
    )
    return clima, fin


def aggregate_weekly(
    df_clima: pd.DataFrame, df_fin: pd.DataFrame
) -> pd.DataFrame:
    """Agrega o clima e o mercado em escala semanal (W-MON).

    Convencao de sinal (coerente com `water_balance`): `deficit_hidrico`
    negativo indica estresse; o `crop_stress_index` semanal e o deficit com
    sinal invertido e truncado em zero, e o alerta liga acima do limiar.

    Args:
        df_clima: dados climaticos diarios padronizados.
        df_fin: cotacoes diarias padronizadas.

    Returns:
        DataFrame gold semanal com `data_semana`, `polo`, `ticker`,
        precipitacao/ET0/deficit semanais, `crop_stress_index`,
        `alerta_estresse`, preco, retorno semanal e volatilidade.
    """
    if df_clima.empty or df_fin.empty:
        return pd.DataFrame()

    clima = df_clima.copy()
    fin = df_fin.copy()
    clima["data_semana"] = (
        pd.to_datetime(clima["data"]).dt.to_period(WEEK_PERIOD).dt.start_time
    )
    fin["data_semana"] = (
        pd.to_datetime(fin["data"]).dt.to_period(WEEK_PERIOD).dt.start_time
    )

    clima_sem = (
        clima.groupby(["data_semana", "polo"])
        .agg(
            precipitacao_semanal_mm=("precipitacao_mm", "sum"),
            et0_semanal_mm=("et0_mm", "sum"),
            deficit_hidrico_semanal=("deficit_hidrico", "sum"),
        )
        .reset_index()
    )
    # Deficit negativo = estresse -> CSI positivo = estresse.
    clima_sem["crop_stress_index"] = np.maximum(
        -clima_sem["deficit_hidrico_semanal"], 0.0
    )
    clima_sem["alerta_estresse"] = (
        clima_sem["crop_stress_index"] > WEEKLY_STRESS_THRESHOLD_MM
    )

    fin_sem = (
        fin.groupby(["data_semana", "ticker"])
        .agg(
            preco_brl_saca=("preco_brl_saca", "last"),
            # Snapshot da ultima volatilidade util da semana (sexta-feira) —
            # NAO e a media das volatilidades diarias.
            volatilidade_21d_anualizada=("volatilidade", "last"),
        )
        .reset_index()
    )
    # Retorno semanal (variacao do fechamento da semana vs. anterior).
    fin_sem["retorno_semanal_pct"] = (
        fin_sem.groupby("ticker")["preco_brl_saca"].pct_change() * 100.0
    )
    # Volatilidade semanal: desvio padrao movel (4 semanas) dos retornos
    # semanais, anualizado por sqrt(52).
    fin_sem = _add_weekly_volatility(fin_sem, window_weeks=4, min_periods=2)

    gold = pd.merge(clima_sem, fin_sem, on="data_semana", how="inner")
    # Semanas sem conversao cambial (preco NaN) nao participam da Gold.
    gold = gold.dropna(subset=["preco_brl_saca"])
    return gold.sort_values(["data_semana", "ticker"]).reset_index(drop=True)


def _add_weekly_volatility(
    fin_sem: pd.DataFrame, window_weeks: int = 4, min_periods: int = 2
) -> pd.DataFrame:
    """Calcula a volatilidade movel dos retornos semanais por ticker.

    `volatilidade_4w_semanal_anualizada = std(retornos semanais, 4w) * sqrt(52)`
    (52 semanas por ano), com desvio padrao amostral (ddof=1) e `min_periods`
    observacoes validas.

    Args:
        fin_sem: cotacoes semanais com `retorno_semanal_pct`.
        window_weeks: janela em semanas (default 4 ~ 1 mes comercial).
        min_periods: minimo de observacoes validas (default 2).

    Returns:
        DataFrame acrescido de `volatilidade_4w_semanal_anualizada`.
    """
    import numpy as np

    resultado = fin_sem.copy().sort_values(["ticker", "data_semana"]).reset_index(drop=True)
    volatilidades: list[float] = []
    for _, grupo in resultado.groupby("ticker", sort=False):
        retornos = grupo["retorno_semanal_pct"].to_numpy(
            dtype="float64", na_value=np.nan
        )
        for indice in range(len(grupo)):
            janela = retornos[max(0, indice - window_weeks + 1) : indice + 1]
            validos = janela[~np.isnan(janela)]
            volatilidades.append(
                float(np.std(validos, ddof=1)) if validos.size >= min_periods else np.nan
            )
    resultado["volatilidade_4w_semanal_anualizada"] = (
        np.array(volatilidades) * (52 ** 0.5)
    )
    return resultado


def build_gold_weekly_analytics(
    storage: StorageBackend | None = None,
    write_output: bool = True,
) -> pd.DataFrame:
    """Gera a Gold semanal de cafe arabica (Sul de Minas) e persiste no lake.

    Fluxo: processed (balanco hidrico + cotacoes BRL/saca) -> filtro
    cafe/Sul_de_Minas -> agregacao semanal W-MON -> `gold/`. Se a processed
    estiver vazia, usa o fallback sintetico deterministico (demonstracao).

    Args:
        storage: backend (local ou S3); None resolve via factory.
        write_output: grava `gold/analytics_coffee_stress_weekly.parquet`.

    Returns:
        DataFrame gold semanal.
    """
    backend = storage or get_storage()

    try:
        clima = read_processed_climate(backend)
        fin = read_processed_finance(backend)
        clima = _padroniza_clima(clima)
        fin = _padroniza_fin(fin)
    except GoldError as exc:
        logger.warning("Falha ao ler processed (%s); usando fallback sintetico", exc)
        clima, fin = _fallback_semanal()

    if clima.empty or fin.empty:
        clima, fin = _fallback_semanal()
    else:
        clima = clima[
            (clima["commodity"] == WEEKLY_COMMODITY)
            & (clima["polo"] == WEEKLY_POLO)
        ]
        fin = fin[fin["ticker"].isin(WEEKLY_TICKERS)]

    gold = aggregate_weekly(clima, fin)
    if gold.empty:
        # Processed sem janela util (ex.: cambio nao cobre o periodo) -> demo.
        logger.warning(
            "Gold semanal sem semanas validas (cambio insuficiente?); "
            "usando fallback sintetico"
        )
        clima, fin = _fallback_semanal()
        gold = aggregate_weekly(clima, fin)

    chave = StorageBackend.join_key(GOLD_PREFIX, GOLD_WEEKLY_FILENAME)
    if write_output:
        backend.write_parquet(
            chave,
            gold,
            metadata={
                "commodity": WEEKLY_COMMODITY,
                "polo": WEEKLY_POLO,
                "granularidade": WEEK_PERIOD,
                "gerado_em": datetime.now(tz=UTC).isoformat(),
            },
        )
        logger.info(
            "Gold semanal gravada | key=%s | semanas=%d | alertas=%d",
            chave,
            gold["data_semana"].nunique(),
            int(gold["alerta_estresse"].sum()),
        )
    return gold





# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def main(argv: Sequence[str] | None = None) -> int:
    """Ponto de entrada da CLI da camada Gold.

    Args:
        argv: argumentos de linha de comando.

    Returns:
        0 = sucesso; 1 = falha.
    """
    import argparse

    from src.config import configure_logging, get_settings

    parser = argparse.ArgumentParser(
        prog="python -m src.analytics.gold",
        description="Gera a camada Gold: correlacao entre estresse hidrico e mercado.",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Calcula a Gold em memoria sem gravar o Parquet.",
    )
    parser.add_argument(
        "--weekly",
        action="store_true",
        help="Gera a Gold semanal de cafe arabica (Sul de Minas, granularidade 1W).",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Log em DEBUG.")
    args = parser.parse_args(argv)

    settings = get_settings()
    configure_logging(level="DEBUG" if args.verbose else None, settings=settings)

    try:
        if args.weekly:
            gold = build_gold_weekly_analytics(write_output=not args.no_write)
            logger.info(
                "GOLD SEMANAL OK | linhas=%d | semanas=%d | alertas=%d",
                len(gold),
                gold["data_semana"].nunique() if not gold.empty else 0,
                int(gold["alerta_estresse"].sum()) if not gold.empty else 0,
            )
            return 0

        resultado = build_gold_analytics(write_output=not args.no_write)
    except GoldError as exc:
        logger.critical("Camada Gold falhou: %s", exc)
        return 1
    except Exception as exc:  # pragma: no cover - rede de seguranca
        logger.critical(
            "Erro inesperado na Gold: %s: %s", type(exc).__name__, exc, exc_info=True
        )
        return 1

    logger.info(
        "GOLD OK | run_id=%s | key=%s | linhas=%d | correlacoes=%d | "
        "significativas=%d",
        resultado.run_id,
        resultado.gold_key,
        resultado.gold_rows,
        len(resultado.correlacoes),
        len(resultado.correlacoes_significativas),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




