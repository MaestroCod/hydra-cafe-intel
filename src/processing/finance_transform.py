"""ETAPA 4 - Transformacao financeira: USD internacional -> BRL por saca.

Fluxo:
    raw/finance/**.parquet  ->  join temporal com BRL=X  ->  conversao por
    fatores de contrato  ->  retornos diarios e volatilidade movel de 7 dias

Fatores de conversao (unidades de mercado)
    KC=F (ICE Arabica)  : cents USD / libra-peso     -> 1 saca 60 kg = 132.277 lb
    ZC=F (CBOT Corn)    : cents USD / bushel (25.40 kg) -> 60 kg = 2.3622 bu
    ZS=F (CBOT Soybean) : cents USD / bushel (27.2155 kg) -> 60 kg = 2.2046 bu
    CCM=F / SJC=F / ICF=F (B3): ja cotados por saca/60kg em USD ou BRL

Formula geral:
    preco_usd_unidade = close / divisor_para_usd
    preco_usd_saca    = preco_usd_unidade * unidades_por_saca
    preco_brl_saca    = preco_usd_saca * usd_brl        (se moeda origem = USD)

O cambio (`BRL=X` = BRL por 1 USD) e alinhado por `merge_asof` com
`direction="backward"`, ou seja, feriados/fins de semana herdam a ultima cotacao
disponivel - comportamento padrao em mesas de risco.
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from src.config import RAW_FINANCE_PREFIX, Settings, get_logger, get_settings
from src.storage import StorageBackend, StorageError

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

logger = get_logger("processing.finance_transform")

FX_TICKER: Final[str] = "BRL=X"
#: Libras-peso em uma saca de 60 kg.
POUNDS_PER_SACA: Final[float] = 132.2774
#: Bushels em uma saca de 60 kg (milho: 56 lb/bu; soja: 60 lb/bu).
CORN_BUSHELS_PER_SACA: Final[float] = 60.0 / 25.4012
SOY_BUSHELS_PER_SACA: Final[float] = 60.0 / 27.2155


class FinanceTransformError(RuntimeError):
    """Falha na transformacao financeira da camada Silver."""


@dataclass(frozen=True, slots=True)
class ConversionSpec:
    """Regra de conversao de um contrato para BRL por saca de 60 kg.

    Attributes:
        commodity: cultura de referencia.
        moeda_origem: "USD" ou "BRL".
        unidade_origem: unidade cotada na bolsa.
        divisor_para_usd: divide a cotacao bruta (100 para centavos).
        unidades_por_saca: quantas unidades cotadas cabem em 1 saca de 60 kg.
        exchange: bolsa de origem.
    """

    commodity: str
    moeda_origem: str
    unidade_origem: str
    divisor_para_usd: float
    unidades_por_saca: float
    exchange: str


#: Especificacoes por ticker do Yahoo Finance.
CONVERSION_SPECS: Final[dict[str, ConversionSpec]] = {
    "KC=F": ConversionSpec(
        commodity="coffee_arabica",
        moeda_origem="USD",
        unidade_origem="cents_usd_per_pound",
        divisor_para_usd=100.0,
        unidades_por_saca=POUNDS_PER_SACA,
        exchange="ICE",
    ),
    "ZC=F": ConversionSpec(
        commodity="corn",
        moeda_origem="USD",
        unidade_origem="cents_usd_per_bushel",
        divisor_para_usd=100.0,
        unidades_por_saca=CORN_BUSHELS_PER_SACA,
        exchange="CBOT",
    ),
    "ZS=F": ConversionSpec(
        commodity="soybean",
        moeda_origem="USD",
        unidade_origem="cents_usd_per_bushel",
        divisor_para_usd=100.0,
        unidades_por_saca=SOY_BUSHELS_PER_SACA,
        exchange="CBOT",
    ),
    "ICF=F": ConversionSpec(
        commodity="coffee_arabica",
        moeda_origem="USD",
        unidade_origem="usd_per_saca_60kg",
        divisor_para_usd=1.0,
        unidades_por_saca=1.0,
        exchange="B3",
    ),
    "CCM=F": ConversionSpec(
        commodity="corn",
        moeda_origem="BRL",
        unidade_origem="brl_per_saca_60kg",
        divisor_para_usd=1.0,
        unidades_por_saca=1.0,
        exchange="B3",
    ),
    "SJC=F": ConversionSpec(
        commodity="soybean",
        moeda_origem="BRL",
        unidade_origem="brl_per_saca_60kg",
        divisor_para_usd=1.0,
        unidades_por_saca=1.0,
        exchange="B3",
    ),
}


# -----------------------------------------------------------------------------
# Leitura da camada raw
# -----------------------------------------------------------------------------
def read_raw_finance(
    storage: StorageBackend, prefix: str = RAW_FINANCE_PREFIX
) -> pd.DataFrame:
    """Le todos os Parquets de cotacoes da camada raw via storage.

    Args:
        storage: backend de leitura (local ou S3).
        prefix: prefixo do lake a varrer.

    Returns:
        DataFrame concatenado com as colunas originais + `source_key`.
        Vazio se nao houver objetos.

    Raises:
        FinanceTransformError: se algum Parquet estiver corrompido ou o storage
            falhar.
    """
    import pandas as pd_mod

    try:
        objetos = [obj for obj in storage.list_objects(prefix) if obj.key.endswith(".parquet")]
    except StorageError as exc:
        raise FinanceTransformError(f"Falha ao listar {prefix}: {exc}") from exc

    if not objetos:
        logger.warning("Nenhum Parquet de cotacoes em %s", prefix)
        return pd_mod.DataFrame()

    frames: list[pd.DataFrame] = []
    for obj in objetos:
        try:
            frame = pd_mod.read_parquet(io.BytesIO(storage.read_bytes(obj.key)))
        except (StorageError, ValueError, OSError) as exc:
            raise FinanceTransformError(f"Parquet invalido em {obj.key}: {exc}") from exc
        frame["source_key"] = obj.key
        frames.append(frame)

    dados = pd_mod.concat(frames, ignore_index=True)
    logger.info(
        "Cotacoes lidas da camada raw | objetos=%d | linhas=%d | tickers=%s",
        len(objetos),
        len(dados),
        sorted(dados["ticker"].unique().tolist()) if "ticker" in dados else [],
    )
    return dados


def _normalize_dates(frame: pd.DataFrame) -> pd.DataFrame:
    """Garante a coluna `dt` (date) a partir de `date`/`dt`, sem timezone."""
    import pandas as pd_mod

    dados = frame.copy()
    coluna = "date" if "date" in dados.columns else "dt"
    if coluna not in dados.columns:
        raise FinanceTransformError("Cotacoes sem coluna de data ('date' ou 'dt')")
    dados["dt"] = pd_mod.to_datetime(dados[coluna], errors="coerce").dt.tz_localize(None)
    dados = dados.dropna(subset=["dt"])
    return dados


def split_fx(
    frame: pd.DataFrame, fx_ticker: str = FX_TICKER
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Separa as cotacoes de commodities da serie de cambio.

    Args:
        frame: dados brutos concatenados.
        fx_ticker: ticker do cambio (default "BRL=X").

    Returns:
        Tupla (commodities, cambio). O cambio tem colunas `dt` e `usd_brl`.
    """
    import pandas as pd_mod

    if frame.empty:
        return frame, pd_mod.DataFrame(columns=["dt", "usd_brl"])

    dados = _normalize_dates(frame)
    e_cambio = dados["ticker"].astype(str).str.upper() == fx_ticker.upper()

    cambio = (
        dados.loc[e_cambio, ["dt", "close"]]
        .rename(columns={"close": "usd_brl"})
        .dropna(subset=["usd_brl"])
        .drop_duplicates(subset=["dt"], keep="last")
        .sort_values("dt")
        .reset_index(drop=True)
    )
    commodities = dados.loc[~e_cambio].copy()

    if cambio.empty:
        logger.warning(
            "Serie de cambio %s ausente na camada raw; conversao para BRL ficara NaN "
            "nos contratos cotados em USD",
            fx_ticker,
        )
    else:
        logger.info(
            "Cambio %s | %d cotacoes | %s..%s | ultimo=%.4f",
            fx_ticker,
            len(cambio),
            cambio["dt"].min().date(),
            cambio["dt"].max().date(),
            float(cambio["usd_brl"].iloc[-1]),
        )
    return commodities, cambio


# -----------------------------------------------------------------------------
# Conversao para BRL/saca + metricas de risco
# -----------------------------------------------------------------------------
def apply_conversion(
    commodities: pd.DataFrame,
    cambio: pd.DataFrame,
    specs: dict[str, ConversionSpec] = CONVERSION_SPECS,
    saca_weight_kg: float = 60.0,
) -> pd.DataFrame:
    """Aplica as regras de conversao de contrato para preco em BRL por saca.

    Args:
        commodities: cotacoes sem o ticker de cambio (ja com `dt` normalizada).
        cambio: serie `dt` / `usd_brl` (pode vir vazia).
        specs: especificacoes de conversao por ticker.
        saca_weight_kg: peso da saca de referencia (padrao 60 kg).

    Returns:
        DataFrame com `preco_usd_saca`, `usd_brl_utilizado`, `preco_brl_saca` e
        as colunas derivadas de moeda/unidade.

    Raises:
        FinanceTransformError: se algum ticker de commodity nao tiver spec.
    """
    import numpy as np
    import pandas as pd_mod

    dados = commodities.copy()
    if dados.empty:
        return dados

    dados["ticker"] = dados["ticker"].astype(str).str.upper()
    desconhecidos = sorted(set(dados["ticker"]) - set(specs))
    if desconhecidos:
        raise FinanceTransformError(
            f"Sem regra de conversao para os tickers: {desconhecidos}"
        )

    dados["spec_commodity"] = dados["ticker"].map(lambda t: specs[t].commodity)
    dados["spec_moeda_origem"] = dados["ticker"].map(lambda t: specs[t].moeda_origem)
    dados["spec_unidade_origem"] = dados["ticker"].map(lambda t: specs[t].unidade_origem)
    dados["spec_divisor_para_usd"] = dados["ticker"].map(
        lambda t: specs[t].divisor_para_usd
    )
    dados["spec_unidades_por_saca"] = dados["ticker"].map(
        lambda t: specs[t].unidades_por_saca
    )
    dados["spec_exchange"] = dados["ticker"].map(lambda t: specs[t].exchange)

    dados["preco_usd_unidade"] = dados["close"] / dados["spec_divisor_para_usd"]
    dados["preco_usd_saca"] = (
        dados["preco_usd_unidade"] * dados["spec_unidades_por_saca"]
    )

    if cambio.empty:
        dados["usd_brl_utilizado"] = np.nan
    else:
        dados = dados.sort_values("dt").reset_index(drop=True)
        cambio = cambio.sort_values("dt").reset_index(drop=True)
        # Preserva a data original da cotacao; o merge_asof cola a ultima taxa
        # disponivel (backward) em uma coluna auxiliar.
        dados = pd_mod.merge_asof(
            dados,
            cambio.rename(columns={"dt": "dt_cambio", "usd_brl": "usd_brl_utilizado"}),
            left_on="dt",
            right_on="dt_cambio",
            direction="backward",
            allow_exact_matches=True,
        )
        dados = dados.drop(columns=["dt_cambio"])
        dados = dados.reset_index(drop=True)

    e_brl = dados["spec_moeda_origem"] == "BRL"
    dados.loc[e_brl, "preco_brl_saca"] = dados.loc[e_brl, "preco_usd_saca"]
    dados.loc[~e_brl, "preco_brl_saca"] = (
        dados.loc[~e_brl, "preco_usd_saca"] * dados.loc[~e_brl, "usd_brl_utilizado"]
    )
    dados["saca_kg_referencia"] = saca_weight_kg

    colunas_manter = [
        coluna
        for coluna in (
            "dt",
            "ticker",
            "commodity",
            "exchange",
            "close",
            "open",
            "high",
            "low",
            "volume",
            "adj_close",
            "preco_usd_unidade",
            "preco_usd_saca",
            "usd_brl_utilizado",
            "preco_brl_saca",
            "saca_kg_referencia",
            "source_key",
            "ingested_at",
            "run_id",
        )
        if coluna in dados.columns
    ]
    return dados[colunas_manter]


def add_risk_metrics(
    dados: pd.DataFrame, window_days: int = 21, min_periods: int = 5
) -> pd.DataFrame:
    """Acrescenta retorno diario e volatilidade movel ao preco BRL/saca.

    Convencoes (refatoracao da volatilidade):
        - `retorno_diario_pct = preco_brl_saca.pct_change() * 100` (% por dia);
        - `volatilidade_21d_diaria` = desvio padrao amostral (ddof=1) dos
          retornos diarios em janela movel de 21 pregoes (~1 mes comercial),
          exigindo no minimo `min_periods` observacoes validas (senao NaN);
        - `volatilidade_21d_anualizada = vol_21d_diaria * sqrt(252)`
          (252 dias uteis por ano).

    Args:
        dados: saida de `apply_conversion`.
        window_days: tamanho da janela movel em pregoes (default 21).
        min_periods: minimo de observacoes para calcular o desvio (default 5).

    Returns:
        DataFrame com `retorno_diario_pct`, `volatilidade_21d_diaria` e
        `volatilidade_21d_anualizada`.
    """
    import numpy as np

    resultado = dados.copy()
    if resultado.empty:
        return resultado

    resultado = resultado.sort_values(["ticker", "dt"]).reset_index(drop=True)
    agrupado = resultado.groupby("ticker", group_keys=False)
    resultado["retorno_diario_pct"] = (
        agrupado["preco_brl_saca"].pct_change() * 100.0
    )

    # Desvio padrao movel por ticker (loop explicito para controlar
    # min_periods/ddof sem depender do bug de buffer do `.rolling().std()`).
    volatilidades: list[float] = []
    for _, grupo in agrupado:
        retornos = grupo["retorno_diario_pct"].to_numpy(
            dtype="float64", na_value=np.nan
        )
        for indice in range(len(grupo)):
            janela = retornos[max(0, indice - window_days + 1) : indice + 1]
            validos = janela[~np.isnan(janela)]
            volatilidades.append(
                float(np.std(validos, ddof=1)) if validos.size >= min_periods else np.nan
            )
    resultado[f"volatilidade_{window_days}d_diaria"] = volatilidades
    #: Volatilidade anualizada assumindo 252 dias uteis por ano.
    resultado["volatilidade_21d_anualizada"] = (
        resultado[f"volatilidade_{window_days}d_diaria"] * (252 ** 0.5)
    )
    return resultado



def transform_finance(
    storage: StorageBackend,
    settings: Settings | None = None,
    specs: dict[str, ConversionSpec] = CONVERSION_SPECS,
    fx_ticker: str = FX_TICKER,
    volatility_window_days: int = 21,
    max_days: int | None = None,
) -> pd.DataFrame:
    """Pipeline financeiro completo: raw -> cotacoes BRL/saca + risco.

    Args:
        storage: backend de leitura da camada raw.
        settings: configuracao; se None usa `get_settings()`.
        specs: regras de conversao por ticker.
        fx_ticker: ticker do cambio (default "BRL=X").
        volatility_window_days: janela da volatilidade movel.
        max_days: limita o historico aos ultimos N dias de negocio.

    Returns:
        DataFrame Silver de cotacoes convertidas com metricas de risco.
        Vazio se nao houver cotacoes na camada raw.

    Raises:
        FinanceTransformError: em qualquer falha de leitura ou conversao.
    """
    cfg = settings or get_settings()
    try:
        import pandas as pd_mod

        bruto = read_raw_finance(storage)
        commodities, cambio = split_fx(bruto, fx_ticker=fx_ticker)
        convertido = apply_conversion(
            commodities,
            cambio,
            specs=specs,
            saca_weight_kg=cfg.saca_weight_kg,
        )
        risco = add_risk_metrics(convertido, window_days=volatility_window_days)
    except FinanceTransformError:
        raise
    except StorageError as exc:
        raise FinanceTransformError(f"Falha no storage: {exc}") from exc

    if risco.empty:
        return risco

    if max_days is not None and max_days > 0:
        limite = risco["dt"].max() - pd_mod.Timedelta(days=max_days)
        risco = risco[risco["dt"] >= limite]

    logger.info(
        "Transformacao financeira | linhas=%d | tickers=%s | faixa=%s..%s",
        len(risco),
        sorted(risco["ticker"].unique()),
        risco["dt"].min().date(),
        risco["dt"].max().date(),
    )
    return risco.sort_values(["ticker", "dt"]).reset_index(drop=True)



