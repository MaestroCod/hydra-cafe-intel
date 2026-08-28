"""ETAPA 4 - Consolidacao diaria do ERA5-Land e balanco hidrico (FAO-56 simpl.).

Etapas:
    1. `daily_from_hourly`: reduz as 24 h do ERA5-Land a indicadores diarios
       (Tmax, Tmin, Tmean, precipitacao, ETP, umidade do solo).
    2. `water_balance`: cruza a chuva do CHIRPS com a ETP e calcula
       `deficit_hidrico_mm = precipitacao_chirps_mm - etp_mm`, o deficit
       acumulado em janela movel e a flag `alerta_estresse_hidrico`.

Convencoes de unidade (ERA5-Land)
    - `t2m` chega em K e e convertido para degC no modulo `zonal_stats`.
    - `tp` e `pev` sao ACUMULADOS desde 00 UTC e chegam em mm (x1000). O total
      diario e obtido somando os incrementos positivos da serie horaria, o que
      equivale ao ultimo valor quando a acumulacao e monotonica e continua
      correto quando ha reset no meio da serie.
    - `pev` do ERA5 e negativo (fluxo para a atmosfera); `zonal_stats` aplica
       valor absoluto, portanto aqui a ETP ja e positiva.

Fallback de ETP (FAO-56 simplificado)
    Sem `pev` disponivel, a ET0 e estimada por Hargreaves-Samani
    (ET0 = 0.0023 * Ra * (Tmean + 17.8) * sqrt(Tmax - Tmin)), recomendada pela
    FAO-56 quando so ha dados de temperatura. A coluna `etp_fonte` registra a
    origem ("era5_pev", "hargreaves" ou "indisponivel").
"""

from __future__ import annotations

import math
from datetime import date
from typing import TYPE_CHECKING, Final

from src.config import Settings, get_logger, get_settings

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

logger = get_logger("processing.water_balance")

#: Constante solar em MJ m-2 min-1 (FAO-56, eq. 21).
SOLAR_CONSTANT: Final[float] = 0.0820
#: Coeficiente de Hargreaves-Samani (FAO-56, eq. 52).
HARGREAVES_COEFFICIENT: Final[float] = 0.0023
#: Fator de conversao de MJ m-2 dia-1 para mm de agua equivalente.
MJ_TO_MM: Final[float] = 0.408

#: Variaveis acumuladas do ERA5 (total diario = soma dos incrementos).
ACCUMULATED_COLUMNS: Final[tuple[str, ...]] = (
    "precipitacao_mm",
    "evapotranspiracao_potencial_mm",
    "evaporacao_total_mm",
    "radiacao_solar_j_m2",
)


class WaterBalanceError(RuntimeError):
    """Falha no calculo do balanco hidrico."""


# -----------------------------------------------------------------------------
# ETP por Hargreaves-Samani (fallback FAO-56)
# -----------------------------------------------------------------------------
def extraterrestrial_radiation(latitude_deg: float, day_of_year: int) -> float:
    """Radiacao extraterrestre Ra em MJ m-2 dia-1 (FAO-56, eq. 21).

    Args:
        latitude_deg: latitude em graus decimais (negativa no hemisferio sul).
        day_of_year: dia juliano (1-366).

    Returns:
        Ra em MJ m-2 dia-1.
    """
    phi = math.radians(latitude_deg)
    dr = 1 + 0.033 * math.cos(2 * math.pi * day_of_year / 365)
    delta = 0.409 * math.sin(2 * math.pi * day_of_year / 365 - 1.39)
    argumento = max(-1.0, min(1.0, -math.tan(phi) * math.tan(delta)))
    omega = math.acos(argumento)
    return (
        (24 * 60 / math.pi)
        * SOLAR_CONSTANT
        * dr
        * (omega * math.sin(phi) * math.sin(delta)
           + math.cos(phi) * math.cos(delta) * math.sin(omega))
    )


def hargreaves_et0(
    tmax_c: float,
    tmin_c: float,
    tmean_c: float | None,
    latitude_deg: float,
    target_date: date,
) -> float:
    """Estima a ET0 diaria por Hargreaves-Samani (FAO-56, eq. 52).

    Args:
        tmax_c: temperatura maxima diaria (degC).
        tmin_c: temperatura minima diaria (degC).
        tmean_c: temperatura media; se None usa (tmax + tmin) / 2.
        latitude_deg: latitude do centroide do polo.
        target_date: data de referencia (define o dia juliano).

    Returns:
        ET0 em mm/dia; `nan` se as temperaturas forem invalidas.
    """
    if any(valor is None or math.isnan(valor) for valor in (tmax_c, tmin_c)):
        return float("nan")
    amplitude = max(tmax_c - tmin_c, 0.0)
    media = tmean_c if tmean_c is not None and not math.isnan(tmean_c) else (tmax_c + tmin_c) / 2
    radiacao = extraterrestrial_radiation(latitude_deg, target_date.timetuple().tm_yday)
    et0 = HARGREAVES_COEFFICIENT * MJ_TO_MM * radiacao * (media + 17.8) * math.sqrt(amplitude)
    return max(float(et0), 0.0)


# -----------------------------------------------------------------------------
# Consolidacao horaria -> diaria
# -----------------------------------------------------------------------------
def total_from_accumulated(series: pd.Series) -> float:
    """Total diario de uma variavel acumulada do ERA5-Land.

    Soma apenas os incrementos positivos da serie horaria (robusto a resets de
    acumulacao no meio do dia) e adiciona o primeiro valor observado.

    Args:
        series: serie horaria da variavel acumulada (ordenada no tempo).

    Returns:
        Total diario; `nan` se a serie nao tiver nenhum valor valido.
    """
    valores = series.dropna()
    if valores.empty:
        return float("nan")
    incrementos = valores.diff()
    return float(valores.iloc[0] + incrementos[incrementos > 0].sum())


def daily_from_hourly(
    hourly: pd.DataFrame,
    polos: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Consolida a serie horaria do ERA5-Land em indicadores diarios por polo.

    Args:
        hourly: saida de `era5_zonal_hourly` (pode vir vazia).
        polos: metadados dos polos para recuperar a latitude do centroide
            (colunas `polo_produtor`, `min_lat`, `max_lat`); opcional.

    Returns:
        DataFrame com uma linha por (`dt`, `polo_produtor`) contendo
        `temp_max_c`, `temp_min_c`, `temp_media_c`, `amplitude_termica_c`,
        `precipitacao_era5_mm`, `etp_era5_mm`, `umidade_solo_m3m3`,
        `horas_disponiveis` e `era5_disponivel`.
    """
    import pandas as pd_mod

    if hourly is None or hourly.empty:
        logger.warning("Serie horaria ERA5 vazia; nenhum indicador diario gerado")
        return pd_mod.DataFrame()

    dados = hourly.copy()
    dados["timestamp"] = pd_mod.to_datetime(dados["timestamp"])
    registros: list[dict[str, object]] = []

    for (dia, polo), grupo in dados.groupby(["dt", "polo_produtor"], sort=True):
        grupo = grupo.sort_values("timestamp")
        temperatura = grupo.get("temperatura_2m_c")

        registro: dict[str, object] = {
            "dt": dia,
            "polo_produtor": polo,
            "commodity": grupo["commodity"].iloc[0] if "commodity" in grupo else "unknown",
            "uf": grupo["uf"].iloc[0] if "uf" in grupo else "unknown",
            "horas_disponiveis": int(grupo["timestamp"].nunique()),
            "era5_disponivel": True,
        }

        if temperatura is not None and temperatura.notna().any():
            registro["temp_max_c"] = float(temperatura.max())
            registro["temp_min_c"] = float(temperatura.min())
            registro["temp_media_c"] = float(temperatura.mean())
            registro["amplitude_termica_c"] = float(temperatura.max() - temperatura.min())
        else:
            for coluna in ("temp_max_c", "temp_min_c", "temp_media_c", "amplitude_termica_c"):
                registro[coluna] = float("nan")

        registro["precipitacao_era5_mm"] = (
            total_from_accumulated(grupo["precipitacao_mm"])
            if "precipitacao_mm" in grupo
            else float("nan")
        )
        registro["etp_era5_mm"] = (
            total_from_accumulated(grupo["evapotranspiracao_potencial_mm"])
            if "evapotranspiracao_potencial_mm" in grupo
            else float("nan")
        )
        registro["umidade_solo_m3m3"] = (
            float(grupo["umidade_solo_camada1_m3m3"].mean())
            if "umidade_solo_camada1_m3m3" in grupo
            else float("nan")
        )
        registros.append(registro)

    diario = pd_mod.DataFrame(registros)
    if polos is not None and not polos.empty:
        diario = _attach_latitude(diario, polos)

    logger.info(
        "Consolidacao diaria ERA5 | linhas=%d | datas=%d | polos=%d",
        len(diario),
        diario["dt"].nunique(),
        diario["polo_produtor"].nunique(),
    )
    return diario.sort_values(["dt", "polo_produtor"]).reset_index(drop=True)


def _attach_latitude(diario: pd.DataFrame, polos: pd.DataFrame) -> pd.DataFrame:
    """Acrescenta a latitude do centroide de cada polo (para Hargreaves)."""
    if {"min_lat", "max_lat"}.issubset(polos.columns):
        referencia = polos[["polo_produtor", "min_lat", "max_lat"]].copy()
        referencia["latitude_centroide"] = (
            referencia["min_lat"] + referencia["max_lat"]
        ) / 2
        return diario.merge(
            referencia[["polo_produtor", "latitude_centroide"]],
            on="polo_produtor",
            how="left",
        )
    return diario


# -----------------------------------------------------------------------------
# Balanco hidrico e alerta de estresse
# -----------------------------------------------------------------------------
def water_balance(
    chirps_daily: pd.DataFrame,
    era5_daily: pd.DataFrame | None = None,
    polos: pd.DataFrame | None = None,
    settings: Settings | None = None,
    require_full_window: bool = True,
) -> pd.DataFrame:
    """Calcula o balanco hidrico diario e o alerta de estresse por polo.

    Regras:
        - `deficit_hidrico_mm = precipitacao_chirps_mm - etp_mm`
          (negativo = a demanda atmosferica superou a chuva).
        - `deficit_acumulado_Nd_mm`: soma movel de N dias (N = janela do .env).
        - `alerta_estresse_hidrico`: True quando o acumulado fica abaixo do
          limiar critico; exige janela completa se `require_full_window`.
        - ETP: usa `etp_era5_mm`; se ausente, tenta Hargreaves; se ainda faltar
          temperatura, o deficit fica `NaN` e `dados_completos=False`.

    Args:
        chirps_daily: saida de `chirps_zonal_stats`.
        era5_daily: saida de `daily_from_hourly` (pode ser None/vazia).
        polos: metadados dos polos (para latitude do centroide).
        settings: configuracao; se None usa `get_settings()`.
        require_full_window: exige janela cheia para acionar o alerta.

    Returns:
        DataFrame por (`dt`, `polo_produtor`) com as metricas de balanco hidrico.

    Raises:
        WaterBalanceError: se `chirps_daily` nao tiver as colunas minimas.
    """
    import numpy as np
    import pandas as pd_mod

    cfg = settings or get_settings()

    if chirps_daily is None or chirps_daily.empty:
        logger.warning("CHIRPS diario vazio; balanco hidrico nao pode ser calculado")
        return pd_mod.DataFrame()

    obrigatorias = {"dt", "polo_produtor"}
    if not obrigatorias.issubset(chirps_daily.columns):
        raise WaterBalanceError(
            f"chirps_daily precisa das colunas {sorted(obrigatorias)}"
        )

    base = chirps_daily.copy()
    coluna_chuva = (
        "precipitacao_media_ponderada_mm"
        if "precipitacao_media_ponderada_mm" in base.columns
        else "precipitacao_media_mm"
    )
    base["precipitacao_chirps_mm"] = base[coluna_chuva]

    if era5_daily is not None and not era5_daily.empty:
        colunas_era5 = [
            coluna
            for coluna in (
                "dt",
                "polo_produtor",
                "temp_max_c",
                "temp_min_c",
                "temp_media_c",
                "amplitude_termica_c",
                "precipitacao_era5_mm",
                "etp_era5_mm",
                "umidade_solo_m3m3",
                "horas_disponiveis",
                "era5_disponivel",
                "latitude_centroide",
            )
            if coluna in era5_daily.columns
        ]
        base = base.merge(
            era5_daily[colunas_era5], on=["dt", "polo_produtor"], how="left"
        )

    for coluna in (
        "temp_max_c",
        "temp_min_c",
        "temp_media_c",
        "amplitude_termica_c",
        "precipitacao_era5_mm",
        "etp_era5_mm",
        "umidade_solo_m3m3",
    ):
        if coluna not in base.columns:
            base[coluna] = np.nan
    if "era5_disponivel" not in base.columns:
        base["era5_disponivel"] = False
    base["era5_disponivel"] = base["era5_disponivel"].fillna(False).astype(bool)
    if "horas_disponiveis" not in base.columns:
        base["horas_disponiveis"] = 0
    base["horas_disponiveis"] = base["horas_disponiveis"].fillna(0).astype(int)
    if "chirps_disponivel" not in base.columns:
        base["chirps_disponivel"] = base["precipitacao_chirps_mm"].notna()

    if "latitude_centroide" not in base.columns and polos is not None:
        base = _attach_latitude(base, polos)
    if "latitude_centroide" not in base.columns:
        base["latitude_centroide"] = np.nan

    base["etp_mm"], base["etp_fonte"] = _resolve_etp(base)
    base["deficit_hidrico_mm"] = base["precipitacao_chirps_mm"] - base["etp_mm"]
    base["dados_completos"] = base["deficit_hidrico_mm"].notna() & base[
        "chirps_disponivel"
    ].fillna(False).astype(bool)

    return _apply_rolling_window(base, cfg, require_full_window)


def _apply_rolling_window(
    base: pd.DataFrame, cfg: Settings, require_full_window: bool
) -> pd.DataFrame:
    """Aplica a janela movel do deficit e liga a flag de alerta.

    Args:
        base: DataFrame com `deficit_hidrico_mm` por (dt, polo).
        cfg: configuracao (janela e limiar).
        require_full_window: exige janela cheia para o alerta.

    Returns:
        DataFrame ordenado com as colunas de acumulado e alerta.
    """
    janela = max(int(cfg.water_stress_window_days), 1)
    limiar = float(cfg.water_stress_deficit_mm)
    coluna_acumulado = f"deficit_acumulado_{janela}d_mm"

    base = base.sort_values(["polo_produtor", "dt"]).reset_index(drop=True)
    agrupado = base.groupby("polo_produtor", group_keys=False)

    base[coluna_acumulado] = agrupado["deficit_hidrico_mm"].transform(
        lambda serie: serie.rolling(janela, min_periods=1).sum()
    )
    base["dias_na_janela"] = (
        agrupado["deficit_hidrico_mm"]
        .transform(lambda serie: serie.notna().rolling(janela, min_periods=1).sum())
        .astype("int64")
    )
    base[f"precipitacao_acumulada_{janela}d_mm"] = agrupado[
        "precipitacao_chirps_mm"
    ].transform(lambda serie: serie.rolling(janela, min_periods=1).sum())

    minimo_dias = janela if require_full_window else 1
    base["janela_completa"] = base["dias_na_janela"] >= minimo_dias
    base["alerta_estresse_hidrico"] = (
        base[coluna_acumulado].notna()
        & base["janela_completa"]
        & (base[coluna_acumulado] < limiar)
    )
    base["limiar_estresse_mm"] = limiar
    base["janela_estresse_dias"] = janela

    logger.info(
        "Balanco hidrico | linhas=%d | ETP=%s | alertas=%d | limiar=%.1f mm/%dd",
        len(base),
        base["etp_fonte"].value_counts().to_dict(),
        int(base["alerta_estresse_hidrico"].sum()),
        limiar,
        janela,
    )
    return base.sort_values(["dt", "polo_produtor"]).reset_index(drop=True)


def _resolve_etp(base: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Define a ETP diaria e sua origem (ERA5 pev, Hargreaves ou indisponivel).

    Args:
        base: DataFrame com `etp_era5_mm`, temperaturas e `latitude_centroide`.

    Returns:
        Tupla (serie de ETP em mm, serie com a origem do valor).
    """
    import numpy as np
    import pandas as pd_mod

    etp = base["etp_era5_mm"].astype("float64").copy()
    fonte = pd_mod.Series(
        np.where(etp.notna(), "era5_pev", "indisponivel"), index=base.index
    )

    for indice in base.index[etp.isna()]:
        tmax = base.at[indice, "temp_max_c"]
        tmin = base.at[indice, "temp_min_c"]
        latitude = base.at[indice, "latitude_centroide"]
        dia = base.at[indice, "dt"]
        if (
            pd_mod.isna(tmax)
            or pd_mod.isna(tmin)
            or pd_mod.isna(latitude)
            or pd_mod.isna(dia)
        ):
            continue
        estimativa = hargreaves_et0(
            float(tmax), float(tmin), None, float(latitude), _as_date(dia)
        )
        if not math.isnan(estimativa):
            etp.at[indice] = estimativa
            fonte.at[indice] = "hargreaves"

    return etp, fonte


def _as_date(valor: object) -> date:
    """Converte diferentes representacoes de data para `datetime.date`."""
    import pandas as pd_mod

    if isinstance(valor, date):
        return valor
    return pd_mod.Timestamp(valor).date()



