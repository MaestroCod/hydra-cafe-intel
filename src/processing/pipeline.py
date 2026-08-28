"""ETAPA 4 - Orquestracao Raw -> Processed (Silver) com particionamento Hive.

Fluxo climatico:
    raw/climate_chirps/dt=*  +  raw/climate_era5/dt=*
    -> chirps_zonal_stats -> daily_from_hourly -> water_balance
    -> processed/climate/water_balance/dt=YYYY-MM-DD/water_balance.parquet

Fluxo financeiro:
    raw/finance/**.parquet (inclui BRL=X)
    -> transform_finance
    -> processed/finance/cotacoes_brl_saca/ticker=<TICKER>/cotacoes_brl_saca.parquet

Datas sem cobertura de alguma fonte nao interrompem o pipeline: geram linhas
com indicadores `NaN` e flags `*_disponivel=False`, preservando a integridade
temporal (requisito de idoneidade com ausencia parcial de dados).
"""

from __future__ import annotations

import argparse
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Final
from uuid import uuid4

from src.config import (
    PROCESSED_CLIMATE_PREFIX,
    PROCESSED_FINANCE_PREFIX,
    Settings,
    configure_logging,
    get_logger,
    get_settings,
)
from src.processing.finance_transform import transform_finance
from src.processing.geometry import load_polos, polos_dataframe
from src.processing.water_balance import daily_from_hourly, water_balance
from src.processing.zonal_stats import chirps_zonal_stats, era5_zonal_hourly
from src.storage import StorageBackend, StorageError, get_storage

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

logger = get_logger("processing.pipeline")

PARQUET_CONTENT_TYPE: Final[str] = "application/vnd.apache.parquet"
WATER_BALANCE_FILENAME: Final[str] = "water_balance.parquet"
FINANCE_FILENAME: Final[str] = "cotacoes_brl_saca.parquet"


class PipelineError(RuntimeError):
    """Falha na orquestracao do pipeline de transformacao."""


@dataclass(frozen=True, slots=True)
class PipelineResult:
    """Consolidado de uma execucao do pipeline.

    Attributes:
        run_id: identificador da execucao.
        processado_em: timestamp UTC da execucao.
        climate_keys: chaves gravadas no fluxo climatico.
        finance_keys: chaves gravadas no fluxo financeiro.
        finance_rows: total de linhas de cotacoes transformadas.
        alertas_estresse: total de alertas por polo.
    """

    run_id: str
    processado_em: datetime
    climate_keys: tuple[str, ...] = field(default_factory=tuple)
    finance_keys: tuple[str, ...] = field(default_factory=tuple)
    finance_rows: int = 0
    alertas_estresse: int = 0


# -----------------------------------------------------------------------------
# Datas disponiveis na camada raw
# -----------------------------------------------------------------------------
def dates_from_partitions(storage: StorageBackend, prefix: str) -> tuple[date, ...]:
    """Extrai as datas `dt=YYYY-MM-DD` dos objetos sob um prefixo.

    Args:
        storage: backend de leitura.
        prefix: prefixo do lake (ex.: "raw/climate_chirps").

    Returns:
        Tupla de datas ordenadas e unicas.
    """
    import pandas as pd_mod

    datas: set[date] = set()
    for obj in storage.list_objects(prefix):
        for segmento in obj.key.split("/"):
            if segmento.startswith("dt="):
                try:
                    datas.add(pd_mod.Timestamp(segmento[3:]).date())
                except (ValueError, TypeError):
                    logger.debug("Segmento de particao ignorado: %s", segmento)
                break
    return tuple(sorted(datas))


def default_date_range(settings: Settings, dias: int = 3) -> tuple[date, date]:
    """Janela de datas recentes para backfill quando a raw esta vazia.

    Args:
        settings: configuracao.
        dias: quantidade de dias retroativos.

    Returns:
        Tupla (inicio, fim).
    """
    fim = datetime.now(tz=UTC).date()
    return fim - timedelta(days=dias), fim


# -----------------------------------------------------------------------------
# Escrita com particionamento Hive
# -----------------------------------------------------------------------------
def write_hive_partitioned(
    frame: pd.DataFrame,
    storage: StorageBackend,
    prefix: str,
    partition_column: str,
    filename: str,
    run_id: str,
) -> tuple[str, ...]:
    """Grava um DataFrame particionado por `partition_column` como Parquet.

    A chave resultante segue o padrao Hive, legivel por Athena/Glue:

        <prefix>/<coluna>=<valor>/<filename>

    Args:
        frame: DataFrame a persistir.
        storage: backend de destino.
        prefix: prefixo da camada processed.
        partition_column: coluna que vira particao Hive.
        filename: nome do arquivo dentro de cada particao.
        run_id: identificador da execucao (usado nos metadados).

    Returns:
        Tupla com as chaves gravadas.

    Raises:
        PipelineError: se a serializacao ou a escrita falhar.
    """
    import io

    import pandas as pd_mod

    chaves: list[str] = []
    if frame is None or frame.empty:
        logger.warning("Nada para gravar em %s (DataFrame vazio)", prefix)
        return tuple(chaves)

    dados = frame.copy()
    dados["dt"] = pd_mod.to_datetime(dados["dt"], errors="coerce").dt.date
    dados["processed_at"] = datetime.now(tz=UTC)
    dados["pipeline_run_id"] = run_id

    for valor, grupo in dados.groupby(partition_column, sort=True):
        try:
            buffer = io.BytesIO()
            grupo.to_parquet(
                buffer, engine="pyarrow", compression="snappy", index=False
            )
        except (ValueError, ImportError, OSError) as exc:
            raise PipelineError(
                f"Falha ao serializar Parquet em {prefix}: {exc}"
            ) from exc

        chave = StorageBackend.join_key(
            prefix, f"{partition_column}={valor}", filename
        )
        try:
            storage.write_bytes(
                chave,
                buffer.getvalue(),
                content_type=PARQUET_CONTENT_TYPE,
                metadata={
                    "run_id": run_id,
                    "partition_value": str(valor),
                    "rows": str(len(grupo)),
                    "processed_at": datetime.now(tz=UTC).isoformat(),
                },
            )
            chaves.append(chave)
            logger.info("Processed gravado | key=%s | linhas=%d", chave, len(grupo))
        except StorageError as exc:
            raise PipelineError(f"Falha ao gravar {chave}: {exc}") from exc

    return tuple(chaves)


# -----------------------------------------------------------------------------
# Fluxo climatico
# -----------------------------------------------------------------------------
def run_climate_pipeline(
    storage: StorageBackend | None = None,
    settings: Settings | None = None,
    start: date | None = None,
    end: date | None = None,
    write_outputs: bool = True,
) -> tuple[pd.DataFrame, PipelineResult]:
    """Executa CHIRPS/ERA5 -> zonal -> diario -> balanco hidrico -> processed.

    Args:
        storage: backend (local ou S3); None resolve via factory.
        settings: configuracao; None usa `get_settings()`.
        start: data inicial; None usa as datas disponiveis na raw.
        end: data final; None usa o maximo disponivel.
        write_outputs: grava os Parquets na camada processed.

    Returns:
        Tupla (DataFrame de balanco hidrico, PipelineResult com as chaves).

    Raises:
        PipelineError: em falhas de leitura/escrita do storage.
    """
    cfg = settings or get_settings()
    backend = storage or get_storage(settings=cfg)
    polos_gdf = load_polos(cfg)
    polos_df = polos_dataframe(cfg)
    run_id = uuid4().hex[:12]

    datas_raw = dates_from_partitions(backend, "raw/climate_chirps")
    if not datas_raw:
        datas_raw = dates_from_partitions(backend, "raw/climate_era5")
    if not datas_raw:
        inicio_default, fim_default = default_date_range(cfg)
        datas_raw = tuple(
            inicio_default + timedelta(days=offset)
            for offset in range((fim_default - inicio_default).days + 1)
        )
        logger.warning(
            "Nenhuma data na raw climatica; usando janela de fallback %s..%s",
            inicio_default,
            fim_default,
        )

    inicio = start or (datas_raw[0] if datas_raw else None)
    fim = end or (datas_raw[-1] if datas_raw else None)
    datas = tuple(
        data
        for data in datas_raw
        if (inicio is None or data >= inicio) and (fim is None or data <= fim)
    )

    logger.info(
        "Pipeline climatico | run_id=%s | backend=%s | datas=%d (%s..%s)",
        run_id,
        backend.name,
        len(datas),
        datas[0] if datas else None,
        datas[-1] if datas else None,
    )

    chirps_daily = chirps_zonal_stats(dates=datas, polos=polos_gdf, storage=backend)
    era5_hourly = era5_zonal_hourly(dates=datas, polos=polos_gdf, storage=backend)
    era5_daily = daily_from_hourly(era5_hourly, polos=polos_df)

    balanco = water_balance(
        chirps_daily=chirps_daily,
        era5_daily=era5_daily,
        polos=polos_df,
        settings=cfg,
    )

    climate_keys: tuple[str, ...] = ()
    if write_outputs:
        climate_keys = write_hive_partitioned(
            balanco,
            backend,
            PROCESSED_CLIMATE_PREFIX,
            partition_column="dt",
            filename=WATER_BALANCE_FILENAME,
            run_id=run_id,
        )

    alertas = int(balanco["alerta_estresse_hidrico"].sum()) if not balanco.empty else 0
    resultado = PipelineResult(
        run_id=run_id,
        processado_em=datetime.now(tz=UTC),
        climate_keys=climate_keys,
        alertas_estresse=alertas,
    )
    logger.info(
        "Pipeline climatico concluido | run_id=%s | linhas=%d | chaves=%d | alertas=%d",
        run_id,
        len(balanco),
        len(climate_keys),
        alertas,
    )
    return balanco, resultado


# -----------------------------------------------------------------------------
# Fluxo financeiro
# -----------------------------------------------------------------------------
def run_finance_pipeline(
    storage: StorageBackend | None = None,
    settings: Settings | None = None,
    write_outputs: bool = True,
    max_days: int | None = None,
) -> tuple[pd.DataFrame, PipelineResult]:
    """Executa raw/finance -> cotacoes BRL/saca + risco -> processed.

    Args:
        storage: backend (local ou S3); None resolve via factory.
        settings: configuracao; None usa `get_settings()`.
        write_outputs: grava os Parquets na camada processed.
        max_days: limita o historico aos ultimos N dias.

    Returns:
        Tupla (DataFrame transformado, PipelineResult com as chaves).

    Raises:
        PipelineError: em falhas de leitura/escrita do storage.
    """
    cfg = settings or get_settings()
    backend = storage or get_storage(settings=cfg)
    run_id = uuid4().hex[:12]

    try:
        financeiro = transform_finance(backend, settings=cfg, max_days=max_days)
    except Exception as exc:
        raise PipelineError(f"Transformacao financeira falhou: {exc}") from exc

    finance_keys: tuple[str, ...] = ()
    if write_outputs and not financeiro.empty:
        financeiro = financeiro.copy()
        financeiro["ticker_safe"] = financeiro["ticker"].str.replace("=", "_", regex=False)
        finance_keys = write_hive_partitioned(
            financeiro,
            backend,
            PROCESSED_FINANCE_PREFIX,
            partition_column="ticker_safe",
            filename=FINANCE_FILENAME,
            run_id=run_id,
        )

    resultado = PipelineResult(
        run_id=run_id,
        processado_em=datetime.now(tz=UTC),
        finance_keys=finance_keys,
        finance_rows=len(financeiro),
    )
    logger.info(
        "Pipeline financeiro concluido | run_id=%s | linhas=%d | chaves=%d",
        run_id,
        len(financeiro),
        len(finance_keys),
    )
    return financeiro, resultado


def run_pipeline(
    storage: StorageBackend | None = None,
    settings: Settings | None = None,
    start: date | None = None,
    end: date | None = None,
    write_outputs: bool = True,
    finance_max_days: int | None = None,
) -> PipelineResult:
    """Orquestra os fluxos climatico e financeiro ponta a ponta.

    Args:
        storage: backend; None resolve via factory.
        settings: configuracao; None usa `get_settings()`.
        start/end: janela do fluxo climatico.
        write_outputs: grava os Parquets na camada processed.
        finance_max_days: limita o historico financeiro transformado.

    Returns:
        PipelineResult consolidado (chaves climaticas + financeiras).

    Raises:
        PipelineError: se algum fluxo falhar.
    """
    cfg = settings or get_settings()
    backend = storage or get_storage(settings=cfg)

    _balanco, clima = run_climate_pipeline(
        storage=backend,
        settings=cfg,
        start=start,
        end=end,
        write_outputs=write_outputs,
    )
    _financeiro, financas = run_finance_pipeline(
        storage=backend,
        settings=cfg,
        write_outputs=write_outputs,
        max_days=finance_max_days,
    )

    return PipelineResult(
        run_id=clima.run_id,
        processado_em=datetime.now(tz=UTC),
        climate_keys=clima.climate_keys,
        finance_keys=financas.finance_keys,
        finance_rows=financas.finance_rows,
        alertas_estresse=clima.alertas_estresse,
    )


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    """Cria o parser de argumentos do pipeline de transformacao."""
    parser = argparse.ArgumentParser(
        prog="python -m src.processing.pipeline",
        description=(
            "Executa a transformacao Raw -> Processed (Silver): estatistica zonal, "
            "balanco hidrico FAO-56 e cotacoes BRL/saca com risco."
        ),
    )
    parser.add_argument(
        "--start", default=None, help="Data inicial do fluxo climatico (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--end", default=None, help="Data final do fluxo climatico (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--climate-only",
        action="store_true",
        help="Executa apenas o fluxo climatico.",
    )
    parser.add_argument(
        "--finance-only",
        action="store_true",
        help="Executa apenas o fluxo financeiro.",
    )
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="Processa em memoria sem gravar na camada processed.",
    )
    parser.add_argument(
        "--finance-max-days",
        type=int,
        default=None,
        help="Limita o historico financeiro aos ultimos N dias.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Log em DEBUG.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Ponto de entrada da CLI do pipeline.

    Args:
        argv: argumentos de linha de comando.

    Returns:
        0 = sucesso; 1 = falha tecnica.
    """
    args = build_arg_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(level="DEBUG" if args.verbose else None, settings=settings)

    try:
        start = date.fromisoformat(args.start) if args.start else None
        end = date.fromisoformat(args.end) if args.end else None
        storage = get_storage(settings=settings)

        if args.finance_only:
            resultado = run_finance_pipeline(
                storage=storage,
                settings=settings,
                write_outputs=not args.no_write,
                max_days=args.finance_max_days,
            )[1]
        elif args.climate_only:
            resultado = run_climate_pipeline(
                storage=storage,
                settings=settings,
                start=start,
                end=end,
                write_outputs=not args.no_write,
            )[1]
        else:
            resultado = run_pipeline(
                storage=storage,
                settings=settings,
                start=start,
                end=end,
                write_outputs=not args.no_write,
                finance_max_days=args.finance_max_days,
            )
    except (PipelineError, StorageError, ValueError) as exc:
        logger.critical("Pipeline falhou: %s: %s", type(exc).__name__, exc)
        return 1
    except Exception as exc:  # pragma: no cover - rede de seguranca
        logger.critical(
            "Erro inesperado no pipeline: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        return 1

    logger.info(
        "PIPELINE OK | run_id=%s | climate_keys=%d | finance_keys=%d | "
        "finance_rows=%d | alertas_estresse=%d",
        resultado.run_id,
        len(resultado.climate_keys),
        len(resultado.finance_keys),
        resultado.finance_rows,
        resultado.alertas_estresse,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())




