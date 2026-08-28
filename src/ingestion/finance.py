"""ETAPA 2 - Ingestao do Eixo Financeiro (Yahoo Finance -> camada raw).

Fluxo (100% em memoria, sem arquivos temporarios em disco):

    yfinance -> pandas.DataFrame -> metadados de auditoria
             -> BytesIO (Parquet/snappy) -> StorageBackend (local ou S3)

Layout de destino (particionamento estilo Hive, compativel com Athena/Glue):

    raw/finance/ticker_safe=<TICKER_SEGURO>/dt=<YYYY-MM-DD>/<TICKER>_<intervalo>.parquet

A chave de particao usa `ticker_safe` (valor sanitizado, ex.: "KC_F") para NAO
colidir com a coluna `ticker` gravada dentro do arquivo (ex.: "KC=F") - colunas
de particao e colunas de dados com o mesmo nome causam erro de merge de schema
no PyArrow/Athena.

`dt` e a data logica da ingestao (batch), permitindo reprocessar um dia sem
afetar os anteriores. A data de cada cotacao permanece na coluna `date`.

Uso:
    python -m src.ingestion.finance --period 1mo
    python -m src.ingestion.finance --start 2015-01-01 --tickers KC=F,ZC=F
"""

from __future__ import annotations

import argparse
import io
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Final, Literal
from uuid import uuid4

import pandas as pd

from src.config import (
    RAW_FINANCE_PREFIX,
    Settings,
    configure_logging,
    get_logger,
    get_settings,
    resolve_relative_date,
)
from src.storage import StorageBackend, StorageError, get_storage

logger = get_logger("ingestion.finance")

SOURCE_NAME: Final[str] = "yahoo_finance"
PARQUET_CONTENT_TYPE: Final[str] = "application/vnd.apache.parquet"
PARQUET_COMPRESSION: Final[str] = "snappy"

IngestionStatus = Literal["written", "no_data", "skipped", "failed"]

#: Enriquecimento de dominio: commodity, bolsa e moeda de cada contrato futuro.
COMMODITY_METADATA: Final[dict[str, dict[str, str]]] = {
    "KC=F": {"commodity": "coffee_arabica", "exchange": "ICE", "currency": "USD"},
    "ICF=F": {"commodity": "coffee_arabica", "exchange": "B3", "currency": "USD"},
    "ZC=F": {"commodity": "corn", "exchange": "CBOT", "currency": "USD"},
    "CCM=F": {"commodity": "corn", "exchange": "B3", "currency": "BRL"},
    "ZS=F": {"commodity": "soybean", "exchange": "CBOT", "currency": "USD"},
    "SJC=F": {"commodity": "soybean", "exchange": "B3", "currency": "BRL"},
    "BRL=X": {"commodity": "fx_usd_brl", "exchange": "FX", "currency": "BRL"},
}


class FinanceIngestionError(RuntimeError):
    """Falha irrecuperavel na ingestao de um ticker financeiro."""


@dataclass(frozen=True, slots=True)
class IngestionResult:
    """Resultado da ingestao de um unico ticker.

    Attributes:
        ticker: simbolo original (ex.: "KC=F").
        status: "written" | "no_data" | "skipped" | "failed".
        rows: numero de linhas persistidas.
        key: chave logica gravada no lake (None quando nada foi gravado).
        uri: URI fisica do objeto (file:// ou s3://).
        size_bytes: tamanho do Parquet gerado.
        error: mensagem de erro quando status == "failed".
    """

    ticker: str
    status: IngestionStatus
    rows: int = 0
    key: str | None = None
    uri: str | None = None
    size_bytes: int = 0
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        """True quando o objeto foi efetivamente gravado ou ja existia."""
        return self.status in ("written", "skipped")


@dataclass(frozen=True, slots=True)
class IngestionRun:
    """Consolidado de uma execucao (batch) da ingestao financeira."""

    run_id: str
    partition_date: date
    results: tuple[IngestionResult, ...] = field(default_factory=tuple)

    @property
    def total_rows(self) -> int:
        """Total de linhas gravadas no batch."""
        return sum(result.rows for result in self.results)

    @property
    def failures(self) -> tuple[IngestionResult, ...]:
        """Tickers que falharam com erro tecnico."""
        return tuple(r for r in self.results if r.status == "failed")

    @property
    def empties(self) -> tuple[IngestionResult, ...]:
        """Tickers sem dados no provedor (ex.: simbolos B3 no Yahoo)."""
        return tuple(r for r in self.results if r.status == "no_data")


# -----------------------------------------------------------------------------
# Helpers de chave/particao
# -----------------------------------------------------------------------------
def sanitize_ticker(ticker: str) -> str:
    """Converte um ticker em token seguro para caminhos e valores Hive.

    O "=" quebraria o parsing de particoes `chave=valor`, e o "^"/"." nao sao
    amigaveis em S3/Windows.

    Example:
        >>> sanitize_ticker("KC=F")
        'KC_F'
    """
    safe = ticker.strip().upper()
    for char in ("=", "^", ".", " ", "/", "\\", ":"):
        safe = safe.replace(char, "_")
    return safe


def build_object_key(
    ticker: str,
    partition_date: date,
    interval: str = "1d",
    prefix: str = RAW_FINANCE_PREFIX,
) -> str:
    """Monta a chave Hive-particionada do objeto na camada raw.

    A particao usa `ticker_safe` para nao colidir com a coluna `ticker` do
    arquivo (evita ArrowTypeError ao ler o dataset completo).

    Example:
        >>> build_object_key("KC=F", date(2026, 8, 24))
        'raw/finance/ticker_safe=KC_F/dt=2026-08-24/KC_F_1d.parquet'
    """
    safe = sanitize_ticker(ticker)
    return StorageBackend.join_key(
        prefix,
        f"ticker_safe={safe}",
        f"dt={partition_date.isoformat()}",
        f"{safe}_{interval}.parquet",
    )


# -----------------------------------------------------------------------------
# Extracao (yfinance) com retry e backoff exponencial
# -----------------------------------------------------------------------------
def fetch_history(
    ticker: str,
    start: str | None = None,
    end: str | None = None,
    period: str | None = None,
    interval: str = "1d",
    max_retries: int = 3,
    backoff_seconds: float = 2.0,
) -> pd.DataFrame:
    """Baixa o historico OHLCV de um ticker no Yahoo Finance.

    Args:
        ticker: simbolo do contrato (ex.: "KC=F").
        start: data inicial "YYYY-MM-DD" (ignorada se `period` for informado).
        end: data final "YYYY-MM-DD" (exclusiva no Yahoo).
        period: janela relativa ("5d", "1mo", "max"); tem prioridade sobre start.
        interval: granularidade ("1d", "1wk", "1mo").
        max_retries: tentativas totais antes de desistir.
        backoff_seconds: base do backoff exponencial entre tentativas.

    Returns:
        DataFrame cru do yfinance (pode vir vazio se o simbolo nao tiver dados).

    Raises:
        FinanceIngestionError: se todas as tentativas falharem por erro tecnico.
    """
    try:
        import yfinance as yf
    except ImportError as exc:  # pragma: no cover
        raise FinanceIngestionError("yfinance nao instalado") from exc

    kwargs: dict[str, object] = {
        "interval": interval,
        "auto_adjust": False,
        "actions": False,
        "raise_errors": False,
    }
    if period:
        kwargs["period"] = period
    else:
        kwargs["start"] = start
        if end:
            kwargs["end"] = end

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            frame = yf.Ticker(ticker).history(**kwargs)
            logger.debug(
                "Download concluido | ticker=%s | tentativa=%d | linhas=%d",
                ticker,
                attempt,
                len(frame),
            )
            return frame
        except Exception as exc:  # rede, rate limit, mudanca de schema, etc.
            last_error = exc
            wait = backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Falha no download | ticker=%s | tentativa=%d/%d | erro=%s: %s",
                ticker,
                attempt,
                max_retries,
                type(exc).__name__,
                exc,
            )
            if attempt < max_retries:
                time.sleep(wait)

    raise FinanceIngestionError(
        f"Nao foi possivel baixar {ticker} apos {max_retries} tentativas: {last_error}"
    )


# -----------------------------------------------------------------------------
# Transformacao minima + metadados de auditoria
# -----------------------------------------------------------------------------
def normalize_history(
    frame: pd.DataFrame,
    ticker: str,
    interval: str,
    run_id: str,
    ingested_at: datetime | None = None,
) -> pd.DataFrame:
    """Padroniza colunas e adiciona as colunas de auditoria/linhagem.

    Regras aplicadas (camada raw = fidelidade + rastreabilidade):
        - indice `Date` -> coluna `date` (tz-naive, em data pura);
        - nomes de colunas em snake_case ("Adj Close" -> "adj_close");
        - colunas de dominio: `ticker`, `commodity`, `exchange`, `currency`;
        - colunas de auditoria: `source`, `interval`, `ingested_at`, `run_id`.

    Args:
        frame: DataFrame cru retornado pelo yfinance.
        ticker: simbolo original.
        interval: granularidade solicitada.
        run_id: identificador da execucao (linhagem).
        ingested_at: timestamp UTC da ingestao (default: agora).

    Returns:
        Novo DataFrame normalizado (o original nao e mutado).
    """
    stamp = ingested_at or datetime.now(tz=UTC)
    normalized = frame.copy()

    normalized = normalized.reset_index()
    normalized.columns = [
        str(col).strip().lower().replace(" ", "_") for col in normalized.columns
    ]

    if "date" not in normalized.columns and "datetime" in normalized.columns:
        normalized = normalized.rename(columns={"datetime": "date"})
    if "date" in normalized.columns:
        dates = pd.to_datetime(normalized["date"], errors="coerce", utc=True)
        normalized["date"] = dates.dt.tz_localize(None).dt.normalize()

    meta = COMMODITY_METADATA.get(ticker.upper(), {})
    normalized["ticker"] = ticker.upper()
    normalized["commodity"] = meta.get("commodity", "unknown")
    normalized["exchange"] = meta.get("exchange", "unknown")
    normalized["currency"] = meta.get("currency", "unknown")
    normalized["interval"] = interval
    normalized["source"] = SOURCE_NAME
    normalized["ingested_at"] = stamp
    normalized["run_id"] = run_id

    return normalized.sort_values("date").reset_index(drop=True)


def to_parquet_bytes(frame: pd.DataFrame) -> io.BytesIO:
    """Serializa o DataFrame em Parquet dentro de um buffer de memoria.

    Args:
        frame: DataFrame ja normalizado.

    Returns:
        BytesIO posicionado no inicio, pronto para envio ao storage.

    Raises:
        FinanceIngestionError: se a serializacao Parquet falhar.
    """
    buffer = io.BytesIO()
    try:
        frame.to_parquet(
            buffer,
            engine="pyarrow",
            compression=PARQUET_COMPRESSION,
            index=False,
        )
    except (ValueError, ImportError, OSError) as exc:
        raise FinanceIngestionError(f"Falha ao serializar Parquet: {exc}") from exc
    buffer.seek(0)
    return buffer


# -----------------------------------------------------------------------------
# Orquestracao da ingestao
# -----------------------------------------------------------------------------
def ingest_ticker(
    ticker: str,
    storage: StorageBackend,
    run_id: str,
    partition_date: date | None = None,
    start: str | None = None,
    end: str | None = None,
    period: str | None = None,
    interval: str = "1d",
    max_retries: int = 3,
    overwrite: bool = True,
    dry_run: bool = False,
) -> IngestionResult:
    """Ingere um ticker: baixa, normaliza, serializa e persiste no storage.

    Nunca lanca excecao: qualquer falha e capturada e devolvida no
    `IngestionResult` para que o batch continue processando os demais tickers.

    Args:
        ticker: simbolo do contrato futuro.
        storage: backend de destino (local ou S3).
        run_id: identificador da execucao.
        partition_date: valor da particao `dt=` (default: hoje em UTC).
        start: data inicial do historico.
        end: data final do historico.
        period: janela relativa (prioritaria sobre `start`).
        interval: granularidade da serie.
        max_retries: tentativas de download.
        overwrite: se False e a chave ja existir, marca status "skipped".
        dry_run: executa tudo, exceto a gravacao no storage.

    Returns:
        IngestionResult com status, contagem de linhas e URI de destino.
    """
    partition = partition_date or datetime.now(tz=UTC).date()
    key = build_object_key(ticker, partition, interval)

    try:
        if not overwrite and storage.exists(key):
            logger.info("Ingestao ignorada (objeto ja existe) | key=%s", key)
            return IngestionResult(
                ticker=ticker, status="skipped", key=key, uri=storage.uri(key)
            )

        raw_frame = fetch_history(
            ticker,
            start=start,
            end=end,
            period=period,
            interval=interval,
            max_retries=max_retries,
        )

        if raw_frame is None or raw_frame.empty:
            meta = COMMODITY_METADATA.get(ticker.upper(), {})
            logger.warning(
                "Sem dados no provedor | ticker=%s | commodity=%s | exchange=%s",
                ticker,
                meta.get("commodity", "unknown"),
                meta.get("exchange", "unknown"),
            )
            return IngestionResult(ticker=ticker, status="no_data")

        frame = normalize_history(raw_frame, ticker, interval, run_id)
        buffer = to_parquet_bytes(frame)
        payload_size = buffer.getbuffer().nbytes

        if dry_run:
            logger.info(
                "[DRY-RUN] Gravacao simulada | key=%s | linhas=%d | bytes=%d",
                key,
                len(frame),
                payload_size,
            )
            return IngestionResult(
                ticker=ticker,
                status="written",
                rows=len(frame),
                key=key,
                uri=storage.uri(key),
                size_bytes=payload_size,
            )

        metadata = {
            "source": SOURCE_NAME,
            "ticker": ticker,
            "interval": interval,
            "rows": str(len(frame)),
            "run_id": run_id,
            "ingested_at": datetime.now(tz=UTC).isoformat(),
            "first_date": str(frame["date"].min().date()),
            "last_date": str(frame["date"].max().date()),
        }
        stored = storage.write_buffer(
            key, buffer, content_type=PARQUET_CONTENT_TYPE, metadata=metadata
        )
        logger.info(
            "Ingestao concluida | ticker=%s | linhas=%d | %s..%s -> %s",
            ticker,
            len(frame),
            frame["date"].min().date(),
            frame["date"].max().date(),
            stored.uri,
        )
        return IngestionResult(
            ticker=ticker,
            status="written",
            rows=len(frame),
            key=stored.key,
            uri=stored.uri,
            size_bytes=stored.size_bytes,
        )

    except (FinanceIngestionError, StorageError) as exc:
        logger.error("Ingestao falhou | ticker=%s | erro=%s", ticker, exc)
        return IngestionResult(ticker=ticker, status="failed", error=str(exc))
    except Exception as exc:  # rede de seguranca do batch
        logger.exception("Erro inesperado na ingestao | ticker=%s", ticker)
        return IngestionResult(
            ticker=ticker, status="failed", error=f"{type(exc).__name__}: {exc}"
        )


def ingest_tickers(
    tickers: Sequence[str] | None = None,
    storage: StorageBackend | None = None,
    settings: Settings | None = None,
    start: str | None = None,
    end: str | None = None,
    period: str | None = None,
    interval: str | None = None,
    partition_date: date | None = None,
    overwrite: bool = True,
    dry_run: bool = False,
) -> IngestionRun:
    """Ingere todos os tickers configurados e devolve o consolidado do batch.

    Args:
        tickers: lista de simbolos; se None usa `FINANCE_TICKERS` +
            `FINANCE_FX_TICKERS` do .env (commodities + cambio BRL=X).
        storage: backend de destino; se None resolve via factory.
        settings: configuracao; se None usa `get_settings()`.
        start: data inicial; se None usa `FINANCE_START_DATE`.
        end: data final.
        period: janela relativa (sobrepoe `start`).
        interval: granularidade; se None usa `FINANCE_INTERVAL`.
        partition_date: valor da particao `dt=`.
        overwrite: sobrescreve objetos existentes.
        dry_run: nao grava no storage.

    Returns:
        IngestionRun com um IngestionResult por ticker.
    """
    cfg = settings or get_settings()
    backend = storage or get_storage(settings=cfg)
    symbols = tuple(tickers) if tickers else cfg.all_finance_tickers
    resolved_interval = interval or cfg.finance_interval
    # Datas relativas ("1y", "6M", "180d") viram data literal antes do yfinance.
    resolved_start = resolve_relative_date(
        start or cfg.finance_start_date
    ) if (start or cfg.finance_start_date) else None
    partition = partition_date or datetime.now(tz=UTC).date()
    run_id = uuid4().hex[:12]

    logger.info(
        "Batch iniciado | run_id=%s | backend=%s | tickers=%s | janela=%s | dt=%s",
        run_id,
        backend.name,
        ",".join(symbols),
        period or f"{resolved_start}..{end or 'hoje'}",
        partition.isoformat(),
    )

    results = tuple(
        ingest_ticker(
            ticker=symbol,
            storage=backend,
            run_id=run_id,
            partition_date=partition,
            start=resolved_start,
            end=end,
            period=period,
            interval=resolved_interval,
            max_retries=cfg.finance_max_retries,
            overwrite=overwrite,
            dry_run=dry_run,
        )
        for symbol in symbols
    )

    run = IngestionRun(run_id=run_id, partition_date=partition, results=results)
    log_run_summary(run)
    return run


def log_run_summary(run: IngestionRun) -> None:
    """Registra o sumario do batch no log estruturado."""
    logger.info("-" * 78)
    logger.info("SUMARIO DA INGESTAO FINANCEIRA | run_id=%s", run.run_id)
    for result in run.results:
        logger.info(
            "  %-6s | %-8s | linhas=%-6d | %s",
            result.ticker,
            result.status,
            result.rows,
            result.uri or result.error or "-",
        )
    logger.info(
        "Total: %d tickers | %d gravados | %d sem dados | %d falhas | %d linhas",
        len(run.results),
        sum(1 for r in run.results if r.status == "written"),
        len(run.empties),
        len(run.failures),
        run.total_rows,
    )
    logger.info("-" * 78)


# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
def build_arg_parser() -> argparse.ArgumentParser:
    """Cria o parser de argumentos da ingestao financeira."""
    parser = argparse.ArgumentParser(
        prog="python -m src.ingestion.finance",
        description=(
            "Ingere cotacoes de commodities agricolas do Yahoo Finance na camada "
            "raw do Data Lake (Parquet particionado por ticker e dt)."
        ),
    )
    parser.add_argument(
        "--tickers",
        default=None,
        help="Lista separada por virgulas (default: FINANCE_TICKERS do .env).",
    )
    parser.add_argument("--start", default=None, help="Data inicial YYYY-MM-DD.")
    parser.add_argument("--end", default=None, help="Data final YYYY-MM-DD.")
    parser.add_argument(
        "--period",
        default=None,
        help="Janela relativa (5d, 1mo, 1y, max). Sobrepoe --start/--end.",
    )
    parser.add_argument(
        "--interval", default=None, help="Granularidade (1d, 1wk, 1mo)."
    )
    parser.add_argument(
        "--partition-date",
        default=None,
        help="Valor da particao dt= (YYYY-MM-DD, default: hoje UTC).",
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
        help="Nao regrava objetos que ja existem na particao.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Executa a extracao sem gravar no storage.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Log em DEBUG.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Ponto de entrada da CLI de ingestao financeira.

    Args:
        argv: argumentos de linha de comando (default: sys.argv[1:]).

    Returns:
        0 = todos os tickers com dados foram gravados;
        1 = houve falha tecnica em pelo menos um ticker;
        2 = execucao ok, porem algum ticker nao possui dados no provedor.
    """
    args = build_arg_parser().parse_args(argv)
    settings = get_settings()
    configure_logging(level="DEBUG" if args.verbose else None, settings=settings)

    try:
        partition = (
            date.fromisoformat(args.partition_date) if args.partition_date else None
        )
        tickers = (
            tuple(t.strip() for t in args.tickers.split(",") if t.strip())
            if args.tickers
            else None
        )
        storage = get_storage(backend=args.backend, settings=settings)
        run = ingest_tickers(
            tickers=tickers,
            storage=storage,
            settings=settings,
            start=args.start,
            end=args.end,
            period=args.period,
            interval=args.interval,
            partition_date=partition,
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
            "Erro inesperado na ingestao: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        return 1

    if run.failures:
        return 1
    if run.empties:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
