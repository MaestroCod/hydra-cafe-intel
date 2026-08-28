#!/usr/bin/env python
"""ETAPA 5 - Backfill de 1 ano do escopo Cafe Arabica (tipo 4/5).

Executa o fluxo completo com historico de 1 ano e granularidade semanal:

    1. Financeiro : KC=F, ICF=F, BRL=X desde `FINANCE_START_DATE` (default "1y").
    2. CHIRPS     : precipitacao diaria dos ultimos `CHIRPS_LOOKBACK_DAYS` dias.
    3. ERA5-Land  : reanalise mensal dos ultimos `ERA5_LOOKBACK_DAYS` dias
                    (12 requisicoes ao CDS, uma por mes).
    4. Pipeline   : raw -> processed (balanco hidrico + cotacoes BRL/saca).
    5. Gold       : camada semanal (52 semanas) -> banco relacional.

Uso:
    python scripts/backfill_1y.py                    # 1 ano completo
    python scripts/backfill_1y.py --dry-run          # mostra o plano sem executar
    python scripts/backfill_1y.py --finance-only     # so o eixo financeiro
    python scripts/backfill_1y.py --skip-era5        # pula o ERA5 (licenca pendente)
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path

# Garante que o pacote `src` seja importavel ao rodar como script.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.analytics.gold import build_gold_weekly_analytics
from src.config import Settings, configure_logging, get_logger, get_settings
from src.database.sync import sync_weekly_gold_to_db
from src.ingestion import chirps as chirps_mod
from src.ingestion import era5 as era5_mod
from src.ingestion.chirps import iter_dates
from src.ingestion.era5 import default_target_date as era5_default_date
from src.ingestion.finance import ingest_tickers
from src.processing.pipeline import run_pipeline
from src.storage import get_storage

logger = get_logger("scripts.backfill")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parser de argumentos do backfill."""
    parser = argparse.ArgumentParser(
        prog="python scripts/backfill_1y.py",
        description="Backfill de 1 ano do escopo Cafe Arabica (tipo 4/5) com "
        "granularidade semanal.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Historico em dias (default: 365).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Mostra o plano e o total de datas/meses sem executar.",
    )
    parser.add_argument(
        "--finance-only", action="store_true", help="Executa apenas o eixo financeiro."
    )
    parser.add_argument(
        "--skip-era5", action="store_true", help="Pula o ERA5-Land (licenca pendente)."
    )
    parser.add_argument(
        "--no-overwrite", action="store_true", help="Nao regrava objetos existentes."
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Log em DEBUG.")
    return parser.parse_args(argv)


def build_plan(cfg: Settings, days: int) -> dict[str, str]:
    """Descreve a janela do backfill (datas e meses) sem executar nada."""
    fim_chirps = chirps_mod.default_target_date(cfg)
    inicio_chirps = fim_chirps - timedelta(days=days - 1)
    fim_era5 = era5_default_date(cfg)
    inicio_era5 = fim_era5 - timedelta(days=days - 1)
    meses = sum(1 for _ in era5_mod.iter_months(inicio_era5, fim_era5))
    return {
        "financeiro": f"{cfg.finance_start_date} (tickers={cfg.all_finance_tickers})",
        "chirps": f"{inicio_chirps.isoformat()}..{fim_chirps.isoformat()} "
        f"({days} dias)",
        "era5_mensal": f"{inicio_era5.isoformat()}..{fim_era5.isoformat()} "
        f"({meses} requisicoes ao CDS)",
    }


def main(argv: Sequence[str] | None = None) -> int:
    """Executa o backfill de 1 ano ponta a ponta."""
    args = parse_args(argv)
    cfg: Settings = get_settings()
    configure_logging(
        level="DEBUG" if args.verbose else None,
        log_file="backfill.log",
        settings=cfg,
    )
    dias = args.days or min(cfg.chirps_lookback_days, cfg.era5_lookback_days, 365)

    plano = build_plan(cfg, dias)
    logger.info("PLANO DO BACKFILL 1 ANO | %s", plano)
    if args.dry_run:
        logger.info("DRY-RUN: nenhuma chamada externa sera executada")
        return 0

    storage = get_storage(settings=cfg)

    # 1) Financeiro (cafe + cambio), 1 ano para tras.
    run_fin = ingest_tickers(
        storage=storage,
        settings=cfg,
        start=cfg.finance_start_date,
        overwrite=not args.no_overwrite,
    )
    logger.info(
        "Financeiro OK | gravados=%d | falhas=%d",
        sum(1 for r in run_fin.results if r.status == "written"),
        len(run_fin.failures),
    )
    if args.finance_only:
        return 0

    # 2) CHIRPS diario (365 dias).
    fim_chirps = chirps_mod.default_target_date(cfg)
    datas_chirps = tuple(
        iter_dates(fim_chirps - timedelta(days=dias - 1), fim_chirps)
    )
    run_chirps = chirps_mod.ingest_dates(
        dates=datas_chirps,
        storage=storage,
        settings=cfg,
        overwrite=not args.no_overwrite,
    )
    logger.info(
        "CHIRPS OK | gravados=%d | indisponiveis=%d",
        sum(1 for r in run_chirps.results if r.status == "written"),
        len(run_chirps.unavailable),
    )

    # 3) ERA5-Land mensal (12 requisicoes ao CDS) - opcional.
    if not args.skip_era5:
        run_era5 = era5_mod.ingest_backfill(
            lookback_days=dias,
            storage=storage,
            settings=cfg,
            overwrite=not args.no_overwrite,
        )
        logger.info(
            "ERA5 OK | gravados=%d | falhas=%d",
            sum(1 for r in run_era5.results if r.status == "written"),
            len(run_era5.failures),
        )

    # 4) Pipeline raw -> processed (balanco hidrico + cotacoes BRL/saca).
    resultado = run_pipeline(storage=storage, settings=cfg)
    logger.info(
        "PIPELINE OK | climate_keys=%d | finance_keys=%d | finance_rows=%d",
        len(resultado.climate_keys),
        len(resultado.finance_keys),
        resultado.finance_rows,
    )

    # 5) Gold semanal + persistencia relacional.
    gold = build_gold_weekly_analytics(storage=storage, write_output=True)
    logger.info(
        "GOLD SEMANAL OK | semanas=%d | alertas=%d",
        gold["data_semana"].nunique() if not gold.empty else 0,
        int(gold["alerta_estresse"].sum()) if not gold.empty else 0,
    )
    try:
        total = sync_weekly_gold_to_db(storage=storage, settings=cfg)
        logger.info("SYNC DB OK | linhas=%d", total)
    except RuntimeError as exc:
        logger.warning("Sync DB nao executado: %s", exc)

    logger.info("BACKFILL 1 ANO CONCLUIDO | %s", datetime.now(tz=UTC).isoformat())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
