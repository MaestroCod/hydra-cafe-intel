"""Utilitario de retentativas com backoff exponencial, compartilhado pelos
ingestores (CHIRPS via HTTP e ERA5-Land via cdsapi).

Centralizar essa logica evita duplicar `for attempt in range(...)` em cada
modulo e garante o mesmo padrao de log estruturado em toda a plataforma.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from logging import Logger

from src.config import get_logger

_default_logger = get_logger("ingestion.retry")


def retry_call[T](
    func: Callable[[], T],
    *,
    description: str,
    attempts: int = 3,
    backoff_seconds: float = 2.0,
    max_backoff_seconds: float = 60.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
    giveup: Callable[[Exception], bool] | None = None,
    logger: Logger | None = None,
) -> T:
    """Executa `func` com retentativas e backoff exponencial.

    Args:
        func: callable sem argumentos (use `functools.partial` ou lambda).
        description: descricao da operacao, usada no log.
        attempts: numero total de tentativas (>= 1).
        backoff_seconds: base do backoff; a espera e `base * 2**(tentativa-1)`.
        max_backoff_seconds: teto da espera entre tentativas.
        exceptions: excecoes consideradas transitorias (passiveis de retry).
        giveup: predicado que, se retornar True para a excecao, aborta na hora
            (ex.: HTTP 404 nao deve ser repetido).
        logger: logger a usar; default = logger do modulo.

    Returns:
        O valor retornado por `func`.

    Raises:
        Exception: a ultima excecao capturada, apos esgotar as tentativas.
        ValueError: se `attempts` for menor que 1.
    """
    if attempts < 1:
        raise ValueError("attempts deve ser >= 1")

    log = logger or _default_logger
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return func()
        except exceptions as exc:
            last_error = exc
            if giveup is not None and giveup(exc):
                log.error(
                    "Erro nao recuperavel | operacao=%s | erro=%s: %s",
                    description,
                    type(exc).__name__,
                    exc,
                )
                raise
            wait = min(backoff_seconds * (2 ** (attempt - 1)), max_backoff_seconds)
            log.warning(
                "Falha transitoria | operacao=%s | tentativa=%d/%d | erro=%s: %s"
                "%s",
                description,
                attempt,
                attempts,
                type(exc).__name__,
                exc,
                f" | aguardando {wait:.1f}s" if attempt < attempts else "",
            )
            if attempt < attempts:
                time.sleep(wait)

    if last_error is None:  # pragma: no cover - inalcancavel
        raise RuntimeError(f"retry_call falhou sem excecao registrada: {description}")
    raise last_error
