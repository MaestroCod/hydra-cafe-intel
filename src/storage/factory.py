"""Factory de backends de storage (padrao Factory + Registry).

O resto da aplicacao nunca instancia `LocalStorage`/`S3Storage` diretamente:

    >>> from src.storage import get_storage
    >>> storage = get_storage()          # decide pelo STORAGE_BACKEND do .env
    >>> storage.write_bytes("raw/finance/x.parquet", b"...")

Para migrar para a AWS basta trocar `STORAGE_BACKEND=s3` no .env.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Final

from src.config import Settings, get_logger, get_settings
from src.storage.base import StorageBackend, StorageError
from src.storage.local import LocalStorage
from src.storage.s3 import S3Storage

logger = get_logger("storage.factory")

#: Assinatura de um construtor de backend a partir das configuracoes.
BackendBuilder = Callable[[Settings], StorageBackend]


def _build_local(settings: Settings) -> StorageBackend:
    """Constroi o backend local usando a raiz definida no .env."""
    return LocalStorage(root=settings.lake_root_path)


def _build_s3(settings: Settings) -> StorageBackend:
    """Constroi o backend S3 usando buckets/credenciais do .env."""
    return S3Storage.from_settings(settings)


_REGISTRY: Final[dict[str, BackendBuilder]] = {
    "local": _build_local,
    "s3": _build_s3,
}


def register_backend(name: str, builder: BackendBuilder) -> None:
    """Registra um backend customizado (ex.: MinIO, GCS, Azure Blob).

    Args:
        name: identificador usado em STORAGE_BACKEND.
        builder: callable que recebe Settings e devolve um StorageBackend.
    """
    _REGISTRY[name.strip().lower()] = builder
    logger.debug("Backend de storage registrado: %s", name)


def available_backends() -> tuple[str, ...]:
    """Lista os backends disponiveis no registry."""
    return tuple(sorted(_REGISTRY))


def get_storage(
    backend: str | None = None,
    settings: Settings | None = None,
) -> StorageBackend:
    """Retorna o backend de storage configurado.

    Args:
        backend: forca um backend especifico ("local"/"s3"); se None usa
            `STORAGE_BACKEND` do .env.
        settings: configuracao; se None usa `get_settings()`.

    Returns:
        Instancia concreta de StorageBackend.

    Raises:
        StorageError: se o backend solicitado nao estiver registrado ou se a
            construcao falhar (ex.: bucket ausente no modo s3).
    """
    cfg = settings or get_settings()
    chosen = (backend or cfg.storage_backend).strip().lower()

    builder = _REGISTRY.get(chosen)
    if builder is None:
        raise StorageError(
            f"Backend de storage desconhecido: {chosen!r}. "
            f"Disponiveis: {', '.join(available_backends())}"
        )

    storage = builder(cfg)
    target = storage.root if isinstance(storage, LocalStorage) else storage.uri("raw")
    logger.info("Storage ativo | backend=%s | destino=%s", storage.name, target)
    return storage
