"""Camada de abstracao de armazenamento do Data Lake.

Exporta a interface, as implementacoes concretas e a factory:

    >>> from src.storage import get_storage, StorageBackend
"""

from __future__ import annotations

from src.storage.base import (
    ObjectNotFoundError,
    StorageBackend,
    StorageError,
    StorageObject,
)
from src.storage.factory import available_backends, get_storage, register_backend
from src.storage.local import LocalStorage
from src.storage.s3 import S3Storage

__all__ = [
    "LocalStorage",
    "ObjectNotFoundError",
    "S3Storage",
    "StorageBackend",
    "StorageError",
    "StorageObject",
    "available_backends",
    "get_storage",
    "register_backend",
]
