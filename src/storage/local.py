"""Implementacao de `StorageBackend` sobre o sistema de arquivos local.

Simula os buckets S3 usando a arvore `data_lake/` criada na Etapa 1. A escrita
e atomica (arquivo temporario + `os.replace`), evitando arquivos Parquet
truncados caso o processo seja interrompido no meio da gravacao.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from src.config import get_logger
from src.storage.base import (
    ObjectNotFoundError,
    StorageBackend,
    StorageError,
    StorageObject,
)

logger = get_logger("storage.local")


class LocalStorage(StorageBackend):
    """Armazenamento local que espelha a estrutura de prefixos do S3.

    Args:
        root: raiz do Data Lake local.
        write_metadata_sidecar: se True, grava um arquivo "<key>.meta.json"
            com os metadados de auditoria (equivale ao Object Metadata do S3).
    """

    name = "local"

    def __init__(self, root: Path | str, write_metadata_sidecar: bool = True) -> None:
        self.root: Path = Path(root).expanduser().resolve()
        self.write_metadata_sidecar = write_metadata_sidecar
        try:
            self.root.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise StorageError(f"Nao foi possivel criar {self.root}: {exc}") from exc
        logger.debug("LocalStorage inicializado em %s", self.root)

    # -- Helpers ---------------------------------------------------------------
    def _path(self, key: str) -> Path:
        """Converte uma chave logica em caminho absoluto seguro."""
        normalized = self.normalize_key(key)
        return self.root.joinpath(*normalized.split("/"))

    @staticmethod
    def _sidecar_path(target: Path) -> Path:
        """Caminho do sidecar de metadados de um objeto.

        O nome recebe o prefixo "_" porque o PyArrow/Spark ignoram arquivos
        iniciados por "_" ou "." ao descobrir um dataset particionado - sem isso
        `pd.read_parquet("data_lake/raw/finance")` tentaria abrir o JSON.
        """
        return target.with_name(f"_{target.name}.meta.json")

    def _to_object(self, key: str, path: Path) -> StorageObject:
        """Constroi o StorageObject a partir do arquivo em disco."""
        stat = path.stat()
        return StorageObject(
            key=key,
            uri=path.as_uri(),
            size_bytes=stat.st_size,
            backend=self.name,
            last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
        )

    # -- Contrato --------------------------------------------------------------
    def write_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> StorageObject:
        """Grava bytes de forma atomica, criando os diretorios necessarios."""
        target = self._path(key)
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                dir=target.parent, prefix=".tmp_", suffix=".part", delete=False
            ) as tmp:
                tmp.write(data)
                tmp.flush()
                os.fsync(tmp.fileno())
                tmp_path = Path(tmp.name)
            os.replace(tmp_path, target)
        except OSError as exc:
            raise StorageError(f"Falha ao gravar {key!r} em {target}: {exc}") from exc

        if metadata and self.write_metadata_sidecar:
            self._write_sidecar(target, key, content_type, metadata)

        stored = self._to_object(self.normalize_key(key), target)
        logger.info(
            "Objeto gravado | backend=%s | key=%s | bytes=%d",
            self.name,
            stored.key,
            stored.size_bytes,
        )
        return stored

    def _write_sidecar(
        self,
        target: Path,
        key: str,
        content_type: str | None,
        metadata: dict[str, str],
    ) -> None:
        """Grava os metadados do objeto num arquivo .meta.json (best effort)."""
        payload = {
            "key": self.normalize_key(key),
            "content_type": content_type,
            "metadata": metadata,
            "written_at": datetime.now(tz=UTC).isoformat(),
        }
        sidecar = self._sidecar_path(target)
        try:
            sidecar.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
            )
        except OSError as exc:  # nao invalida a escrita principal
            logger.warning("Nao foi possivel gravar metadados de %s: %s", key, exc)

    def read_bytes(self, key: str) -> bytes:
        """Le o conteudo binario de um arquivo do lake local."""
        path = self._path(key)
        try:
            return path.read_bytes()
        except FileNotFoundError as exc:
            raise ObjectNotFoundError(f"Chave inexistente: {key!r}") from exc
        except OSError as exc:
            raise StorageError(f"Falha ao ler {key!r}: {exc}") from exc

    def exists(self, key: str) -> bool:
        """Indica se o arquivo existe no lake local."""
        return self._path(key).is_file()

    def list_objects(self, prefix: str = "") -> list[StorageObject]:
        """Lista recursivamente os objetos sob um prefixo (ignora sidecars)."""
        base = self.root if not prefix else self._path(prefix)
        if base.is_file():
            return [self._to_object(self.normalize_key(prefix), base)]
        if not base.is_dir():
            return []

        objects: list[StorageObject] = []
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            if path.name == ".gitkeep" or path.name.endswith(".meta.json"):
                continue
            if path.name.startswith("."):  # restos de escrita atomica (.tmp_*.part)
                continue
            key = path.relative_to(self.root).as_posix()
            objects.append(self._to_object(key, path))
        return objects

    def delete(self, key: str) -> bool:
        """Remove o arquivo e seu sidecar de metadados, se existirem."""
        path = self._path(key)
        try:
            path.unlink()
        except FileNotFoundError:
            logger.debug("Delete ignorado, chave inexistente: %s", key)
            return False
        except OSError as exc:
            raise StorageError(f"Falha ao remover {key!r}: {exc}") from exc

        sidecar = self._sidecar_path(path)
        if sidecar.is_file():
            sidecar.unlink(missing_ok=True)
        logger.info("Objeto removido | backend=%s | key=%s", self.name, key)
        return True

    def uri(self, key: str) -> str:
        """Retorna a URI file:// do objeto."""
        return self._path(key).as_uri()
