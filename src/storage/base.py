"""Interface abstrata da camada de armazenamento do Data Lake.

Todo o codigo de ingestao/transformacao depende APENAS desta interface, nunca
de `open()` ou de `boto3` diretamente. Isso permite trocar disco local por
AWS S3 alterando somente `STORAGE_BACKEND` no .env.

Convencao de chaves (keys):
    "raw/finance/ticker=KC_F/dt=2026-08-24/KC_F_1d.parquet"

    - Sempre com "/" (mesmo no Windows).
    - O primeiro segmento e a camada do Medallion ("raw" | "processed").
    - No backend local a key e relativa a `DATA_LAKE_ROOT`.
    - No backend S3 a camada mapeia para o bucket e o restante para o prefixo.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, BinaryIO

if TYPE_CHECKING:  # pragma: no cover - apenas para type checkers
    import pandas as pd


class StorageError(RuntimeError):
    """Falha generica na camada de armazenamento."""


class ObjectNotFoundError(StorageError):
    """A chave solicitada nao existe no backend."""


@dataclass(frozen=True, slots=True)
class StorageObject:
    """Metadados de um objeto persistido no Data Lake.

    Attributes:
        key: chave logica dentro do lake (independente do backend).
        uri: localizacao fisica ("file:///..." ou "s3://bucket/key").
        size_bytes: tamanho do objeto em bytes.
        backend: nome do backend que persistiu o objeto.
        last_modified: data da ultima modificacao, quando disponivel.
    """

    key: str
    uri: str
    size_bytes: int
    backend: str
    last_modified: datetime | None = None


class StorageBackend(ABC):
    """Contrato de armazenamento orientado a bytes (padrao Repository)."""

    #: Nome curto do backend, usado em logs e no factory.
    name: str = "abstract"

    # -- Operacoes obrigatorias ------------------------------------------------
    @abstractmethod
    def write_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> StorageObject:
        """Persiste `data` na chave informada, sobrescrevendo se existir.

        Args:
            key: chave logica de destino.
            data: conteudo binario.
            content_type: MIME type (ex.: "application/vnd.apache.parquet").
            metadata: metadados de auditoria (tags do objeto).

        Returns:
            StorageObject descrevendo o objeto gravado.

        Raises:
            StorageError: em qualquer falha de escrita.
        """

    @abstractmethod
    def read_bytes(self, key: str) -> bytes:
        """Le o conteudo binario de uma chave.

        Raises:
            ObjectNotFoundError: se a chave nao existir.
            StorageError: em qualquer outra falha de leitura.
        """

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Indica se a chave existe no backend."""

    @abstractmethod
    def list_objects(self, prefix: str = "") -> list[StorageObject]:
        """Lista objetos sob um prefixo, ordenados por chave."""

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Remove a chave. Retorna False se ela nao existia."""

    @abstractmethod
    def uri(self, key: str) -> str:
        """Retorna a URI fisica correspondente a chave."""

    # -- Utilitarios concretos -------------------------------------------------
    def read_parquet(self, key: str) -> pd.DataFrame:
        """Le um Parquet do lake e devolve um DataFrame (sem tocar o disco).

        Args:
            key: chave logica do objeto.

        Returns:
            DataFrame com o conteudo do Parquet.

        Raises:
            ObjectNotFoundError: se a chave nao existir.
            StorageError: em falha de leitura ou parsing.
        """
        import io

        import pandas as pd_mod

        try:
            payload = self.read_bytes(key)
            return pd_mod.read_parquet(io.BytesIO(payload), engine="pyarrow")
        except pd_mod.errors.ParserError as exc:
            raise StorageError(f"Parquet invalido em {key!r}: {exc}") from exc

    def write_parquet(
        self,
        key: str,
        frame: pd.DataFrame,
        compression: str = "snappy",
        metadata: dict[str, str] | None = None,
    ) -> StorageObject:
        """Serializa um DataFrame em Parquet e grava no lake (memoria).

        Args:
            key: chave logica de destino.
            frame: DataFrame a persistir.
            compression: codec do Parquet (default "snappy").
            metadata: metadados de auditoria do objeto.

        Returns:
            StorageObject descrevendo o objeto gravado.

        Raises:
            StorageError: em falha de serializacao ou escrita.
        """
        import io

        buffer = io.BytesIO()
        try:
            frame.to_parquet(
                buffer, engine="pyarrow", compression=compression, index=False
            )
        except (ValueError, ImportError, OSError) as exc:
            raise StorageError(f"Falha ao serializar Parquet em {key!r}: {exc}") from exc
        return self.write_buffer(
            key,
            buffer,
            content_type="application/vnd.apache.parquet",
            metadata=metadata,
        )

    def write_buffer(
        self,
        key: str,
        buffer: BinaryIO,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> StorageObject:
        """Persiste o conteudo de um buffer em memoria (ex.: BytesIO).

        O buffer e rebobinado antes da leitura, evitando gravacoes vazias.
        """
        try:
            buffer.seek(0)
            payload = buffer.read()
        except (OSError, ValueError) as exc:
            raise StorageError(f"Buffer invalido para a chave {key!r}: {exc}") from exc
        return self.write_bytes(
            key, payload, content_type=content_type, metadata=metadata
        )

    @staticmethod
    def join_key(*parts: str) -> str:
        """Monta uma chave canonica a partir de segmentos.

        Normaliza separadores do Windows e remove barras redundantes.

        Example:
            >>> StorageBackend.join_key("raw/finance", "ticker=KC_F", "a.parquet")
            'raw/finance/ticker=KC_F/a.parquet'
        """
        segments: list[str] = []
        for part in parts:
            if not part:
                continue
            for chunk in str(part).replace("\\", "/").split("/"):
                if chunk:
                    segments.append(chunk)
        return "/".join(segments)

    @staticmethod
    def normalize_key(key: str) -> str:
        """Valida e normaliza uma chave (sem barras iniciais, sem "..").

        Raises:
            StorageError: se a chave for vazia ou tentar escapar da raiz.
        """
        normalized = StorageBackend.join_key(key)
        if not normalized:
            raise StorageError("Chave de storage vazia")
        if ".." in normalized.split("/"):
            raise StorageError(f"Chave de storage insegura: {key!r}")
        return normalized

    def total_size(self, prefix: str = "") -> int:
        """Soma o tamanho (bytes) de todos os objetos sob um prefixo."""
        return sum(obj.size_bytes for obj in self.list_objects(prefix))

    def keys(self, prefix: str = "") -> Iterable[str]:
        """Itera apenas as chaves sob um prefixo."""
        return (obj.key for obj in self.list_objects(prefix))

    def __repr__(self) -> str:  # pragma: no cover - conveniencia de debug
        return f"{type(self).__name__}(backend={self.name!r})"
