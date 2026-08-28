"""Implementacao de `StorageBackend` sobre o AWS S3 (boto3).

Roteamento camada -> bucket (mesmo mapeamento documentado em
`data_lake/README.md` na Etapa 1):

    "raw/finance/..."        ->  s3://<S3_BUCKET_RAW>/finance/...
    "processed/climate/..."  ->  s3://<S3_BUCKET_PROCESSED>/climate/...

Assim o codigo de ingestao usa sempre a mesma chave logica, independente do
backend ativo. O cliente boto3 e criado de forma preguicosa (lazy), portanto
importar este modulo em ambiente 100% local nao exige credenciais validas.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from src.config import Settings, get_logger, get_settings
from src.storage.base import (
    ObjectNotFoundError,
    StorageBackend,
    StorageError,
    StorageObject,
)

logger = get_logger("storage.s3")

#: Codigos de erro do S3 que representam "objeto/prefixo inexistente".
_NOT_FOUND_CODES = frozenset({"404", "NoSuchKey", "NoSuchBucket", "NotFound"})


class S3Storage(StorageBackend):
    """Armazenamento em AWS S3 com roteamento por camada do Medallion.

    Args:
        bucket_map: mapeamento camada -> bucket (ex.: {"raw": "agro-intel-raw"}).
        default_bucket: bucket usado quando a chave nao comeca por uma camada
            conhecida.
        region_name: regiao AWS.
        aws_access_key_id: chave de acesso (None = cadeia padrao do boto3:
            variaveis de ambiente, ~/.aws/credentials, IAM Role da EC2).
        aws_secret_access_key: segredo de acesso.
        aws_session_token: token temporario (STS/SSO).
        endpoint_url: endpoint alternativo (MinIO/LocalStack).
        server_side_encryption: algoritmo SSE aplicado no put_object.
    """

    name = "s3"

    def __init__(
        self,
        bucket_map: Mapping[str, str],
        default_bucket: str,
        region_name: str = "us-east-1",
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        endpoint_url: str | None = None,
        server_side_encryption: str | None = "AES256",
    ) -> None:
        if not default_bucket:
            raise StorageError("S3Storage exige um bucket padrao configurado")
        self.bucket_map: dict[str, str] = {k: v for k, v in bucket_map.items() if v}
        self.default_bucket = default_bucket
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.server_side_encryption = server_side_encryption
        self._credentials: dict[str, str | None] = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_session_token": aws_session_token,
        }
        self._client: Any | None = None

    # -- Construcao a partir do .env ------------------------------------------
    @classmethod
    def from_settings(cls, settings: Settings | None = None) -> S3Storage:
        """Cria o backend S3 a partir das configuracoes do .env."""
        cfg = settings or get_settings()
        cfg.require("s3_bucket_raw", "aws_region")
        return cls(
            bucket_map=cfg.s3_bucket_map,
            default_bucket=cfg.s3_bucket_raw,
            region_name=cfg.aws_region,
            aws_access_key_id=cfg.aws_access_key_id,
            aws_secret_access_key=cfg.aws_secret_access_key,
            aws_session_token=cfg.aws_session_token,
            endpoint_url=cfg.s3_endpoint_url,
        )

    # -- Cliente boto3 (lazy) --------------------------------------------------
    @property
    def client(self) -> Any:
        """Cliente boto3 S3, criado sob demanda e reutilizado.

        Raises:
            StorageError: se o boto3 nao estiver instalado ou a sessao falhar.
        """
        if self._client is not None:
            return self._client
        try:
            import boto3
            from botocore.exceptions import BotoCoreError
        except ImportError as exc:  # pragma: no cover
            raise StorageError(
                "boto3 nao instalado: execute 'pip install -r requirements.txt'"
            ) from exc

        try:
            session = boto3.session.Session(
                region_name=self.region_name,
                **{k: v for k, v in self._credentials.items() if v},
            )
            self._client = session.client("s3", endpoint_url=self.endpoint_url)
        except BotoCoreError as exc:
            raise StorageError(f"Falha ao criar cliente S3: {exc}") from exc

        logger.debug(
            "Cliente S3 criado | region=%s | endpoint=%s",
            self.region_name,
            self.endpoint_url or "default",
        )
        return self._client

    # -- Roteamento camada -> bucket ------------------------------------------
    def resolve_target(self, key: str) -> tuple[str, str]:
        """Traduz uma chave logica em (bucket, chave S3).

        Example:
            >>> storage.resolve_target("raw/finance/a.parquet")
            ('agro-intel-raw', 'finance/a.parquet')
        """
        normalized = self.normalize_key(key)
        layer, _, remainder = normalized.partition("/")
        if layer in self.bucket_map and remainder:
            return self.bucket_map[layer], remainder
        return self.default_bucket, normalized

    @staticmethod
    def _error_code(exc: Exception) -> str:
        """Extrai o codigo de erro de um ClientError do botocore."""
        response = getattr(exc, "response", {}) or {}
        error = response.get("Error", {}) if isinstance(response, dict) else {}
        return str(error.get("Code", ""))

    # -- Contrato --------------------------------------------------------------
    def write_bytes(
        self,
        key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
    ) -> StorageObject:
        """Envia bytes ao S3 via `put_object` (sem tocar o disco local)."""
        from botocore.exceptions import BotoCoreError, ClientError

        bucket, s3_key = self.resolve_target(key)
        params: dict[str, Any] = {"Bucket": bucket, "Key": s3_key, "Body": data}
        if content_type:
            params["ContentType"] = content_type
        if metadata:
            params["Metadata"] = {k: str(v) for k, v in metadata.items()}
        if self.server_side_encryption:
            params["ServerSideEncryption"] = self.server_side_encryption

        try:
            self.client.put_object(**params)
        except (ClientError, BotoCoreError) as exc:
            raise StorageError(
                f"Falha ao gravar s3://{bucket}/{s3_key}: {exc}"
            ) from exc

        stored = StorageObject(
            key=self.normalize_key(key),
            uri=f"s3://{bucket}/{s3_key}",
            size_bytes=len(data),
            backend=self.name,
        )
        logger.info(
            "Objeto gravado | backend=%s | uri=%s | bytes=%d",
            self.name,
            stored.uri,
            stored.size_bytes,
        )
        return stored

    def read_bytes(self, key: str) -> bytes:
        """Le um objeto do S3 inteiramente em memoria."""
        from botocore.exceptions import BotoCoreError, ClientError

        bucket, s3_key = self.resolve_target(key)
        try:
            response = self.client.get_object(Bucket=bucket, Key=s3_key)
            body: bytes = response["Body"].read()
            return body
        except ClientError as exc:
            if self._error_code(exc) in _NOT_FOUND_CODES:
                raise ObjectNotFoundError(
                    f"Chave inexistente: s3://{bucket}/{s3_key}"
                ) from exc
            raise StorageError(f"Falha ao ler s3://{bucket}/{s3_key}: {exc}") from exc
        except BotoCoreError as exc:
            raise StorageError(f"Falha ao ler s3://{bucket}/{s3_key}: {exc}") from exc

    def exists(self, key: str) -> bool:
        """Verifica a existencia do objeto via `head_object`."""
        from botocore.exceptions import BotoCoreError, ClientError

        bucket, s3_key = self.resolve_target(key)
        try:
            self.client.head_object(Bucket=bucket, Key=s3_key)
            return True
        except ClientError as exc:
            if self._error_code(exc) in _NOT_FOUND_CODES:
                return False
            raise StorageError(
                f"Falha no head_object s3://{bucket}/{s3_key}: {exc}"
            ) from exc
        except BotoCoreError as exc:
            raise StorageError(
                f"Falha no head_object s3://{bucket}/{s3_key}: {exc}"
            ) from exc

    def list_objects(self, prefix: str = "") -> list[StorageObject]:
        """Lista objetos sob um prefixo usando paginacao do boto3."""
        from botocore.exceptions import BotoCoreError, ClientError

        if prefix:
            bucket, s3_prefix = self.resolve_target(prefix)
            layer = self.normalize_key(prefix).partition("/")[0]
        else:
            bucket, s3_prefix, layer = self.default_bucket, "", ""

        objects: list[StorageObject] = []
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
                for item in page.get("Contents", []):
                    s3_key = str(item["Key"])
                    logical_key = self.join_key(layer, s3_key) if layer else s3_key
                    objects.append(
                        StorageObject(
                            key=logical_key,
                            uri=f"s3://{bucket}/{s3_key}",
                            size_bytes=int(item.get("Size", 0)),
                            backend=self.name,
                            last_modified=item.get("LastModified"),
                        )
                    )
        except (ClientError, BotoCoreError) as exc:
            raise StorageError(
                f"Falha ao listar s3://{bucket}/{s3_prefix}: {exc}"
            ) from exc
        return sorted(objects, key=lambda obj: obj.key)

    def delete(self, key: str) -> bool:
        """Remove um objeto do S3 (retorna False se ele nao existia)."""
        from botocore.exceptions import BotoCoreError, ClientError

        bucket, s3_key = self.resolve_target(key)
        if not self.exists(key):
            return False
        try:
            self.client.delete_object(Bucket=bucket, Key=s3_key)
        except (ClientError, BotoCoreError) as exc:
            raise StorageError(
                f"Falha ao remover s3://{bucket}/{s3_key}: {exc}"
            ) from exc
        logger.info("Objeto removido | backend=%s | uri=s3://%s/%s", self.name, bucket, s3_key)
        return True

    def uri(self, key: str) -> str:
        """Retorna a URI s3:// correspondente a chave logica."""
        bucket, s3_key = self.resolve_target(key)
        return f"s3://{bucket}/{s3_key}"
