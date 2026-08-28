"""Testes da Etapa 2: abstracao de storage e helpers da ingestao financeira.

Nao dependem de rede: usam um DataFrame sintetico e um LocalStorage em tmp_path.

Execucao:
    .\\.venv\\Scripts\\python.exe -m pytest tests -q
"""

from __future__ import annotations

import io
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.ingestion.finance import (
    build_object_key,
    normalize_history,
    sanitize_ticker,
    to_parquet_bytes,
)
from src.storage import LocalStorage, ObjectNotFoundError, StorageError
from src.storage.s3 import S3Storage


@pytest.fixture()
def sample_frame() -> pd.DataFrame:
    """DataFrame no formato bruto devolvido pelo yfinance."""
    index = pd.DatetimeIndex(
        pd.to_datetime(["2026-08-20", "2026-08-21"]).tz_localize("America/New_York"),
        name="Date",
    )
    return pd.DataFrame(
        {
            "Open": [340.0, 341.5],
            "High": [345.0, 343.0],
            "Low": [338.0, 340.0],
            "Close": [344.0, 341.8],
            "Adj Close": [344.0, 341.8],
            "Volume": [1200, 1500],
        },
        index=index,
    )


def test_sanitize_and_key() -> None:
    assert sanitize_ticker("KC=F") == "KC_F"
    assert (
        build_object_key("KC=F", date(2026, 8, 24))
        == "raw/finance/ticker_safe=KC_F/dt=2026-08-24/KC_F_1d.parquet"
    )


def test_normalize_history_adds_audit_columns(sample_frame: pd.DataFrame) -> None:
    frame = normalize_history(sample_frame, "KC=F", "1d", run_id="abc123")
    for column in (
        "date",
        "adj_close",
        "ticker",
        "commodity",
        "exchange",
        "currency",
        "source",
        "ingested_at",
        "run_id",
    ):
        assert column in frame.columns
    assert frame["ticker"].unique().tolist() == ["KC=F"]
    assert frame["commodity"].iloc[0] == "coffee_arabica"
    assert frame["date"].dt.tz is None
    assert frame["date"].is_monotonic_increasing


def test_parquet_roundtrip_in_memory(sample_frame: pd.DataFrame) -> None:
    frame = normalize_history(sample_frame, "ZC=F", "1d", run_id="abc123")
    buffer = to_parquet_bytes(frame)
    assert isinstance(buffer, io.BytesIO) and buffer.getbuffer().nbytes > 0
    reloaded = pd.read_parquet(buffer, engine="pyarrow")
    assert len(reloaded) == len(frame)
    assert reloaded["exchange"].iloc[0] == "CBOT"


def test_local_storage_crud(tmp_path: Path, sample_frame: pd.DataFrame) -> None:
    storage = LocalStorage(root=tmp_path)
    key = build_object_key("ZS=F", date(2026, 8, 24))
    frame = normalize_history(sample_frame, "ZS=F", "1d", run_id="abc123")

    stored = storage.write_buffer(
        key, to_parquet_bytes(frame), metadata={"ticker": "ZS=F"}
    )
    assert stored.size_bytes > 0
    assert storage.exists(key)
    assert pd.read_parquet(io.BytesIO(storage.read_bytes(key))).shape[0] == 2

    listed = storage.list_objects("raw/finance")
    assert [obj.key for obj in listed] == [key]  # sidecar .meta.json e ignorado

    assert storage.delete(key) is True
    assert storage.delete(key) is False
    with pytest.raises(ObjectNotFoundError):
        storage.read_bytes(key)


def test_local_storage_rejects_unsafe_key(tmp_path: Path) -> None:
    storage = LocalStorage(root=tmp_path)
    with pytest.raises(StorageError):
        storage.write_bytes("raw/../../etc/passwd", b"x")


def test_s3_layer_routing_without_credentials() -> None:
    storage = S3Storage(
        bucket_map={"raw": "agro-intel-raw", "processed": "agro-intel-processed"},
        default_bucket="agro-intel-raw",
    )
    assert storage.resolve_target("raw/finance/a.parquet") == (
        "agro-intel-raw",
        "finance/a.parquet",
    )
    assert storage.resolve_target("processed/climate/b.parquet") == (
        "agro-intel-processed",
        "climate/b.parquet",
    )
    assert storage.uri("raw/finance/a.parquet") == "s3://agro-intel-raw/finance/a.parquet"
