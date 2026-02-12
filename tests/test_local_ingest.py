"""Tests for local file reading via batched ADBC bulk ingest.

These tests verify that read_csv, read_parquet, and read_json read files
locally using DuckDB and upload them to GizmoSQL via ADBC bulk ingest,
rather than sending file paths to the server.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
import pytest


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample CSV file."""
    path = tmp_path / "sample.csv"
    path.write_text("id,name,value\n1,alice,10.5\n2,bob,20.3\n3,carol,30.1\n")
    return str(path)


@pytest.fixture
def sample_parquet(tmp_path):
    """Create a sample Parquet file."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "value": [10.5, 20.3, 30.1],
        }
    )
    path = tmp_path / "sample.parquet"
    pq.write_table(table, path)
    return str(path)


@pytest.fixture
def sample_json(tmp_path):
    """Create a sample newline-delimited JSON file."""
    path = tmp_path / "sample.json"
    rows = [
        {"id": 1, "name": "alice", "value": 10.5},
        {"id": 2, "name": "bob", "value": 20.3},
        {"id": 3, "name": "carol", "value": 30.1},
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")
    return str(path)


@pytest.fixture
def large_parquet(tmp_path):
    """Create a Parquet file with more rows than the default batch size."""
    n = 25_000  # Larger than default batch_size of 10,000
    table = pa.table(
        {
            "id": list(range(n)),
            "value": [float(i) for i in range(n)],
        }
    )
    path = tmp_path / "large.parquet"
    pq.write_table(table, path)
    return str(path)


def test_read_csv(con, sample_csv):
    """Test reading a local CSV file into GizmoSQL."""
    t = con.read_csv(sample_csv, table_name="test_csv_ingest")
    result = t.execute()
    assert len(result) == 3
    assert list(result.columns) == ["id", "name", "value"]
    assert result["name"].tolist() == ["alice", "bob", "carol"]


def test_read_parquet(con, sample_parquet):
    """Test reading a local Parquet file into GizmoSQL."""
    t = con.read_parquet(sample_parquet, table_name="test_parquet_ingest")
    result = t.execute()
    assert len(result) == 3
    assert list(result.columns) == ["id", "name", "value"]
    assert result["id"].tolist() == [1, 2, 3]


def test_read_json(con, sample_json):
    """Test reading a local JSON file into GizmoSQL."""
    t = con.read_json(sample_json, table_name="test_json_ingest")
    result = t.execute()
    assert len(result) == 3
    assert "name" in result.columns
    assert result["name"].tolist() == ["alice", "bob", "carol"]


def test_read_parquet_batching(con, large_parquet):
    """Test that large files are correctly ingested with batching."""
    t = con.read_parquet(
        large_parquet, table_name="test_batch_ingest", batch_size=5_000
    )
    result = t.execute()
    assert len(result) == 25_000
    # Verify data integrity: first and last values
    assert result["id"].iloc[0] == 0
    assert result["id"].iloc[-1] == 24_999


def test_read_csv_custom_batch_size(con, sample_csv):
    """Test read_csv with a custom batch size."""
    t = con.read_csv(
        sample_csv, table_name="test_csv_batch", batch_size=2
    )
    result = t.execute()
    assert len(result) == 3


def test_read_parquet_auto_table_name(con, sample_parquet):
    """Test that auto-generated table names work."""
    t = con.read_parquet(sample_parquet)
    result = t.execute()
    assert len(result) == 3


def test_read_csv_auto_table_name(con, sample_csv):
    """Test that auto-generated table names work for CSV."""
    t = con.read_csv(sample_csv)
    result = t.execute()
    assert len(result) == 3


def test_read_parquet_multiple_files(con, tmp_path):
    """Test reading multiple Parquet files."""
    for i in range(3):
        table = pa.table({"id": [i * 10 + j for j in range(5)]})
        pq.write_table(table, tmp_path / f"part_{i}.parquet")

    paths = [str(tmp_path / f"part_{i}.parquet") for i in range(3)]
    t = con.read_parquet(paths, table_name="test_multi_parquet")
    result = t.execute()
    assert len(result) == 15


def test_read_csv_ibis_operations(con, sample_csv):
    """Test that ibis operations work on ingested CSV data."""
    t = con.read_csv(sample_csv, table_name="test_csv_ops")
    # Filter
    filtered = t.filter(t.value > 15).execute()
    assert len(filtered) == 2
    # Aggregate
    total = t.value.sum().execute()
    assert abs(total - 60.9) < 0.01


def test_read_parquet_ibis_operations(con, sample_parquet):
    """Test that ibis operations work on ingested Parquet data."""
    t = con.read_parquet(sample_parquet, table_name="test_parquet_ops")
    # Select specific columns
    result = t.select("name", "value").execute()
    assert list(result.columns) == ["name", "value"]
    # Sort
    result = t.order_by("id").execute()
    assert result["id"].tolist() == [1, 2, 3]
