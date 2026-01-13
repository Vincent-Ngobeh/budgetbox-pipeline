"""
Pytest fixtures for BudgetBox Pipeline tests.
"""

import os
import tempfile
from pathlib import Path

import pytest
import duckdb


@pytest.fixture
def temp_db_path():
    """Create a temporary DuckDB database path (file doesn't exist yet)."""
    # Create temp directory
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test.duckdb")
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.remove(db_path)
    wal_path = f"{db_path}.wal"
    if os.path.exists(wal_path):
        os.remove(wal_path)
    if os.path.exists(temp_dir):
        os.rmdir(temp_dir)


@pytest.fixture
def mock_env_db_path(temp_db_path, monkeypatch):
    """Set DUCKDB_PATH environment variable to temp database."""
    monkeypatch.setenv("DUCKDB_PATH", temp_db_path)
    return temp_db_path


@pytest.fixture
def duckdb_connection(temp_db_path):
    """Provide a DuckDB connection to temp database."""
    conn = duckdb.connect(temp_db_path)
    yield conn
    conn.close()
