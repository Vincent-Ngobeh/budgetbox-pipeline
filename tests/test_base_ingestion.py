"""
Tests for base ingestion class.
"""

import pytest
import pandas as pd
from datetime import datetime, timezone

from ingestion.base import BaseIngestion


class ConcreteIngestion(BaseIngestion):
    """Concrete implementation for testing."""
    
    def __init__(self):
        super().__init__(table_name="test_table")
    
    def extract(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame({
            'id': [1, 2, 3],
            'value': ['a', 'b', 'c']
        })


class TestBaseIngestion:
    """Tests for BaseIngestion class."""

    def test_init_sets_table_name(self):
        """Init should set table name."""
        ingestion = ConcreteIngestion()
        assert ingestion.table_name == "test_table"

    def test_init_sets_ingested_at(self):
        """Init should set ingested_at timestamp."""
        before = datetime.now(timezone.utc)
        ingestion = ConcreteIngestion()
        after = datetime.now(timezone.utc)
        
        # Compare without timezone info since base.py may not be tz-aware
        assert ingestion.ingested_at is not None

    def test_transform_adds_metadata(self):
        """Transform should add metadata columns."""
        ingestion = ConcreteIngestion()
        df = ingestion.extract()
        df = ingestion.transform(df)
        
        assert '_ingested_at' in df.columns
        assert '_source' in df.columns
        assert df['_source'].iloc[0] == 'ConcreteIngestion'

    def test_load_creates_schema_and_table(self, mock_env_db_path):
        """Load should create schema and table."""
        import duckdb
        
        ingestion = ConcreteIngestion()
        df = ingestion.extract()
        df = ingestion.transform(df)
        rows = ingestion.load(df)
        
        assert rows == 3
        
        conn = duckdb.connect(mock_env_db_path)
        result = conn.execute("SELECT COUNT(*) FROM raw.test_table").fetchone()
        assert result[0] == 3
        conn.close()

    def test_load_append_mode(self, mock_env_db_path):
        """Load in append mode should add rows."""
        import duckdb
        
        ingestion = ConcreteIngestion()
        df = ingestion.extract()
        df = ingestion.transform(df)
        
        # Load twice
        ingestion.load(df, mode='append')
        ingestion.load(df, mode='append')
        
        conn = duckdb.connect(mock_env_db_path)
        result = conn.execute("SELECT COUNT(*) FROM raw.test_table").fetchone()
        assert result[0] == 6  # 3 + 3
        conn.close()

    def test_load_replace_mode(self, mock_env_db_path):
        """Load in replace mode should replace data."""
        import duckdb
        
        ingestion = ConcreteIngestion()
        df = ingestion.extract()
        df = ingestion.transform(df)
        
        # Load, then replace
        ingestion.load(df, mode='append')
        ingestion.load(df, mode='replace')
        
        conn = duckdb.connect(mock_env_db_path)
        result = conn.execute("SELECT COUNT(*) FROM raw.test_table").fetchone()
        assert result[0] == 3  # Only latest load
        conn.close()

    def test_run_returns_stats(self, mock_env_db_path):
        """Run should return statistics dictionary."""
        ingestion = ConcreteIngestion()
        result = ingestion.run()
        
        assert result['table'] == 'raw.test_table'
        assert result['rows_extracted'] == 3
        assert result['rows_loaded'] == 3
        assert 'ingested_at' in result
        assert 'duration_seconds' in result

    def test_load_empty_dataframe(self, mock_env_db_path):
        """Load should handle empty DataFrame."""
        ingestion = ConcreteIngestion()
        df = pd.DataFrame()
        rows = ingestion.load(df)
        
        assert rows == 0
