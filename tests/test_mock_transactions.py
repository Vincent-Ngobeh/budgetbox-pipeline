"""
Tests for mock transactions ingestion.
"""

import pytest
import pandas as pd
from datetime import datetime, timedelta

from ingestion.mock_transactions import MockTransactionsIngestion


class TestMockTransactionsExtract:
    """Tests for the extract phase."""

    def test_extract_returns_dataframe(self):
        """Extract should return a pandas DataFrame."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=10, days_back=30)
        
        assert isinstance(df, pd.DataFrame)

    def test_extract_correct_row_count(self):
        """Extract should return approximately the requested number of rows."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=50, days_back=30)
        
        # Allow some variance due to random generation
        assert 40 <= len(df) <= 60

    def test_extract_has_required_columns(self):
        """Extract should include all required columns."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=10, days_back=30)
        
        required_columns = [
            'transaction_id',
            'account_id',
            'account_name',
            'account_type',
            'currency',
            'amount',
            'transaction_type',
            'category',
            'merchant',
            'description',
            'transaction_date',
            'transaction_timestamp',
        ]
        
        for col in required_columns:
            assert col in df.columns, f"Missing column: {col}"

    def test_extract_transaction_types_valid(self):
        """Transaction types should only be 'credit' or 'debit'."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=100, days_back=30)
        
        valid_types = {'credit', 'debit'}
        actual_types = set(df['transaction_type'].unique())
        
        assert actual_types.issubset(valid_types)

    def test_extract_amounts_positive(self):
        """All amounts should be positive."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=50, days_back=30)
        
        assert (df['amount'] > 0).all()

    def test_extract_dates_within_range(self):
        """Transaction dates should be within the specified range (with buffer)."""
        days_back = 30
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=50, days_back=days_back)
        
        today = datetime.now().date()
        # Allow 2-day buffer for edge cases
        min_date = today - timedelta(days=days_back + 2)
        
        assert df['transaction_date'].min() >= min_date
        assert df['transaction_date'].max() <= today

    def test_extract_unique_transaction_ids(self):
        """Transaction IDs should be unique."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=100, days_back=30)
        
        assert df['transaction_id'].is_unique


class TestMockTransactionsTransform:
    """Tests for the transform phase."""

    def test_transform_adds_metadata(self):
        """Transform should add _ingested_at and _source columns."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=10, days_back=30)
        df = ingestion.transform(df)
        
        assert '_ingested_at' in df.columns
        assert '_source' in df.columns

    def test_transform_adds_signed_amount(self):
        """Transform should add signed_amount column."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=10, days_back=30)
        df = ingestion.transform(df)
        
        assert 'signed_amount' in df.columns

    def test_transform_signed_amount_negative_for_debits(self):
        """Debits should have negative signed_amount."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=100, days_back=30)
        df = ingestion.transform(df)
        
        debits = df[df['transaction_type'] == 'debit']
        if len(debits) > 0:
            assert (debits['signed_amount'] < 0).all()

    def test_transform_signed_amount_positive_for_credits(self):
        """Credits should have positive signed_amount."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=100, days_back=30)
        df = ingestion.transform(df)
        
        credits = df[df['transaction_type'] == 'credit']
        if len(credits) > 0:
            assert (credits['signed_amount'] > 0).all()

    def test_transform_adds_date_parts(self):
        """Transform should add date part columns."""
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=10, days_back=30)
        df = ingestion.transform(df)
        
        assert 'transaction_year' in df.columns
        assert 'transaction_month' in df.columns
        assert 'transaction_day_of_week' in df.columns


class TestMockTransactionsLoad:
    """Tests for the load phase."""

    def test_load_creates_table(self, mock_env_db_path):
        """Load should create table in DuckDB."""
        import duckdb
        
        ingestion = MockTransactionsIngestion()
        df = ingestion.extract(num_transactions=10, days_back=30)
        df = ingestion.transform(df)
        rows_loaded = ingestion.load(df)
        
        assert rows_loaded == len(df)
        
        # Verify table exists
        conn = duckdb.connect(mock_env_db_path)
        result = conn.execute("SELECT COUNT(*) FROM raw.transactions").fetchone()
        assert result[0] == len(df)
        conn.close()

    def test_load_empty_dataframe(self, mock_env_db_path):
        """Load should handle empty DataFrame gracefully."""
        ingestion = MockTransactionsIngestion()
        df = pd.DataFrame()
        rows_loaded = ingestion.load(df)
        
        assert rows_loaded == 0


class TestMockTransactionsRun:
    """Tests for the full run method."""

    def test_run_returns_stats(self, mock_env_db_path):
        """Run should return statistics dictionary."""
        ingestion = MockTransactionsIngestion()
        result = ingestion.run(num_transactions=10, days_back=30)
        
        assert 'table' in result
        assert 'rows_extracted' in result
        assert 'rows_loaded' in result
        assert 'ingested_at' in result
        assert 'duration_seconds' in result

    def test_run_end_to_end(self, mock_env_db_path):
        """Full pipeline should work end-to-end."""
        import duckdb
        
        ingestion = MockTransactionsIngestion()
        result = ingestion.run(num_transactions=25, days_back=30)
        
        assert result['rows_loaded'] > 0
        
        # Verify data in database
        conn = duckdb.connect(mock_env_db_path)
        count = conn.execute("SELECT COUNT(*) FROM raw.transactions").fetchone()[0]
        assert count == result['rows_loaded']
        conn.close()
