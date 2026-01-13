"""
Tests for exchange rates ingestion.
"""

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

from ingestion.exchange_rates import ExchangeRatesIngestion


class TestExchangeRatesExtract:
    """Tests for the extract phase."""

    @patch('ingestion.exchange_rates.requests.get')
    def test_extract_parses_api_response(self, mock_get):
        """Extract should parse API response into DataFrame."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "base": "GBP",
            "rates": {
                "2024-01-01": {"USD": 1.27, "EUR": 1.15},
                "2024-01-02": {"USD": 1.28, "EUR": 1.16},
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        ingestion = ExchangeRatesIngestion()
        df = ingestion.extract(
            base_currency="GBP",
            target_currencies=["USD", "EUR"],
            start_date="2024-01-01",
            end_date="2024-01-02"
        )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4  # 2 dates × 2 currencies

    @patch('ingestion.exchange_rates.requests.get')
    def test_extract_has_required_columns(self, mock_get):
        """Extract should include all required columns."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "rates": {
                "2024-01-01": {"USD": 1.27},
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        ingestion = ExchangeRatesIngestion()
        df = ingestion.extract(
            base_currency="GBP",
            target_currencies=["USD"],
            start_date="2024-01-01",
            end_date="2024-01-01"
        )

        required_columns = ['rate_date', 'base_currency', 'target_currency', 'rate']
        for col in required_columns:
            assert col in df.columns, f"Missing column: {col}"

    @patch('ingestion.exchange_rates.requests.get')
    def test_extract_handles_empty_response(self, mock_get):
        """Extract should handle empty API response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"rates": {}}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        ingestion = ExchangeRatesIngestion()
        df = ingestion.extract(
            base_currency="GBP",
            target_currencies=["USD"],
            start_date="2024-01-01",
            end_date="2024-01-01"
        )

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @patch('ingestion.exchange_rates.requests.get')
    def test_extract_api_error_raises(self, mock_get):
        """Extract should raise on API error."""
        import requests
        
        mock_get.side_effect = requests.RequestException("API Error")

        ingestion = ExchangeRatesIngestion()
        
        with pytest.raises(requests.RequestException):
            ingestion.extract(
                base_currency="GBP",
                target_currencies=["USD"],
                start_date="2024-01-01",
                end_date="2024-01-01"
            )

    @patch('ingestion.exchange_rates.requests.get')
    def test_extract_builds_correct_url(self, mock_get):
        """Extract should build correct API URL."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"rates": {}}
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        ingestion = ExchangeRatesIngestion()
        ingestion.extract(
            base_currency="GBP",
            target_currencies=["USD", "EUR"],
            start_date="2024-01-01",
            end_date="2024-01-31"
        )

        mock_get.assert_called_once()
        call_args = mock_get.call_args
        
        assert "2024-01-01..2024-01-31" in call_args[0][0]
        assert call_args[1]['params']['from'] == 'GBP'
        assert call_args[1]['params']['to'] == 'USD,EUR'


class TestExchangeRatesTransform:
    """Tests for the transform phase."""

    @patch('ingestion.exchange_rates.requests.get')
    def test_transform_adds_inverse_rate(self, mock_get):
        """Transform should add inverse rate column."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "rates": {"2024-01-01": {"USD": 1.25}}
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        ingestion = ExchangeRatesIngestion()
        df = ingestion.extract(
            base_currency="GBP",
            target_currencies=["USD"],
            start_date="2024-01-01",
            end_date="2024-01-01"
        )
        df = ingestion.transform(df)

        assert 'inverse_rate' in df.columns
        assert df['inverse_rate'].iloc[0] == pytest.approx(0.8, rel=0.01)

    @patch('ingestion.exchange_rates.requests.get')
    def test_transform_adds_metadata(self, mock_get):
        """Transform should add metadata columns."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "rates": {"2024-01-01": {"USD": 1.25}}
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        ingestion = ExchangeRatesIngestion()
        df = ingestion.extract(
            base_currency="GBP",
            target_currencies=["USD"],
            start_date="2024-01-01",
            end_date="2024-01-01"
        )
        df = ingestion.transform(df)

        assert '_ingested_at' in df.columns
        assert '_source' in df.columns

    @patch('ingestion.exchange_rates.requests.get')
    def test_transform_rounds_rates(self, mock_get):
        """Transform should round rates to 6 decimal places."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "rates": {"2024-01-01": {"USD": 1.123456789}}
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        ingestion = ExchangeRatesIngestion()
        df = ingestion.extract(
            base_currency="GBP",
            target_currencies=["USD"],
            start_date="2024-01-01",
            end_date="2024-01-01"
        )
        df = ingestion.transform(df)

        # Check rate is rounded
        rate_str = f"{df['rate'].iloc[0]:.10f}"
        assert len(rate_str.split('.')[1].rstrip('0')) <= 6


class TestExchangeRatesLoad:
    """Tests for the load phase."""

    @patch('ingestion.exchange_rates.requests.get')
    def test_load_creates_table(self, mock_get, mock_env_db_path):
        """Load should create table in DuckDB."""
        import duckdb
        
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "rates": {
                "2024-01-01": {"USD": 1.27, "EUR": 1.15},
            }
        }
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        ingestion = ExchangeRatesIngestion()
        df = ingestion.extract(
            base_currency="GBP",
            target_currencies=["USD", "EUR"],
            start_date="2024-01-01",
            end_date="2024-01-01"
        )
        df = ingestion.transform(df)
        rows_loaded = ingestion.load(df)

        assert rows_loaded == 2

        # Verify table exists
        conn = duckdb.connect(mock_env_db_path)
        result = conn.execute("SELECT COUNT(*) FROM raw.exchange_rates").fetchone()
        assert result[0] == 2
        conn.close()


class TestExchangeRatesIntegration:
    """Integration tests (calls real API)."""

    @pytest.mark.integration
    def test_real_api_call(self):
        """Test real API call (skip in CI)."""
        ingestion = ExchangeRatesIngestion()
        df = ingestion.extract(
            base_currency="GBP",
            target_currencies=["USD"],
            start_date="2024-01-01",
            end_date="2024-01-07"
        )

        assert len(df) > 0
        assert 'rate' in df.columns
        assert (df['rate'] > 0).all()
