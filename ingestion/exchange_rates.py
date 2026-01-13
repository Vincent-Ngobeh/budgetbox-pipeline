"""
Exchange rates ingestion from Frankfurter API.

Fetches daily exchange rates and loads them into raw.exchange_rates.
API Documentation: https://www.frankfurter.app/docs/
"""

from datetime import datetime, timedelta

import pandas as pd
import requests
from loguru import logger

from ingestion.base import BaseIngestion


class ExchangeRatesIngestion(BaseIngestion):
    """Ingest exchange rates from Frankfurter API."""

    API_BASE_URL = "https://api.frankfurter.app"

    def __init__(self):
        super().__init__(table_name="exchange_rates")

    def extract(
        self,
        base_currency: str = "GBP",
        target_currencies: list[str] = None,
        start_date: str = None,
        end_date: str = None,
    ) -> pd.DataFrame:
        """
        Extract exchange rates from API.

        Args:
            base_currency: Base currency code (default: GBP)
            target_currencies: List of target currencies (default: USD, EUR)
            start_date: Start date YYYY-MM-DD (default: 30 days ago)
            end_date: End date YYYY-MM-DD (default: today)

        Returns:
            DataFrame with exchange rates
        """
        if target_currencies is None:
            target_currencies = ["USD", "EUR"]

        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")

        if start_date is None:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

        # Build API URL
        symbols = ",".join(target_currencies)
        url = f"{self.API_BASE_URL}/{start_date}..{end_date}"
        params = {"from": base_currency, "to": symbols}

        logger.info(f"Fetching exchange rates: {url} with params {params}")

        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch exchange rates: {e}")
            raise

        # Parse response into DataFrame
        records = []
        for date, rates in data.get("rates", {}).items():
            for currency, rate in rates.items():
                records.append({
                    "rate_date": date,
                    "base_currency": base_currency,
                    "target_currency": currency,
                    "rate": rate,
                })

        df = pd.DataFrame(records)

        if not df.empty:
            df["rate_date"] = pd.to_datetime(df["rate_date"])

        logger.info(f"Extracted {len(df)} exchange rate records")
        return df

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add calculated fields and metadata."""
        df = super().transform(df)

        if not df.empty:
            # Add inverse rate for convenience
            df["inverse_rate"] = 1 / df["rate"]

            # Round to 6 decimal places
            df["rate"] = df["rate"].round(6)
            df["inverse_rate"] = df["inverse_rate"].round(6)

        return df


def ingest_exchange_rates(
    base_currency: str = "GBP",
    target_currencies: list[str] = None,
) -> dict:
    """
    Callable function for Airflow.
    """
    ingestion = ExchangeRatesIngestion()
    return ingestion.run(
        base_currency=base_currency,
        target_currencies=target_currencies or ["USD", "EUR"],
    )


if __name__ == "__main__":
    result = ingest_exchange_rates()
    print(result)
