"""
Data ingestion modules for BudgetBox Pipeline.

Modules:
    - mock_transactions: Generate realistic UK transaction data
    - exchange_rates: Fetch exchange rates from Frankfurter API (coming soon)

Usage:
    from ingestion.mock_transactions import ingest_mock_transactions
    result = ingest_mock_transactions(num_transactions=100)
"""

__all__ = [
    "mock_transactions",
]
