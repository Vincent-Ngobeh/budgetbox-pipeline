"""
Base ingestion module with common utilities.

Provides abstract base class for all data ingestion tasks with:
- DuckDB connection management
- Standard ETL interface (extract, transform, load)
- Metadata tracking (_ingested_at, _source)
"""

import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import duckdb
import pandas as pd
from loguru import logger


class BaseIngestion(ABC):
    """
    Base class for all data ingestion tasks.

    Subclasses must implement the extract() method.
    Transform and load methods can be overridden as needed.

    Usage:
        class MyIngestion(BaseIngestion):
            def __init__(self):
                super().__init__(table_name="my_table")

            def extract(self, **kwargs) -> pd.DataFrame:
                # Fetch data from source
                return df

        ingestion = MyIngestion()
        result = ingestion.run()
    """

    def __init__(self, table_name: str, schema: str = "raw"):
        """
        Initialise ingestion task.

        Args:
            table_name: Target table name in DuckDB
            schema: Target schema (default: raw)
        """
        self.table_name = table_name
        self.schema = schema
        self.duckdb_path = os.getenv("DUCKDB_PATH", "./data/budgetbox.duckdb")
        self.ingested_at = datetime.utcnow()

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """
        Get DuckDB connection, creating data directory if needed.

        Returns:
            DuckDB connection object
        """
        data_dir = os.path.dirname(self.duckdb_path)
        if data_dir:
            os.makedirs(data_dir, exist_ok=True)
        return duckdb.connect(self.duckdb_path)

    @property
    def full_table_name(self) -> str:
        """Return fully qualified table name (schema.table)."""
        return f"{self.schema}.{self.table_name}"

    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Extract data from source.

        Must be implemented by subclasses.

        Returns:
            DataFrame containing extracted data
        """
        pass

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply transformations to extracted data.

        Default implementation adds metadata columns.
        Override in subclasses for custom transformations.

        Args:
            df: Extracted DataFrame

        Returns:
            Transformed DataFrame with metadata columns
        """
        if df.empty:
            return df

        df = df.copy()
        df["_ingested_at"] = self.ingested_at
        df["_source"] = self.__class__.__name__
        return df

    def load(self, df: pd.DataFrame, mode: str = "append") -> int:
        """
        Load data into DuckDB.

        Args:
            df: DataFrame to load
            mode: 'append' to add rows, 'replace' to overwrite table

        Returns:
            Number of rows loaded
        """
        if df.empty:
            logger.warning(f"No data to load into {self.full_table_name}")
            return 0

        conn = self.get_connection()

        try:
            # Create schema if needed
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")

            if mode == "replace":
                conn.execute(f"DROP TABLE IF EXISTS {self.full_table_name}")

            # Create or append to table
            if mode == "replace" or not self._table_exists(conn):
                conn.execute(
                    f"CREATE TABLE {self.full_table_name} AS SELECT * FROM df"
                )
            else:
                conn.execute(
                    f"INSERT INTO {self.full_table_name} SELECT * FROM df"
                )

            logger.info(f"Loaded {len(df)} rows into {self.full_table_name}")
            return len(df)

        finally:
            conn.close()

    def _table_exists(self, conn: duckdb.DuckDBPyConnection) -> bool:
        """Check if target table exists in DuckDB."""
        try:
            conn.execute(f"SELECT 1 FROM {self.full_table_name} LIMIT 1")
            return True
        except duckdb.CatalogException:
            return False

    def get_row_count(self) -> int:
        """Get current row count in target table."""
        conn = self.get_connection()
        try:
            if not self._table_exists(conn):
                return 0
            result = conn.execute(
                f"SELECT COUNT(*) FROM {self.full_table_name}"
            ).fetchone()
            return result[0] if result else 0
        finally:
            conn.close()

    def run(self, mode: str = "append", **kwargs) -> dict[str, Any]:
        """
        Execute the full ETL process.

        Args:
            mode: Load mode ('append' or 'replace')
            **kwargs: Arguments passed to extract()

        Returns:
            Dictionary with run statistics
        """
        logger.info(f"Starting ingestion for {self.full_table_name}")
        start_time = datetime.utcnow()

        # Extract
        df = self.extract(**kwargs)
        rows_extracted = len(df)
        logger.info(f"Extracted {rows_extracted} rows")

        # Transform
        df = self.transform(df)

        # Load
        rows_loaded = self.load(df, mode=mode)

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        return {
            "table": self.full_table_name,
            "rows_extracted": rows_extracted,
            "rows_loaded": rows_loaded,
            "ingested_at": self.ingested_at.isoformat(),
            "duration_seconds": duration,
        }
