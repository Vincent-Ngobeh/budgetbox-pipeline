"""
Kafka consumer for real-time transaction processing.

Consumes transaction events from Kafka and writes them to DuckDB
in batches for efficiency.
"""

import json
import os
import argparse
from datetime import datetime, timezone
from typing import Optional

import duckdb
import pandas as pd
from kafka import KafkaConsumer
from loguru import logger


class TransactionConsumer:
    """Consume transactions from Kafka and write to DuckDB."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions",
        group_id: str = "budgetbox-consumer",
        batch_size: int = 10,
        duckdb_path: Optional[str] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.batch_size = batch_size
        self.duckdb_path = duckdb_path or os.getenv(
            "DUCKDB_PATH", "data/budgetbox.duckdb"
        )
        self.consumer: Optional[KafkaConsumer] = None
        self.batch: list[dict] = []

    def connect(self) -> None:
        """Connect to Kafka."""
        logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")

        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )

        logger.info(f"Connected to Kafka, subscribed to '{self.topic}'")

    def transform_message(self, message: dict) -> dict:
        """Transform Kafka message to match raw.transactions schema."""
        now = datetime.now(timezone.utc)
        transaction_date = datetime.fromisoformat(
            message["transaction_timestamp"].replace("Z", "+00:00")
        ).date()

        # Map category to category_type
        category = message.get("category", "other")
        category_type = "income" if category == "income" else "expense"

        return {
            "transaction_id": message["transaction_id"],
            "account_id": message["account_id"],
            "account_name": message.get("account_name", "Streaming Account"),
            "account_type": message.get("account_type", "current"),
            "bank_name": message.get("bank_name", "Digital Bank"),
            "currency": message.get("currency", "GBP"),
            "amount": abs(message["amount"]),
            "transaction_type": message.get("transaction_type", "debit"),
            "category": category,
            "category_type": category_type,
            "merchant": message.get("merchant", "Unknown"),
            "description": message.get("description", ""),
            "transaction_date": transaction_date,
            "transaction_timestamp": datetime.fromisoformat(
                message["transaction_timestamp"].replace("Z", "+00:00")
            ),
            "_ingested_at": now,
            "_source": "KafkaConsumer",
            "signed_amount": -abs(message["amount"]) if message.get("transaction_type") == "debit" else abs(message["amount"]),
            "transaction_year": transaction_date.year,
            "transaction_month": transaction_date.month,
            "transaction_day_of_week": transaction_date.weekday(),
        }

    def write_batch(self) -> None:
        """Write accumulated batch to DuckDB."""
        if not self.batch:
            return

        df = pd.DataFrame(self.batch)

        # Ensure column order matches table schema
        column_order = [
            "transaction_id",
            "account_id",
            "account_name",
            "account_type",
            "bank_name",
            "currency",
            "amount",
            "transaction_type",
            "category",
            "category_type",
            "merchant",
            "description",
            "transaction_date",
            "transaction_timestamp",
            "_ingested_at",
            "_source",
            "signed_amount",
            "transaction_year",
            "transaction_month",
            "transaction_day_of_week",
        ]
        df = df[column_order]

        conn = duckdb.connect(self.duckdb_path)
        try:
            conn.execute("INSERT INTO raw.transactions SELECT * FROM df")
            logger.info(f"Wrote batch of {len(self.batch)} transactions to DuckDB")
        finally:
            conn.close()

        self.batch = []

    def run(self, max_messages: Optional[int] = None) -> None:
        """
        Consume messages and write to DuckDB.

        Args:
            max_messages: Stop after this many messages (None = run forever)
        """
        if not self.consumer:
            self.connect()

        messages_processed = 0

        try:
            for message in self.consumer:
                try:
                    transformed = self.transform_message(message.value)
                    self.batch.append(transformed)

                    logger.debug(
                        f"Received: {transformed['merchant']} - £{transformed['amount']:.2f}"
                    )

                    if len(self.batch) >= self.batch_size:
                        self.write_batch()

                    messages_processed += 1
                    if max_messages and messages_processed >= max_messages:
                        logger.info(f"Reached max messages: {max_messages}")
                        break

                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    continue

        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            # Write any remaining messages
            if self.batch:
                self.write_batch()
            self.close()

    def close(self) -> None:
        """Close the consumer."""
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer closed")


def run_consumer(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "transactions",
    group_id: str = "budgetbox-consumer",
    batch_size: int = 10,
    max_messages: Optional[int] = None,
) -> None:
    """Entry point for running the consumer."""
    consumer = TransactionConsumer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        group_id=group_id,
        batch_size=batch_size,
    )
    consumer.connect()
    consumer.run(max_messages=max_messages)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka transaction consumer")
    parser.add_argument(
        "--bootstrap-servers",
        default="localhost:9092",
        help="Kafka bootstrap servers",
    )
    parser.add_argument(
        "--topic",
        default="transactions",
        help="Kafka topic to consume from",
    )
    parser.add_argument(
        "--group-id",
        default="budgetbox-consumer",
        help="Consumer group ID",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Batch size for writes",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Maximum messages to consume (default: unlimited)",
    )

    args = parser.parse_args()

    run_consumer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        batch_size=args.batch_size,
        max_messages=args.max_messages,
    )
