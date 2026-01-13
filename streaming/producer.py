"""
Kafka Producer for real-time transaction events.

Simulates a bank sending transaction events in real-time.
"""

import json
import random
import time
import uuid
from datetime import datetime
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

# Transaction templates (subset for streaming)
STREAMING_CATEGORIES = {
    "groceries": {
        "merchants": ["Tesco Express", "Sainsbury's Local", "Co-op", "Aldi"],
        "amount_range": (5.0, 80.0),
        "weight": 0.25,
    },
    "dining": {
        "merchants": ["Pret A Manger", "Costa Coffee", "Starbucks", "Greggs"],
        "amount_range": (3.0, 25.0),
        "weight": 0.30,
    },
    "transport": {
        "merchants": ["TfL", "Uber", "Bolt"],
        "amount_range": (2.0, 35.0),
        "weight": 0.25,
    },
    "shopping": {
        "merchants": ["Amazon", "ASOS", "Argos"],
        "amount_range": (10.0, 150.0),
        "weight": 0.15,
    },
    "entertainment": {
        "merchants": ["Netflix", "Spotify", "Apple"],
        "amount_range": (5.0, 20.0),
        "weight": 0.05,
    },
}

ACCOUNTS = [
    {"id": "acc_001", "name": "Barclays Current", "type": "current", "currency": "GBP"},
    {"id": "acc_002", "name": "Nationwide Savings", "type": "savings", "currency": "GBP"},
    {"id": "acc_003", "name": "HSBC Credit Card", "type": "credit", "currency": "GBP"},
    {"id": "acc_004", "name": "Monzo Current", "type": "current", "currency": "GBP"},
]


class TransactionProducer:
    """Produces simulated transaction events to Kafka."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer: Optional[KafkaProducer] = None

    def connect(self) -> None:
        """Connect to Kafka broker."""
        logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")
        
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
        )
        
        logger.info("Connected to Kafka")

    def generate_transaction(self) -> dict:
        """Generate a single random transaction."""
        categories = list(STREAMING_CATEGORIES.keys())
        weights = [STREAMING_CATEGORIES[c]["weight"] for c in categories]
        
        category = random.choices(categories, weights=weights)[0]
        template = STREAMING_CATEGORIES[category]
        
        account = random.choice(ACCOUNTS)
        merchant = random.choice(template["merchants"])
        amount = round(random.uniform(*template["amount_range"]), 2)
        
        now = datetime.utcnow()
        
        return {
            "transaction_id": str(uuid.uuid4()),
            "account_id": account["id"],
            "account_name": account["name"],
            "account_type": account["type"],
            "currency": account["currency"],
            "amount": amount,
            "transaction_type": "debit",
            "category": category,
            "merchant": merchant,
            "description": f"{merchant} - {category.title()}",
            "transaction_date": now.strftime("%Y-%m-%d"),
            "transaction_timestamp": now.isoformat(),
            "event_time": now.isoformat(),
        }

    def produce(self, transaction: dict) -> None:
        """Send a transaction to Kafka."""
        if not self.producer:
            raise RuntimeError("Producer not connected. Call connect() first.")
        
        try:
            future = self.producer.send(
                self.topic,
                key=transaction["account_id"],
                value=transaction,
            )
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            logger.debug(
                f"Sent transaction {transaction['transaction_id'][:8]} "
                f"to {record_metadata.topic}:{record_metadata.partition}"
            )
        except KafkaError as e:
            logger.error(f"Failed to send transaction: {e}")
            raise

    def run(
        self,
        num_transactions: Optional[int] = None,
        interval_seconds: float = 1.0,
    ) -> None:
        """
        Continuously produce transactions.
        
        Args:
            num_transactions: Number of transactions to produce (None = infinite)
            interval_seconds: Delay between transactions
        """
        self.connect()
        
        count = 0
        try:
            while num_transactions is None or count < num_transactions:
                transaction = self.generate_transaction()
                self.produce(transaction)
                
                logger.info(
                    f"[{count + 1}] {transaction['merchant']} - "
                    f"£{transaction['amount']:.2f} ({transaction['category']})"
                )
                
                count += 1
                
                # Random interval for realistic simulation
                sleep_time = interval_seconds * random.uniform(0.5, 1.5)
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info(f"Stopped after producing {count} transactions")
        finally:
            self.close()

    def close(self) -> None:
        """Close the producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")


def run_producer(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "transactions",
    num_transactions: Optional[int] = None,
    interval_seconds: float = 1.0,
) -> None:
    """Entry point for running the producer."""
    producer = TransactionProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
    )
    producer.run(
        num_transactions=num_transactions,
        interval_seconds=interval_seconds,
    )


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Kafka Transaction Producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="transactions")
    parser.add_argument("--num-transactions", type=int, default=None)
    parser.add_argument("--interval", type=float, default=1.0)
    
    args = parser.parse_args()
    
    run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        num_transactions=args.num_transactions,
        interval_seconds=args.interval,
    )
