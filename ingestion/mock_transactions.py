"""
Mock transaction data generator for UK personal finance.

Generates realistic transaction data aligned with the BudgetBox Django schema,
using UK-specific categories, merchants, and amounts.

In production, this would be replaced with a real bank API integration
(e.g., Plaid, TrueLayer, or Open Banking APIs).
"""

import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional

import pandas as pd
from faker import Faker
from loguru import logger

from ingestion.base import BaseIngestion


# =============================================================================
# UK-SPECIFIC CONFIGURATION (aligned with Django BudgetBox schema)
# =============================================================================

# Account configuration matching Django BankAccount model
ACCOUNTS = [
    {
        "account_id": "acc_001",
        "account_name": "Main Current Account",
        "account_type": "current",
        "bank_name": "Barclays",
        "currency": "GBP",
    },
    {
        "account_id": "acc_002",
        "account_name": "Savings Account",
        "account_type": "savings",
        "bank_name": "Nationwide",
        "currency": "GBP",
    },
    {
        "account_id": "acc_003",
        "account_name": "Credit Card",
        "account_type": "credit",
        "bank_name": "HSBC",
        "currency": "GBP",
    },
    {
        "account_id": "acc_004",
        "account_name": "Monzo Spending",
        "account_type": "current",
        "bank_name": "Monzo",
        "currency": "GBP",
    },
]

# Transaction templates with UK categories matching Django Category model
# Categories align with seed_user_data.py from the Django project
TRANSACTION_TEMPLATES = {
    # =========================================================================
    # INCOME CATEGORIES
    # =========================================================================
    "Salary": {
        "merchants": [
            "Monthly Salary Payment",
            "Salary - EMPLOYER NAME",
            "BACS Payment - Salary",
        ],
        "amount_range": (2500.00, 4500.00),
        "category_type": "income",
        "frequency": "monthly",
        "preferred_account": "current",
    },
    "Freelance": {
        "merchants": [
            "Freelance Project Payment",
            "Invoice Payment - Client",
            "Consulting Fee",
            "Contract Work Payment",
        ],
        "amount_range": (300.00, 2000.00),
        "category_type": "income",
        "frequency": "rare",
        "preferred_account": "current",
    },
    "Bonus": {
        "merchants": [
            "Annual Bonus",
            "Performance Bonus",
            "Quarterly Bonus",
        ],
        "amount_range": (500.00, 3000.00),
        "category_type": "income",
        "frequency": "rare",
        "preferred_account": "current",
    },
    "Other Income": {
        "merchants": [
            "Bank Interest",
            "Refund",
            "Cashback Reward",
            "Gift Received",
        ],
        "amount_range": (10.00, 200.00),
        "category_type": "income",
        "frequency": "rare",
        "preferred_account": "current",
    },

    # =========================================================================
    # EXPENSE CATEGORIES - ESSENTIALS
    # =========================================================================
    "Rent/Mortgage": {
        "merchants": [
            "Monthly Rent Payment",
            "Mortgage Payment - Halifax",
            "Rent - Landlord",
            "Standing Order - Rent",
        ],
        "amount_range": (1200.00, 2500.00),
        "category_type": "expense",
        "frequency": "monthly",
        "preferred_account": "current",
    },
    "Council Tax": {
        "merchants": [
            "Council Tax - Wandsworth",
            "Council Tax Payment",
            "Council Tax - Camden",
            "Council Tax - Hackney",
            "Council Tax - Islington",
        ],
        "amount_range": (120.00, 200.00),
        "category_type": "expense",
        "frequency": "monthly",
        "preferred_account": "current",
    },
    "Utilities": {
        "merchants": [
            "British Gas - Energy Bill",
            "Thames Water",
            "EDF Energy",
            "Octopus Energy",
            "Bulb Energy",
        ],
        "amount_range": (80.00, 200.00),
        "category_type": "expense",
        "frequency": "monthly",
        "preferred_account": "current",
    },
    "Insurance": {
        "merchants": [
            "Aviva Home Insurance",
            "Direct Line Car Insurance",
            "Vitality Health",
            "Admiral Insurance",
            "Churchill Insurance",
        ],
        "amount_range": (25.00, 150.00),
        "category_type": "expense",
        "frequency": "monthly",
        "preferred_account": "current",
    },
    "Groceries": {
        "merchants": [
            "Tesco Express",
            "Sainsbury's Local",
            "ASDA Superstore",
            "Waitrose & Partners",
            "M&S Food",
            "Lidl GB",
            "ALDI",
            "Co-op Food",
            "Iceland",
            "Morrisons",
        ],
        "amount_range": (15.00, 120.00),
        "category_type": "expense",
        "frequency": "weekly",
        "preferred_account": "current",
    },

    # =========================================================================
    # EXPENSE CATEGORIES - TRANSPORT
    # =========================================================================
    "TfL/Oyster": {
        "merchants": [
            "TfL Auto Top-up",
            "Oyster Card Top-up",
            "Monthly Travelcard Zone 1-3",
            "TfL Contactless",
        ],
        "amount_range": (150.00, 250.00),
        "category_type": "expense",
        "frequency": "monthly",
        "preferred_account": "current",
    },
    "Transport": {
        "merchants": [
            "TfL Travel - Zone 1-3",
            "Uber Trip",
            "Bolt Ride",
            "Santander Cycles",
            "National Rail",
            "Citymapper PASS",
            "Addison Lee",
            "FREE NOW",
        ],
        "amount_range": (3.00, 40.00),
        "category_type": "expense",
        "frequency": "frequent",
        "preferred_account": "credit",
    },
    "Petrol": {
        "merchants": [
            "Shell Petrol Station",
            "BP Garage",
            "Esso",
            "Tesco Petrol",
            "Sainsbury's Fuel",
        ],
        "amount_range": (40.00, 80.00),
        "category_type": "expense",
        "frequency": "occasional",
        "preferred_account": "current",
    },

    # =========================================================================
    # EXPENSE CATEGORIES - SERVICES
    # =========================================================================
    "Mobile Phone": {
        "merchants": [
            "EE Mobile",
            "O2 Mobile",
            "Three Mobile",
            "Vodafone UK",
            "giffgaff",
        ],
        "amount_range": (15.00, 50.00),
        "category_type": "expense",
        "frequency": "monthly",
        "preferred_account": "current",
    },
    "Internet": {
        "merchants": [
            "BT Broadband",
            "Virgin Media",
            "Sky Broadband",
            "TalkTalk",
            "Hyperoptic",
        ],
        "amount_range": (25.00, 60.00),
        "category_type": "expense",
        "frequency": "monthly",
        "preferred_account": "current",
    },
    "Subscriptions": {
        "merchants": [
            "Netflix",
            "Spotify Premium",
            "Amazon Prime",
            "Disney+",
            "Apple Music",
            "PureGym Membership",
            "The Times Digital",
            "PlayStation Plus",
            "NOW TV",
        ],
        "amount_range": (5.99, 49.99),
        "category_type": "expense",
        "frequency": "monthly",
        "preferred_account": "current",
    },

    # =========================================================================
    # EXPENSE CATEGORIES - LIFESTYLE
    # =========================================================================
    "Eating Out": {
        "merchants": [
            "Pret A Manger",
            "Costa Coffee",
            "Nando's",
            "Wagamama",
            "Pizza Express",
            "Dishoom",
            "Five Guys",
            "Leon",
            "Honest Burgers",
            "Starbucks",
            "Greggs",
            "McDonald's",
            "Itsu",
            "Wasabi",
            "EAT.",
            "Gail's Bakery",
            "Paul UK",
        ],
        "amount_range": (4.00, 85.00),
        "category_type": "expense",
        "frequency": "frequent",
        "preferred_account": "credit",
    },
    "Entertainment": {
        "merchants": [
            "Vue Cinema",
            "Odeon Leicester Square",
            "National Theatre",
            "O2 Arena",
            "Steam Purchase",
            "Apple App Store",
            "Ticketmaster",
            "London Zoo",
            "British Museum Donation",
            "Eventbrite",
            "DICE",
            "Royal Albert Hall",
        ],
        "amount_range": (5.00, 120.00),
        "category_type": "expense",
        "frequency": "occasional",
        "preferred_account": "credit",
    },
    "Shopping": {
        "merchants": [
            "Amazon UK",
            "John Lewis",
            "Marks & Spencer",
            "Primark Oxford Street",
            "IKEA Wembley",
            "Argos",
            "Next",
            "H&M",
            "Uniqlo",
            "Boots",
            "Superdrug",
            "ASOS",
            "Zara",
            "TK Maxx",
            "Decathlon",
            "Currys PC World",
        ],
        "amount_range": (10.00, 250.00),
        "category_type": "expense",
        "frequency": "occasional",
        "preferred_account": "credit",
    },
    "Health & Fitness": {
        "merchants": [
            "Boots Pharmacy",
            "Holland & Barrett",
            "GymBox",
            "Barry's Bootcamp",
            "Superdrug",
            "NHS Prescription",
            "Specsavers",
            "Bupa Dental",
        ],
        "amount_range": (10.00, 150.00),
        "category_type": "expense",
        "frequency": "occasional",
        "preferred_account": "credit",
    },
    "Personal Care": {
        "merchants": [
            "Haircut - Local Barber",
            "Toni & Guy",
            "Treatwell Booking",
            "The Body Shop",
            "Lush",
            "Space NK",
        ],
        "amount_range": (15.00, 80.00),
        "category_type": "expense",
        "frequency": "occasional",
        "preferred_account": "credit",
    },
    "Travel": {
        "merchants": [
            "British Airways",
            "easyJet",
            "Ryanair",
            "Eurostar",
            "Booking.com",
            "Airbnb",
            "Hotels.com",
            "Trainline",
            "National Express",
        ],
        "amount_range": (50.00, 500.00),
        "category_type": "expense",
        "frequency": "rare",
        "preferred_account": "credit",
    },
    "Other Expense": {
        "merchants": [
            "ATM Withdrawal",
            "Bank Fee",
            "Miscellaneous",
        ],
        "amount_range": (5.00, 50.00),
        "category_type": "expense",
        "frequency": "rare",
        "preferred_account": "current",
    },
}

# Frequency weights for category selection (higher = more common)
FREQUENCY_CONFIG = {
    "monthly": {"transactions_per_month": 1, "day_range": (1, 5)},
    "weekly": {"transactions_per_month": 4, "day_range": (0, 6)},
    "frequent": {"transactions_per_month": (8, 15), "day_range": (0, 30)},
    "occasional": {"transactions_per_month": (2, 5), "day_range": (0, 30)},
    "rare": {"transactions_per_month": (0, 1), "day_range": (0, 30)},
}


class MockTransactionsIngestion(BaseIngestion):
    """
    Generate realistic UK transaction data for development and testing.

    Produces transactions aligned with the BudgetBox Django schema:
    - UK-specific categories (Council Tax, TfL/Oyster, etc.)
    - Realistic London amounts
    - Multiple account types (current, savings, credit)
    """

    def __init__(self):
        super().__init__(table_name="transactions")
        self.fake = Faker("en_GB")

    def extract(
        self,
        num_transactions: int = 100,
        days_back: int = 30,
        seed: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Generate mock transaction data.

        Args:
            num_transactions: Approximate number of transactions to generate
            days_back: How many days back to generate transactions for
            seed: Random seed for reproducibility (optional)

        Returns:
            DataFrame with transaction records
        """
        if seed is not None:
            random.seed(seed)
            Faker.seed(seed)

        logger.info(
            f"Generating ~{num_transactions} mock transactions "
            f"over {days_back} days"
        )

        transactions = []
        today = datetime.now()

        # Calculate how many months we're covering
        months = max(1, days_back // 30)

        # Generate transactions for each category based on frequency
        for category_name, template in TRANSACTION_TEMPLATES.items():
            frequency = template["frequency"]
            freq_config = FREQUENCY_CONFIG[frequency]

            # Determine number of transactions for this category
            if isinstance(freq_config["transactions_per_month"], tuple):
                min_t, max_t = freq_config["transactions_per_month"]
                num_for_category = random.randint(min_t, max_t) * months
            else:
                num_for_category = freq_config["transactions_per_month"] * months

            # Generate transactions for this category
            for _ in range(num_for_category):
                transaction = self._generate_transaction(
                    category_name, template, today, days_back
                )
                if transaction:
                    transactions.append(transaction)

        # Shuffle and limit to approximate target
        random.shuffle(transactions)
        if len(transactions) > num_transactions * 1.2:
            transactions = transactions[:num_transactions]

        df = pd.DataFrame(transactions)

        if not df.empty:
            df = df.sort_values(
                "transaction_timestamp", ascending=False
            ).reset_index(drop=True)

        logger.info(
            f"Generated {len(df)} transactions across {len(ACCOUNTS)} accounts"
        )
        return df

    def _generate_transaction(
        self,
        category_name: str,
        template: dict,
        today: datetime,
        days_back: int,
    ) -> Optional[dict]:
        """Generate a single transaction."""
        # Random timestamp within date range
        days_ago = random.randint(0, days_back)
        hours = random.randint(6, 22)  # Realistic hours
        minutes = random.randint(0, 59)
        timestamp = today - timedelta(days=days_ago, hours=hours, minutes=minutes)

        # Don't generate future transactions
        if timestamp > today:
            return None

        # Select account (prefer specified type, but allow variation)
        preferred_type = template.get("preferred_account", "current")
        matching_accounts = [
            a for a in ACCOUNTS if a["account_type"] == preferred_type
        ]
        account = random.choice(matching_accounts if matching_accounts else ACCOUNTS)

        # Generate amount
        min_amount, max_amount = template["amount_range"]
        amount = round(random.uniform(min_amount, max_amount), 2)

        # Select merchant
        merchant = random.choice(template["merchants"])

        # Determine transaction type
        is_income = template["category_type"] == "income"
        transaction_type = "credit" if is_income else "debit"

        return {
            "transaction_id": str(uuid.uuid4()),
            "account_id": account["account_id"],
            "account_name": account["account_name"],
            "account_type": account["account_type"],
            "bank_name": account["bank_name"],
            "currency": account["currency"],
            "amount": amount,
            "transaction_type": transaction_type,
            "category": category_name,
            "category_type": template["category_type"],
            "merchant": merchant,
            "description": f"{merchant} - {category_name}",
            "transaction_date": timestamp.date(),
            "transaction_timestamp": timestamp,
        }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add calculated fields and metadata."""
        df = super().transform(df)

        if df.empty:
            return df

        # Add signed amount (negative for debits)
        df["signed_amount"] = df.apply(
            lambda row: row["amount"]
            if row["transaction_type"] == "credit"
            else -row["amount"],
            axis=1,
        )

        # Extract date parts for easier querying
        df["transaction_year"] = pd.to_datetime(df["transaction_date"]).dt.year
        df["transaction_month"] = pd.to_datetime(df["transaction_date"]).dt.month
        df["transaction_day_of_week"] = pd.to_datetime(
            df["transaction_date"]
        ).dt.dayofweek

        return df


def ingest_mock_transactions(
    num_transactions: int = 100,
    days_back: int = 30,
    mode: str = "append",
    **kwargs,
) -> dict:
    """
    Airflow-callable function to generate mock transactions.

    This is the entry point called by the Airflow DAG.

    Args:
        num_transactions: Number of transactions to generate
        days_back: Days of history to generate
        mode: 'append' or 'replace'

    Returns:
        Dictionary with ingestion statistics
    """
    ingestion = MockTransactionsIngestion()
    return ingestion.run(
        num_transactions=num_transactions,
        days_back=days_back,
        mode=mode,
    )


if __name__ == "__main__":
    # Local testing
    result = ingest_mock_transactions(num_transactions=50, mode="replace")
    print(result)

    # Verify data
    import duckdb

    conn = duckdb.connect("./data/budgetbox.duckdb")
    print("\nSample transactions:")
    print(conn.execute("SELECT * FROM raw.transactions LIMIT 5").df())
    print("\nCategory distribution:")
    print(
        conn.execute(
            """
            SELECT category, COUNT(*) as count,
                   ROUND(AVG(amount), 2) as avg_amount
            FROM raw.transactions
            GROUP BY category
            ORDER BY count DESC
            """
        ).df()
    )
    conn.close()
