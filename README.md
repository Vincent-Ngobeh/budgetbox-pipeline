# BudgetBox Pipeline

Data engineering pipeline for financial analytics. Demonstrates ETL design with Airflow orchestration, dbt transformations, dimensional modeling, and incremental loading.

## Overview

This project processes UK personal finance data through a modern data stack:

- **Ingestion**: Python scripts pulling from APIs and generating realistic transaction data
- **Orchestration**: Airflow DAGs managing dependencies and scheduling
- **Transformation**: dbt models following staging → intermediate → marts pattern
- **Storage**: DuckDB as a lightweight analytical warehouse
- **Quality**: dbt tests and data validation

## Architecture

```
Data Sources              Orchestration          Transform           Warehouse
────────────────────────────────────────────────────────────────────────────────
• Exchange Rates API  ─┐                    ┌─────────────┐
                       ├──→   Airflow   ──→ │     dbt     │ ──→   DuckDB
• Mock Transactions  ─┘       (DAGs)        │   models    │
                                            └─────────────┘
```

## Data Model

Star schema optimised for financial analytics:

```
                      ┌────────────────┐
                      │  dim_accounts  │
                      └───────┬────────┘
                              │
┌──────────────┐              │              ┌─────────────────┐
│  dim_dates   │──────────────┼──────────────│ dim_categories  │
└──────────────┘              │              └─────────────────┘
                              │
                    ┌─────────┴─────────┐
                    │ fact_transactions │
                    └───────────────────┘
```

## Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow 2.8 |
| Transformation | dbt-core 1.7 |
| Warehouse | DuckDB 0.9 |
| Language | Python 3.10+ |
| Containerisation | Docker Compose |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.10+
- Git

### Setup

```bash
# Clone repository
git clone https://github.com/yourusername/budgetbox-pipeline.git
cd budgetbox-pipeline

# Start services
docker-compose up -d

# Access Airflow UI
open http://localhost:8080
# Username: airflow / Password: airflow
```

### Run Manually (Development)

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run ingestion
python -m ingestion.mock_transactions

# Run dbt
cd dbt/budgetbox
dbt deps
dbt run
dbt test
```

## Project Structure

```
budgetbox-pipeline/
├── dags/                      # Airflow DAGs
│   └── budgetbox_pipeline.py
├── ingestion/                 # Data ingestion scripts
│   ├── __init__.py
│   ├── base.py
│   ├── exchange_rates.py
│   └── mock_transactions.py
├── dbt/
│   └── budgetbox/             # dbt project
│       ├── models/
│       │   ├── staging/       # Raw data cleaning
│       │   ├── intermediate/  # Business logic
│       │   └── marts/         # Dimensional models
│       └── tests/
├── tests/                     # Python tests
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Design Decisions

*Section to be expanded as project develops.*

## License

MIT

## Author

Vincent Sam Ngobeh
- GitHub: [@Vincent-Ngobeh](https://github.com/Vincent-Ngobeh)
- LinkedIn: [Vincent Ngobeh](https://www.linkedin.com/in/vincent-ngobeh/)
