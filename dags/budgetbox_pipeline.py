"""
BudgetBox Analytics Pipeline DAG

Orchestrates:
1. Ingest mock transaction data → raw.transactions
2. Run dbt models (staging → marts)
3. Run dbt tests

Schedule: Hourly
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


# Paths
PROJECT_ROOT = Path("/opt/airflow")
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "budgetbox"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_transaction_ingestion(**context):
    """Run mock transaction ingestion."""
    import sys
    sys.path.insert(0, str(PROJECT_ROOT))
    
    from ingestion.mock_transactions import MockTransactionsIngestion
    
    ingestion = MockTransactionsIngestion()
    result = ingestion.run(num_transactions=50, days_back=30)
    
    # Log results for Airflow UI
    print(f"Ingested {result['rows_loaded']} transactions")
    return result


with DAG(
    dag_id="budgetbox_pipeline",
    default_args=default_args,
    description="End-to-end financial data pipeline",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["budgetbox", "pipeline"],
    doc_md=__doc__,
) as dag:

    # =========================================================================
    # INGESTION
    # =========================================================================
    ingest_transactions = PythonOperator(
        task_id="ingest_transactions",
        python_callable=run_transaction_ingestion,
        doc_md="Generate and load mock transaction data into raw.transactions",
    )

    # =========================================================================
    # DBT TRANSFORMATION
    # =========================================================================
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
        doc_md="Run all dbt models: staging → marts",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
        doc_md="Run dbt tests to validate data quality",
    )

    # =========================================================================
    # DEPENDENCIES
    # =========================================================================
    ingest_transactions >> dbt_run >> dbt_test
