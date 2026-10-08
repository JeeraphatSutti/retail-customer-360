"""Build Gold feature tables with dbt, then run dbt tests."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT = "cd /opt/airflow/dbt_project && dbt"

with DAG(
    dag_id="olist_gold_dbt",
    description="dbt run + dbt test: Silver -> Gold (customer_360 feature store)",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "data-eng", "retries": 0, "retry_delay": timedelta(minutes=2)},
    tags=["retail360", "phase3"],
) as dag:
    dbt_run = BashOperator(task_id="dbt_run", bash_command=f"{DBT} run --no-use-colors")
    dbt_test = BashOperator(task_id="dbt_test", bash_command=f"{DBT} test --no-use-colors")

    dbt_run >> dbt_test
