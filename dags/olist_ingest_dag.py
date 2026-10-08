"""Olist ingest: check files -> Spark clean to GCS (Bronze) -> load BigQuery Silver -> DQ checks."""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT = os.environ["GCP_PROJECT_ID"]
BUCKET = os.environ["GCS_BUCKET_NAME"]
LOCATION = os.environ.get("BQ_LOCATION", "asia-southeast1")
RAW_DIR = Path("/opt/airflow/data/raw")

# table -> (csv file, primary key columns)
TABLES = {
    "customers": ("olist_customers_dataset.csv", ["customer_id"]),
    "orders": ("olist_orders_dataset.csv", ["order_id"]),
    "order_items": ("olist_order_items_dataset.csv", ["order_id", "order_item_id"]),
    "order_payments": ("olist_order_payments_dataset.csv", ["order_id", "payment_sequential"]),
    "order_reviews": ("olist_order_reviews_dataset.csv", ["review_id", "order_id"]),
}


def check_source_files() -> None:
    missing = [f for f, _ in TABLES.values() if not (RAW_DIR / f).exists()]
    empty = [f for f, _ in TABLES.values() if (RAW_DIR / f).exists() and (RAW_DIR / f).stat().st_size == 0]
    if missing or empty:
        raise AirflowFailException(f"missing={missing} empty={empty}")


def run_quality_checks() -> None:
    from google.cloud import bigquery

    client = bigquery.Client(project=PROJECT, location=LOCATION)
    failures = []
    for table, (_, pk) in TABLES.items():
        key_expr = "TO_JSON_STRING(STRUCT(" + ", ".join(pk) + "))"
        null_cond = " OR ".join(f"{c} IS NULL" for c in pk)
        sql = f"""
            SELECT COUNT(*) AS total,
                   COUNT(DISTINCT {key_expr}) AS distinct_keys,
                   COUNTIF({null_cond}) AS null_keys
            FROM `{PROJECT}.silver.{table}`
        """
        row = list(client.query(sql).result())[0]
        print(f"{table}: total={row.total} distinct_keys={row.distinct_keys} null_keys={row.null_keys}")
        if row.total == 0 or row.total != row.distinct_keys or row.null_keys > 0:
            failures.append(table)
    if failures:
        raise AirflowFailException(f"Data quality failed for: {failures}")


default_args = {"owner": "data-eng", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="olist_ingest",
    description="Olist CSV -> Spark clean -> GCS Parquet (Bronze) -> BigQuery Silver",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
    default_args=default_args,
    tags=["retail360", "phase2"],
) as dag:
    check_files = PythonOperator(task_id="check_source_files", python_callable=check_source_files)

    spark_clean = BashOperator(
        task_id="spark_clean_to_gcs",
        bash_command="python /opt/airflow/spark_jobs/ingest_olist.py",
        execution_timeout=timedelta(minutes=30),
    )

    load_tasks = [
        GCSToBigQueryOperator(
            task_id=f"load_{table}_to_silver",
            bucket=BUCKET,
            source_objects=[f"bronze/{table}/*.parquet"],
            destination_project_dataset_table=f"{PROJECT}.silver.{table}",
            source_format="PARQUET",
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
            location=LOCATION,
        )
        for table in TABLES
    ]

    quality_checks = PythonOperator(task_id="quality_checks", python_callable=run_quality_checks)

    check_files >> spark_clean >> load_tasks >> quality_checks
    trigger_gold = TriggerDagRunOperator(task_id="trigger_gold_dbt", trigger_dag_id="olist_gold_dbt")
    quality_checks >> trigger_gold