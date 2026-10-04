"""Read raw Olist CSVs, clean them with PySpark, write Parquet to GCS (Bronze)."""
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

RAW_DIR = "/opt/airflow/data/raw"
BUCKET = os.environ["GCS_BUCKET_NAME"]
KEYFILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "/opt/airflow/credentials/gcp-sa.json")
OUT_ROOT = f"gs://{BUCKET}/bronze"
GCS_JAR = "/opt/jars/gcs-connector-hadoop3-2.2.21-shaded.jar"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("olist_ingest")
        .master(os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077"))
        # client mode: executors must be able to connect back to the driver
        .config("spark.driver.host", "airflow-scheduler")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.extraClassPath", GCS_JAR)
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "2")
        # GCS access
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", KEYFILE)
        # BigQuery-friendly timestamps (default INT96 is avoided)
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, filename: str) -> DataFrame:
    return (
        spark.read.option("header", True)
        .option("multiLine", True)  # review comments contain line breaks
        .option("quote", '"')
        .option("escape", '"')
        .csv(f"file://{RAW_DIR}/{filename}")
    )


def trim_strings(df: DataFrame) -> DataFrame:
    """Trim every string column; empty strings become NULL."""
    for name, dtype in df.dtypes:
        if dtype == "string":
            c = F.trim(F.col(name))
            df = df.withColumn(name, F.when(c == "", None).otherwise(c))
    return df


def clean_customers(df: DataFrame) -> DataFrame:
    return (
        trim_strings(df)
        .withColumn("customer_zip_code_prefix", F.col("customer_zip_code_prefix").cast("string"))
        .withColumn("customer_city", F.lower("customer_city"))
        .withColumn("customer_state", F.upper("customer_state"))
    )


def clean_orders(df: DataFrame) -> DataFrame:
    df = trim_strings(df).withColumn("order_status", F.lower("order_status"))
    for c in [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ]:
        df = df.withColumn(c, F.to_timestamp(c))
    return df


def clean_items(df: DataFrame) -> DataFrame:
    return (
        trim_strings(df)
        .withColumn("order_item_id", F.col("order_item_id").cast("int"))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
        .withColumn("price", F.col("price").cast("double"))
        .withColumn("freight_value", F.col("freight_value").cast("double"))
    )


def clean_payments(df: DataFrame) -> DataFrame:
    return (
        trim_strings(df)
        .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
        .withColumn("payment_installments", F.col("payment_installments").cast("int"))
        .withColumn("payment_value", F.col("payment_value").cast("double"))
    )


def clean_reviews(df: DataFrame) -> DataFrame:
    return (
        trim_strings(df)
        .withColumn("review_score", F.col("review_score").cast("int"))
        .withColumn("review_creation_date", F.to_timestamp("review_creation_date"))
        .withColumn("review_answer_timestamp", F.to_timestamp("review_answer_timestamp"))
        .filter(F.col("review_score").between(1, 5))
    )


# table -> (csv file, cleaning function, primary key columns)
TABLES = {
    "customers": ("olist_customers_dataset.csv", clean_customers, ["customer_id"]),
    "orders": ("olist_orders_dataset.csv", clean_orders, ["order_id"]),
    "order_items": ("olist_order_items_dataset.csv", clean_items, ["order_id", "order_item_id"]),
    "order_payments": ("olist_order_payments_dataset.csv", clean_payments, ["order_id", "payment_sequential"]),
    # review_id is not unique in Olist, so the key is (review_id, order_id)
    "order_reviews": ("olist_order_reviews_dataset.csv", clean_reviews, ["review_id", "order_id"]),
}


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    summary = []

    for table, (filename, clean_fn, pk) in TABLES.items():
        raw = read_csv(spark, filename)
        raw_count = raw.count()

        cleaned = clean_fn(raw).dropna(subset=pk).dropDuplicates(pk).cache()
        clean_count = cleaned.count()

        # data quality gate: key must be unique and non-null after cleaning
        distinct_keys = cleaned.select(*pk).distinct().count()
        if clean_count == 0 or distinct_keys != clean_count:
            print(f"[DQ FAIL] {table}: rows={clean_count}, distinct_keys={distinct_keys}")
            sys.exit(1)

        target = f"{OUT_ROOT}/{table}"
        cleaned.coalesce(1).write.mode("overwrite").parquet(target)
        cleaned.unpersist()

        summary.append((table, raw_count, clean_count, raw_count - clean_count))
        print(f"[OK] {table}: raw={raw_count} clean={clean_count} -> {target}")

    print("\n=== SUMMARY (table, raw, clean, dropped) ===")
    for row in summary:
        print(row)
    spark.stop()


if __name__ == "__main__":
    main()