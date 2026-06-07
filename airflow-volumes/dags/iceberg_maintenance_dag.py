"""Airflow DAG for automated Iceberg table maintenance.

Runs four sequential tasks: compaction → snapshot expiry →
orphan file cleanup → health report generation.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]

log = logging.getLogger(__name__)

_CONFIG_PATH = "/opt/airflow/config/maintenance_config.toml"

with open(_CONFIG_PATH, "rb") as _f:
    config = tomllib.load(_f)

_SPARK_SUBMIT = (
    "PYSPARK_PYTHON=python3 PYSPARK_DRIVER_PYTHON=python3 "
    "/home/airflow/.local/bin/spark-submit "
    "--master local[*] "
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
    "--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT} "
    "--conf spark.hadoop.fs.s3a.access.key=${MINIO_ROOT_USER} "
    "--conf spark.hadoop.fs.s3a.secret.key=${MINIO_ROOT_PASSWORD} "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog "
    "--conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v2 "
    "--conf spark.sql.catalog.nessie.ref=main "
    "--conf spark.sql.catalog.nessie.warehouse=s3://warehouse/data "
)


def _on_failure_callback(context: dict) -> None:
    """Log DAG/task failure details to Airflow logs."""
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    log.error(
        "[MaintenanceDAG] Task failed | dag=%s | task=%s | run_id=%s | exception=%s",
        context.get("dag").dag_id,
        task_instance.task_id if task_instance else "unknown",
        context.get("run_id"),
        exception,
    )


default_args: dict = {
    "owner": "devops",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "on_failure_callback": _on_failure_callback,
}

with DAG(
    dag_id="iceberg_table_maintenance",
    default_args=default_args,
    description="Automated Iceberg table maintenance: compaction, snapshot expiry, orphan cleanup",
    schedule=config["schedule"]["cron"],
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["iceberg", "maintenance", "devops"],
) as dag:

    # ── Task 1: Compact small files into larger target-sized files ──────────
    compaction = BashOperator(
        task_id="compaction",
        bash_command=_SPARK_SUBMIT + "/opt/spark-jobs/compaction_job.py",
        env={
            "MINIO_ENDPOINT": "{{ var.value.get('MINIO_ENDPOINT', 'http://minio:9000') }}",
            "MINIO_ROOT_USER": "{{ var.value.get('MINIO_ROOT_USER', 'minioadmin') }}",
            "MINIO_ROOT_PASSWORD": "{{ var.value.get('MINIO_ROOT_PASSWORD', 'minioadmin123') }}",
        },
    )

    # ── Task 2: Expire old snapshots to reclaim metadata storage ───────────
    snapshot_expiry = BashOperator(
        task_id="snapshot_expiry",
        bash_command=_SPARK_SUBMIT + "/opt/spark-jobs/snapshot_expiry_job.py",
        env={
            "MINIO_ENDPOINT": "{{ var.value.get('MINIO_ENDPOINT', 'http://minio:9000') }}",
            "MINIO_ROOT_USER": "{{ var.value.get('MINIO_ROOT_USER', 'minioadmin') }}",
            "MINIO_ROOT_PASSWORD": "{{ var.value.get('MINIO_ROOT_PASSWORD', 'minioadmin123') }}",
        },
    )

    # ── Task 3: Remove orphan data files not tracked by any snapshot ───────
    orphan_cleanup = BashOperator(
        task_id="orphan_cleanup",
        bash_command=_SPARK_SUBMIT + "/opt/spark-jobs/orphan_cleanup_job.py",
        env={
            "MINIO_ENDPOINT": "{{ var.value.get('MINIO_ENDPOINT', 'http://minio:9000') }}",
            "MINIO_ROOT_USER": "{{ var.value.get('MINIO_ROOT_USER', 'minioadmin') }}",
            "MINIO_ROOT_PASSWORD": "{{ var.value.get('MINIO_ROOT_PASSWORD', 'minioadmin123') }}",
        },
    )

    # ── Task 4: Write a JSON health report summarising the run ─────────────
    from utils.health_report import generate_report  # noqa: E402

    health_report = PythonOperator(
        task_id="health_report",
        python_callable=generate_report,
        op_kwargs={"config": config},
    )

    # ── Pipeline dependency chain ───────────────────────────────────────────
    compaction >> snapshot_expiry >> orphan_cleanup >> health_report
