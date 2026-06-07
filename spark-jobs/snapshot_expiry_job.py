"""Iceberg snapshot expiry job — removes old snapshots beyond the retention window."""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]

from pyspark.sql import SparkSession

_CONFIG_PATH = "/opt/airflow/config/maintenance_config.toml"

with open(_CONFIG_PATH, "rb") as f:
    config = tomllib.load(f)

_minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
_minio_user = os.getenv("MINIO_ROOT_USER", "minioadmin")
_minio_password = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
_nessie_uri = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")

spark = (
    SparkSession.builder.appName("IcebergSnapshotExpiry")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri", _nessie_uri)
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.warehouse", "s3://warehouse/data")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", _minio_endpoint)
    .config("spark.hadoop.fs.s3a.access.key", _minio_user)
    .config("spark.hadoop.fs.s3a.secret.key", _minio_password)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

try:
    catalog: str = config["iceberg"]["catalog_name"]
    table: str = config["iceberg"]["target_table"]
    max_age_days: int = config["snapshot_expiry"]["max_snapshot_age_days"]
    min_keep: int = config["snapshot_expiry"]["min_snapshots_to_keep"]

    older_than: datetime = datetime.now() - timedelta(days=max_age_days)
    older_than_str: str = older_than.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[SnapshotExpiry] Expiring snapshots older than {max_age_days} days on table: {table}")
    print(f"[SnapshotExpiry] Cutoff timestamp: {older_than_str} | min_keep: {min_keep}")

    spark.sql(f"""
        CALL {catalog}.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{older_than_str}',
            retain_last => {min_keep}
        )
    """)

    print("[SnapshotExpiry] Done.")

except Exception as exc:  # noqa: BLE001
    print(f"[SnapshotExpiry] ERROR: {exc}", file=sys.stderr)
    sys.exit(1)

finally:
    spark.stop()
