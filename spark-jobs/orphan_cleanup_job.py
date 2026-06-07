"""Iceberg orphan file cleanup job — removes data files not referenced by any snapshot."""

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
    SparkSession.builder.appName("IcebergOrphanCleanup")
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
    older_than_days: int = config["orphan_cleanup"]["older_than_days"]
    older_than: datetime = datetime.now() - timedelta(days=older_than_days)
    older_than_str: str = older_than.strftime("%Y-%m-%d %H:%M:%S")

    print(f"[OrphanCleanup] Removing orphan files older than {older_than_days} days on table: {table}")
    print(f"[OrphanCleanup] Cutoff timestamp: {older_than_str}")

    result = spark.sql(f"""
        CALL {catalog}.system.remove_orphan_files(
            table => '{table}',
            older_than => TIMESTAMP '{older_than_str}'
        )
    """)

    removed: int = result.count()
    print(f"[OrphanCleanup] Removed {removed} orphan files.")

except Exception as exc:  # noqa: BLE001
    print(f"[OrphanCleanup] ERROR: {exc}", file=sys.stderr)
    sys.exit(1)

finally:
    spark.stop()
