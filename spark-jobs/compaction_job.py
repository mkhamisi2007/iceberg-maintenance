"""Iceberg compaction job — rewrites small data files into larger target-sized files."""

from __future__ import annotations

import os
import sys

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
    SparkSession.builder.appName("IcebergCompaction")
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
    target_size: int = config["compaction"]["target_file_size_bytes"]

    print(f"[Compaction] Starting compaction on table: {table}")

    before: int = spark.sql(f"SELECT count(*) as cnt FROM {table}.files").collect()[0]["cnt"]
    print(f"[Compaction] Files before: {before}")

    spark.sql(f"""
        CALL {catalog}.system.rewrite_data_files(
            table => '{table}',
            options => map('target-file-size-bytes', '{target_size}')
        )
    """)

    after: int = spark.sql(f"SELECT count(*) as cnt FROM {table}.files").collect()[0]["cnt"]
    print(f"[Compaction] Files after: {after}")
    print(f"[Compaction] Reduced from {before} to {after} files.")

except Exception as exc:  # noqa: BLE001
    print(f"[Compaction] ERROR: {exc}", file=sys.stderr)
    sys.exit(1)

finally:
    spark.stop()
