"""Generate sample Iceberg sales data on MinIO to seed the maintenance pipeline.

Writes 10,000+ rows of synthetic sales data in many small files so compaction
has something meaningful to work on.  Run this once after `docker compose up`:

    make seed
"""

from __future__ import annotations

import os
import random
from datetime import datetime, timedelta

from faker import Faker
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    DateType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

fake = Faker()
random.seed(42)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
NUM_ROWS = 10_000
BATCH_SIZE = 500

PRODUCTS = [
    "Laptop", "Mouse", "Keyboard", "Monitor", "Headset",
    "Webcam", "USB Hub", "Desk Lamp", "Chair", "Mousepad",
]

spark = (
    SparkSession.builder.appName("GenerateSampleData")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri", NESSIE_URI)
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.warehouse", "s3://warehouse/data")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

schema = StructType([
    StructField("sale_date", DateType(), False),
    StructField("product", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", FloatType(), False),
    StructField("customer_name", StringType(), False),
    StructField("region", StringType(), False),
])

print("[SampleData] Creating namespace and table...")

spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.default")
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.default.sales_data (
        sale_date   DATE,
        product     STRING,
        quantity    INT,
        unit_price  FLOAT,
        customer_name STRING,
        region      STRING
    )
    USING iceberg
    TBLPROPERTIES (
        'write.target-file-size-bytes' = '1048576',
        'gc.enabled' = 'true'
    )
""")

print(f"[SampleData] Writing {NUM_ROWS} rows in batches of {BATCH_SIZE}...")

base_date = datetime(2024, 1, 1)
total_written = 0

for batch_start in range(0, NUM_ROWS, BATCH_SIZE):
    rows = []
    for _ in range(BATCH_SIZE):
        sale_date = (base_date + timedelta(days=random.randint(0, 365))).date()
        rows.append(Row(
            sale_date=sale_date,
            product=random.choice(PRODUCTS),
            quantity=random.randint(1, 20),
            unit_price=round(random.uniform(9.99, 999.99), 2),
            customer_name=fake.name(),
            region=fake.state_abbr(),
        ))

    df = spark.createDataFrame(rows, schema)
    df.repartition(1).writeTo("nessie.default.sales_data").append()
    total_written += len(rows)
    print(f"[SampleData] Written {total_written}/{NUM_ROWS} rows...")

print(f"[SampleData] Done. {total_written} rows across {NUM_ROWS // BATCH_SIZE} small files.")

file_count = spark.sql("SELECT count(*) as cnt FROM nessie.default.sales_data.files").collect()[0]["cnt"]
print(f"[SampleData] Total data files in table: {file_count}")

spark.stop()
