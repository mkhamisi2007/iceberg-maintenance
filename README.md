# Iceberg Table Maintenance Pipeline

A production-style DevOps pipeline that automatically keeps Apache Iceberg tables healthy — compaction, snapshot expiry, and orphan file cleanup — orchestrated by **Apache Airflow**, executed on **Apache Spark**, stored on **MinIO**, catalogued by **Project Nessie**, and queryable via **Trino** and **Apache Superset**.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    Docker Compose Stack                         │
│                                                                 │
│  ┌─────────────┐    spark-submit     ┌──────────────────────┐   │
│  │   Airflow   │ ──────────────────► │  Spark (local[*])    │   │
│  │  Scheduler  │   (runs inside      │  inside Airflow      │   │
│  │  0 2 * * *  │    Airflow ctn)     │  container           │   │
│  └─────────────┘                     └──────────┬───────────┘   │
│                                                 │               │
│                                    NessieCatalog│               │
│                                    /api/v2      │               │
│                                                 ▼               │
│  ┌─────────────┐                   ┌────────────────────────┐   │
│  │    Trino    │ ──── /api/v1 ───► │   Project Nessie       │   │
│  │   :8082     │   (nessie cat.)   │   :19120               │   │
│  └──────┬──────┘                   │   (metadata only,      │   │
│         │                          │    no S3 access)       │   │
│         │ S3 direct                └────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                MinIO  :9000                             │    │
│  │         s3://warehouse/data/default/sales_data/         │    │
│  │         ├── data/          (Parquet files)              │    │
│  │         └── metadata/      (Iceberg metadata JSON)      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  ┌─────────────┐                                                │
│  │  Superset   │ ── SQLAlchemy ──► Trino ──► Iceberg/MinIO      │
│  │   :8088     │   trino://admin@trino:8080/iceberg             │
│  └─────────────┘                                                │
└─────────────────────────────────────────────────────────────────┘
```

### Maintenance DAG Pipeline

```
compaction ──► snapshot_expiry ──► orphan_cleanup ──► health_report
```

| Task | What it does | Why it matters |
|---|---|---|
| **compaction** | Rewrites many small files into 128 MB target files | Reduces scan overhead and query latency |
| **snapshot_expiry** | Removes snapshots older than 7 days (keeps ≥ 2) | Cleans metadata, enables file GC |
| **orphan_cleanup** | Deletes data files not referenced by any snapshot | Reclaims wasted object storage |
| **health_report** | Writes a JSON summary to `reports/` | Audit trail for every pipeline run |

### How the Catalog Works

- **Spark → Nessie** via `/api/v2` using `NessieCatalog` — Spark writes Parquet files and metadata directly to MinIO, then commits a snapshot pointer to Nessie. Nessie never touches S3.
- **Trino → Nessie** via `/api/v1` using `iceberg.catalog.type=nessie` — Trino reads snapshot pointers from Nessie, then fetches the actual metadata/data files directly from MinIO using its own S3 credentials.
- **Superset → Trino** via `trino://admin@trino:8080/iceberg` SQLAlchemy connection, pre-registered on startup.

---

## Tech Stack

| Component | Version |
|---|---|
| Apache Airflow | 2.10.5 |
| Apache Spark | 3.5.3 |
| Apache Iceberg | 1.5.0 |
| Project Nessie | 0.76.6 |
| Trino | 442 |
| Apache Superset | 3.1.3 |
| MinIO | latest |
| PostgreSQL | 15 (Airflow metadata DB) |

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) ≥ 24 with Docker Compose v2
- 8 GB free RAM recommended (Spark runs in the Airflow container)
- Apple Silicon (ARM64) supported — all images are multi-arch

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/mkhamisi2007/iceberg-maintenance.git
cd iceberg-maintenance
```

### 2. Start all services

```bash
make up
```

This will:
- Copy `.env.example` → `.env` automatically if `.env` is missing
- Build the custom Airflow and Superset images
- Start all 9 containers and wait for Airflow to become healthy
- Print all service URLs on completion

### 3. Seed the Iceberg table with sample data

```bash
make seed
```

Writes **10,000 rows** of synthetic sales data into `nessie.default.sales_data` across **20 small Parquet files** — intentionally fragmented so compaction has something meaningful to work on.

### 4. Open all UIs

```bash
make ui
```

| UI | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Spark master | http://localhost:8081 | — |
| Trino | http://localhost:8082 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin123 |
| Nessie API | http://localhost:19120 | — |
| Superset | http://localhost:8088 | admin / admin |

### 5. Run the maintenance pipeline

```bash
# Unpause the DAG (required once after first boot)
docker compose exec airflow-webserver airflow dags unpause iceberg_table_maintenance

# Trigger a manual run
make trigger
```

Watch it execute at **http://localhost:8080** → DAGs → `iceberg_table_maintenance`.

The pipeline runs automatically at **2:00 AM every night** after that.

### 6. View the maintenance report

```bash
make report
```

Or browse `reports/maintenance_report_YYYYMMDD_HHMMSS.json` directly.

### 7. Stop everything

```bash
make down
```

---

## All Make Commands

```bash
make up       # Build images and start all services
make down     # Stop all containers and remove volumes
make seed     # Inject 10,000 sample rows into Iceberg
make trigger  # Manually trigger the maintenance DAG
make ui       # Open all UIs in the browser (macOS)
make logs     # Tail logs from all services
make status   # Show container health status
make report   # Print the latest health report JSON
```

---

## Querying Data in Superset

1. Open **http://localhost:8088** → SQL Lab → SQL Editor
2. Select **"Iceberg via Trino"** as the database and **"default"** as the schema
3. Run queries:

```sql
-- Count all rows
SELECT count(*) FROM sales_data;

-- Revenue by product
SELECT product, count(*) as orders, round(sum(unit_price * quantity), 2) as revenue
FROM sales_data
GROUP BY product
ORDER BY revenue DESC;

-- Inspect physical files (Iceberg metadata)
SELECT file_path, record_count, file_size_in_bytes
FROM iceberg.default."sales_data$files";

-- View snapshot history
SELECT snapshot_id, committed_at, operation
FROM iceberg.default."sales_data$snapshots"
ORDER BY committed_at DESC;
```

---

## Configuration

All tunable values live in `config/maintenance_config.toml`:

```toml
[iceberg]
catalog_name = "nessie"
warehouse_path = "s3://warehouse/data"
target_table = "nessie.default.sales_data"

[compaction]
target_file_size_bytes = 134217728  # 128 MB
min_input_files = 5

[snapshot_expiry]
max_snapshot_age_days = 7
min_snapshots_to_keep = 2

[orphan_cleanup]
older_than_days = 3       # safety window — orphans younger than this are kept

[schedule]
cron = "0 2 * * *"        # Every night at 2 AM

[report]
output_path = "/opt/airflow/reports"
```

> **Note on orphan files:** After compaction, old Parquet files remain physically in MinIO for `older_than_days` days. This is by design — long-running queries may still be reading them. The MinIO `data/` folder will always contain more files than `$files` shows until the retention window passes.

---

## Project Structure

```
iceberg-maintenance/
├── Dockerfile.airflow          # Airflow + Java 17 + PySpark 3.5.3
├── Dockerfile.superset         # Superset + trino[sqlalchemy] driver
├── docker-compose.yaml         # All 9 services
├── Makefile                    # Developer commands
├── config/
│   └── maintenance_config.toml # All tunable pipeline settings
├── airflow-volumes/
│   └── dags/
│       ├── iceberg_maintenance_dag.py   # DAG definition + _SPARK_SUBMIT template
│       └── utils/
│           └── health_report.py         # JSON report generator
├── spark-jobs/
│   ├── compaction_job.py       # CALL nessie.system.rewrite_data_files(...)
│   ├── snapshot_expiry_job.py  # CALL nessie.system.expire_snapshots(...)
│   └── orphan_cleanup_job.py   # CALL nessie.system.remove_orphan_files(...)
├── scripts/
│   └── generate_sample_data.py # Seed script (10,000 rows, 20 small files)
├── trino-conf/
│   └── catalog/iceberg.properties  # Trino → Nessie + MinIO config
└── superset-init/
    └── init.sh                 # Auto-registers Trino connection in Superset
```

---

## How It Was Built

This project solved several non-obvious integration challenges:

- **Spark runs inside the Airflow container** in `local[*]` mode — this avoids Python version conflicts between Airflow (Python 3.12) and standalone Spark workers.
- **Project Nessie replaced a REST catalog server** — the `tabulario/iceberg-rest` image connected to real AWS S3 instead of MinIO due to an unfixable environment variable naming bug. Nessie is cleaner: the server stores only snapshot pointers and never needs S3 access.
- **Trino uses Nessie API v1** (`/api/v1`) because Trino 442 bundles `nessie-client:0.71.1` which predates the v2 API. Spark uses `/api/v2` because `iceberg-spark-runtime:1.5.0` bundles `nessie-client:0.76.6`.
- **GC must be explicitly enabled** — Nessie sets `gc.enabled=false` by default to protect data across Git-style branches. Set `'gc.enabled'='true'` on any table before running snapshot expiry or orphan cleanup.
- **MinIO requires path-style access** — `s3.path-style-access=true` in Trino and `fs.s3a.path.style.access=true` in Spark.
## Photos

**After enter seed:**

<img width="997" height="499" alt="image" src="https://github.com/user-attachments/assets/956e0096-3c21-4923-b356-607b9bb6ad3f" />

**Befor run trigger:**

<img width="1234" height="569" alt="image" src="https://github.com/user-attachments/assets/256bcff5-8b33-4c35-87ca-443e6e82b1bf" />

<img width="1027" height="531" alt="image" src="https://github.com/user-attachments/assets/a9894958-7b1a-4c3b-a763-a11ddcd2399b" />

**Report:**

<img width="914" height="393" alt="image" src="https://github.com/user-attachments/assets/163ab755-e9fd-434c-98d9-831718411441" />






