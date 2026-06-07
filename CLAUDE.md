# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

```bash
make up       # Start all services (auto-copies .env.example → .env if missing)
make down     # Stop all containers and delete volumes
make seed     # Inject 10,000 sample rows into the Iceberg table
make trigger  # Manually trigger the maintenance DAG
make report   # Print the latest health report JSON
make ui       # Open all UIs in the browser
make logs     # Tail all container logs
make status   # Show container health
```

Always use `make seed` (not `docker compose exec spark-master ...`) — the seed command runs inside the Airflow container which has the correct Spark + Java environment.

After `make up`, the DAG starts **paused**. Unpause it before triggering:
```bash
docker compose exec airflow-webserver airflow dags unpause iceberg_table_maintenance
make trigger
```

## Architecture

The stack is fully Docker Compose. Every service runs inside the `lakehouse-net` bridge network.

**Data flow:**
```
Airflow Scheduler (cron 0 2 * * *)
  └─► BashOperator: spark-submit (runs inside airflow-webserver container, local[*] mode)
        └─► NessieCatalog (Iceberg catalog client, bundled in iceberg-spark-runtime JAR)
              └─► Nessie server (http://nessie:19120/api/v2) — stores commit metadata only
              └─► MinIO (http://minio:9000) — stores actual Parquet + metadata files
Trino (http://trino:8080) — queries Iceberg via Nessie catalog, reads S3 directly
Superset (http://superset:8088) — SQL Lab over trino://admin@trino:8080/iceberg
```

**Catalog: Project Nessie (not Hive Metastore)**
- Spark uses `NessieCatalog` via `/api/v2`. Table name prefix: `nessie.default.*`
- Trino uses `iceberg.catalog.type=nessie` via `/api/v1` (Trino bundles nessie-client 0.71.1 which only supports v1). Table name prefix in Trino: `iceberg.default.*`
- Nessie server needs **zero S3 access** — it only stores snapshot pointers. All file I/O goes directly from Spark/Trino to MinIO via S3A.

**Spark runs in `local[*]` mode inside the Airflow container**, not on the Spark master/worker containers. The Spark master/worker containers exist for UI reference only. `spark-submit` is at `/home/airflow/.local/bin/spark-submit` — BashOperator uses the full path because the Airflow shell doesn't inherit the user PATH.

**DAG pipeline** (`airflow-volumes/dags/iceberg_maintenance_dag.py`):
```
compaction → snapshot_expiry → orphan_cleanup → health_report
```
All three Spark tasks share the same `_SPARK_SUBMIT` prefix string defined once at the top of the DAG file. Config is read from `config/maintenance_config.toml` at DAG parse time.

## Key Configuration Files

| File | Purpose |
|---|---|
| `config/maintenance_config.toml` | All tunable values: table name, file size targets, retention windows, cron schedule |
| `trino-conf/catalog/iceberg.properties` | Trino → Nessie connection + MinIO S3 credentials |
| `.env` / `.env.example` | Service credentials (copied automatically by `make up`) |

The table must have `'gc.enabled'='true'` as a TBLPROPERTY for `snapshot_expiry` and `orphan_cleanup` to work. Nessie disables GC by default to protect data across branches.

## Critical Constraints

**Iceberg metadata tables in Trino** use `$` suffix notation with quotes:
```sql
SELECT * FROM iceberg.default."sales_data$files"
SELECT * FROM iceberg.default."sales_data$snapshots"
```

**Orphan cleanup has a 3-day safety window** (`older_than_days = 3`). Old data files remain physically in MinIO for 3 days after they become unreferenced. This is intentional — the MinIO `data/` folder will always contain more files than `$files` shows.

**Superset must use sync queries** (`allow_run_async=False`). There is no Celery worker — async mode causes "Failed to start remote query on a worker" errors.

**Spark packages** (downloaded to `~/.ivy2` on first run, cached on subsequent runs):
```
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0
org.apache.hadoop:hadoop-aws:3.3.4
com.amazonaws:aws-java-sdk-bundle:1.12.262
```

## Service URLs and Credentials

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Spark master UI | http://localhost:8081 | — |
| Trino UI | http://localhost:8082 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin123 |
| Nessie API | http://localhost:19120 | — |
| Superset | http://localhost:8088 | admin / admin |

## Adding a New Spark Job

1. Create `spark-jobs/your_job.py` — follow the pattern in `compaction_job.py`: read config from `/opt/airflow/config/maintenance_config.toml`, build SparkSession with NessieCatalog config, use `NESSIE_URI = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")`.
2. Add a `BashOperator` in the DAG using `_SPARK_SUBMIT + "/opt/spark-jobs/your_job.py"`.
3. Wire it into the task chain.

Do not add `rest.io-impl=org.apache.iceberg.aws.s3.S3FileIO` to Spark configs — the bundled JAR uses AWS SDK v1 (hadoop-aws) but S3FileIO requires SDK v2. Use `spark.hadoop.fs.s3a.*` properties instead.
