.DEFAULT_GOAL := help

SPARK_PACKAGES := org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262

# ─────────────────────────────────────────────────────────────────────────────
.PHONY: help
help:
	@echo ""
	@echo "  Iceberg Maintenance — available commands"
	@echo ""
	@echo "  make up       Start all services (copies .env if missing)"
	@echo "  make down     Stop and remove all containers + volumes"
	@echo "  make seed     Inject 10 000 rows of sample sales data"
	@echo "  make ui       Open Airflow, Spark, MinIO, Trino and Superset in the browser"
	@echo "  make logs     Tail logs from all services"
	@echo "  make status   Show container health"
	@echo "  make trigger  Manually trigger the maintenance DAG"
	@echo "  make report   Print the latest health report JSON"
	@echo ""

# ─────────────────────────────────────────────────────────────────────────────
.PHONY: up
up:
	@if [ ! -f .env ]; then \
		cp .env.example .env; \
		echo "[make] Created .env from .env.example"; \
	fi
	@mkdir -p airflow-volumes/{logs,plugins,config} reports
	docker compose up -d --build
	@echo ""
	@echo "[make] Waiting for Airflow webserver to become healthy..."
	@for i in $$(seq 1 40); do \
		if docker compose exec -T airflow-webserver curl -sf http://localhost:8080/health 2>/dev/null | grep -q healthy; then \
			echo "[make] Airflow is healthy."; \
			break; \
		fi; \
		printf "."; \
		sleep 4; \
	done
	@echo ""
	@echo "  Airflow   → http://localhost:8080  (admin / admin)"
	@echo "  Spark     → http://localhost:8081"
	@echo "  Trino     → http://localhost:8082"
	@echo "  MinIO     → http://localhost:9001  (minioadmin / minioadmin123)"
	@echo "  Superset  → http://localhost:8088  (admin / admin)"
	@echo ""

# ─────────────────────────────────────────────────────────────────────────────
.PHONY: down
down:
	docker compose down -v
	@echo "[make] All containers and volumes removed."

# ─────────────────────────────────────────────────────────────────────────────
.PHONY: seed
seed:
	@echo "[make] Submitting generate_sample_data.py via Airflow container (local mode)..."
	docker compose exec \
		-e PYSPARK_PYTHON=python3 \
		-e PYSPARK_DRIVER_PYTHON=python3 \
		airflow-webserver \
		/home/airflow/.local/bin/spark-submit \
		--master local[*] \
		--packages $(SPARK_PACKAGES) \
		--conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
		--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
		--conf spark.hadoop.fs.s3a.access.key=minioadmin \
		--conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
		--conf spark.hadoop.fs.s3a.path.style.access=true \
		--conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
		--conf "spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog" \
		--conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v2 \
		--conf spark.sql.catalog.nessie.ref=main \
		--conf spark.sql.catalog.nessie.warehouse=s3://warehouse/data \
		/opt/scripts/generate_sample_data.py
	@echo "[make] Data injection complete."

# ─────────────────────────────────────────────────────────────────────────────
.PHONY: ui
ui:
	@echo "[make] Opening all UIs in browser..."
	open http://localhost:8080
	open http://localhost:8081
	open http://localhost:8082
	open http://localhost:9001
	open http://localhost:8088

# ─────────────────────────────────────────────────────────────────────────────
.PHONY: logs
logs:
	docker compose logs -f

# ─────────────────────────────────────────────────────────────────────────────
.PHONY: status
status:
	docker compose ps

# ─────────────────────────────────────────────────────────────────────────────
.PHONY: trigger
trigger:
	@echo "[make] Triggering iceberg_table_maintenance DAG..."
	docker compose exec airflow-webserver \
		airflow dags trigger iceberg_table_maintenance
	@echo "[make] DAG triggered. Watch it at http://localhost:8080"

# ─────────────────────────────────────────────────────────────────────────────
.PHONY: report
report:
	@LATEST=$$(ls -t reports/maintenance_report_*.json 2>/dev/null | head -1); \
	if [ -z "$$LATEST" ]; then \
		echo "[make] No report found yet. Run the DAG first (make trigger)."; \
	else \
		echo "[make] Latest report: $$LATEST"; \
		cat "$$LATEST"; \
	fi
