#!/usr/bin/env bash
# Bootstrap script: copies .env.example → .env and starts the stack.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# ── 1. Create .env from example if not present ────────────────────────────
if [[ ! -f .env ]]; then
    cp .env.example .env
    echo "[setup] Created .env from .env.example — edit secrets before production use."
else
    echo "[setup] .env already exists, skipping copy."
fi

# ── 2. Create placeholder dirs Airflow needs (prevent mount errors) ────────
mkdir -p airflow-volumes/{logs,plugins,config}
touch reports/.gitkeep

# ── 3. Bring up the full stack ─────────────────────────────────────────────
echo "[setup] Starting all services..."
docker compose up -d --build

# ── 4. Wait for Airflow webserver to be healthy ───────────────────────────
echo "[setup] Waiting for Airflow webserver (this may take ~60 s)..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8080/health | grep -q '"status": "healthy"'; then
        echo "[setup] Airflow is healthy."
        break
    fi
    sleep 4
done

echo ""
echo "============================================================"
echo " Airflow UI  → http://localhost:8080  (admin / admin)"
echo " Spark UI    → http://localhost:8081"
echo " MinIO UI    → http://localhost:9001  (minioadmin / minioadmin123)"
echo "============================================================"
echo ""
echo "[setup] To seed sample data, run:"
echo "  docker compose exec spark-master spark-submit /opt/spark-jobs/../scripts/generate_sample_data.py"
