#!/bin/bash
set -e

echo "[Superset] Upgrading metadata database..."
superset db upgrade

echo "[Superset] Creating admin user..."
superset fab create-admin \
    --username  "${SUPERSET_ADMIN_USERNAME:-admin}" \
    --firstname "Admin" \
    --lastname  "User" \
    --email     "${SUPERSET_ADMIN_EMAIL:-admin@superset.com}" \
    --password  "${SUPERSET_ADMIN_PASSWORD:-admin}" 2>/dev/null \
  || echo "[Superset] Admin user already exists, skipping."

echo "[Superset] Loading default roles and permissions..."
superset init

echo "[Superset] Registering Trino → Iceberg database connection..."
python3 << 'PYEOF'
import sys

try:
    from superset import create_app

    app = create_app()
    with app.app_context():
        from superset.models.core import Database
        from superset.extensions import db

        name = "Iceberg via Trino"
        uri  = "trino://admin@trino:8080/iceberg"

        existing = db.session.query(Database).filter_by(database_name=name).first()
        if not existing:
            conn = Database(
                database_name=name,
                sqlalchemy_uri=uri,
                expose_in_sqllab=True,
                allow_run_async=False,
            )
            db.session.add(conn)
            db.session.commit()
            print(f"[Superset] '{name}' connection created — URI: {uri}")
        else:
            # Update existing connection to ensure async is disabled
            existing.allow_run_async = False
            db.session.commit()
            print(f"[Superset] '{name}' connection already exists, updated.")

except Exception as exc:
    print(f"[Superset] WARNING: could not auto-register database: {exc}", file=sys.stderr)
    print("[Superset] Add it manually: Settings → Database Connections → +Database → Trino", file=sys.stderr)
PYEOF

echo "[Superset] Initialisation complete."
