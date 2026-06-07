"""Generate a JSON health report after each maintenance pipeline run."""

from __future__ import annotations

import json
import os
from datetime import datetime


def generate_report(config: dict, **kwargs) -> str:
    """Write a JSON maintenance report to the configured output directory.

    Returns the absolute path of the written file.
    """
    report: dict = {
        "timestamp": datetime.now().isoformat(),
        "table": config["iceberg"]["target_table"],
        "tasks_executed": [
            "compaction",
            "snapshot_expiry",
            "orphan_cleanup",
        ],
        "status": "success",
        "config_summary": {
            "max_snapshot_age_days": config["snapshot_expiry"]["max_snapshot_age_days"],
            "orphan_cleanup_days": config["orphan_cleanup"]["older_than_days"],
            "target_file_size_mb": config["compaction"]["target_file_size_bytes"] // 1024 // 1024,
        },
    }

    output_dir: str = config["report"]["output_path"]
    os.makedirs(output_dir, exist_ok=True)

    filename = f"maintenance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w") as f:
        json.dump(report, f, indent=2)

    print(f"[HealthReport] Report saved to: {filepath}")
    print(json.dumps(report, indent=2))
    return filepath
