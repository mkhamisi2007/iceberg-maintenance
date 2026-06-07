"""Helper wrapper around the compaction Spark job (for testing/reuse)."""

from __future__ import annotations

import subprocess


def run_compaction(spark_master: str = "spark://spark-master:7077") -> int:
    """Submit the compaction Spark job and return its exit code."""
    cmd = [
        "spark-submit",
        "--master", spark_master,
        "--packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "/opt/spark-jobs/compaction_job.py",
    ]
    result = subprocess.run(cmd, check=False)
    return result.returncode
