from __future__ import annotations

import subprocess
import sys
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path("/opt/project")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
AIRFLOW_ROOT = PROJECT_ROOT / "airflow"
if str(AIRFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_ROOT))

from plugins.operators.dbt_build_operator import DbtBuildOperator
from src.ops.run_logger import log_pipeline_task


def run_script(script_rel_path: str, task_name: str) -> None:
    run_id = str(uuid.uuid4())
    script_path = PROJECT_ROOT / script_rel_path
    try:
        subprocess.run(["python", str(script_path)], check=True)
        try:
            log_pipeline_task(run_id=run_id, task_name=task_name, status="success")
        except Exception:
            # Do not fail task execution if observability logging fails.
            pass
    except Exception:
        try:
            log_pipeline_task(run_id=run_id, task_name=task_name, status="failed")
        except Exception:
            pass
        raise


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="cmapss_pipeline",
    default_args=default_args,
    description="NASA C-MAPSS medallion pipeline",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule_interval=None,
) as dag:
    validate_layout = PythonOperator(
        task_id="validate_layout",
        python_callable=run_script,
        op_kwargs={
            "script_rel_path": "src/ingestion/validate_layout.py",
            "task_name": "validate_layout",
        },
        retries=0,
    )

    build_manifest = PythonOperator(
        task_id="build_manifest",
        python_callable=run_script,
        op_kwargs={
            "script_rel_path": "src/ingestion/build_manifest.py",
            "task_name": "build_manifest",
        },
        retries=0,
    )

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=run_script,
        op_kwargs={
            "script_rel_path": "src/ingestion/load_bronze.py",
            "task_name": "load_bronze",
        },
    )

    dbt_build = DbtBuildOperator(
        task_id="dbt_build",
        project_dir=str(PROJECT_ROOT),
        command="build",
        profiles_dir=str(PROJECT_ROOT / "profiles"),
    )

    dbt_docs = DbtBuildOperator(
        task_id="dbt_docs",
        project_dir=str(PROJECT_ROOT),
        command="docs generate",
        profiles_dir=str(PROJECT_ROOT / "profiles"),
    )

    publish_views = PythonOperator(
        task_id="publish_views",
        python_callable=run_script,
        op_kwargs={
            "script_rel_path": "src/ops/publish_gold.py",
            "task_name": "publish_views",
        },
    )

    validate_layout >> build_manifest >> load_bronze >> dbt_build >> dbt_docs >> publish_views
