"""Airflow DAG for the churn prediction pipeline."""

import os
import subprocess
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
PIPELINE_TASK_SCRIPT = os.path.join(PROJECT_ROOT, "src", "pipeline_tasks.py")

STEP_CONFIGS = [
    ("data_ingestion", "Data Ingestion", "Fetch data from multiple sources"),
    ("raw_data_storage", "Raw Data Storage", "Organize and catalog raw data"),
    ("data_validation", "Data Validation", "Validate data quality and generate reports"),
    ("data_preparation", "Data Preparation", "Clean and preprocess data"),
    ("data_transformation", "Data Transformation", "Feature engineering and transformation"),
    ("feature_store", "Feature Store", "Manage engineered features in the feature store"),
    ("data_versioning", "Data Versioning", "Version control for datasets with DVC"),
    ("model_building", "Model Building", "Train the machine learning model"),
]

dag = DAG(
    "churn_prediction_pipeline",
    default_args={
        "owner": "data-team",
        "depends_on_past": False,
        "start_date": datetime(2025, 8, 24),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "catchup": False,
    },
    description="Complete churn prediction pipeline",
    schedule=timedelta(hours=6),
    max_active_runs=1,
    tags=["churn", "ml", "pipeline", "sequential"],
)


def run_pipeline_step(step_key: str, task_name: str, **context):
    """Execute a registered pipeline step through the shared CLI."""
    print(f"Starting {task_name}...")

    env = os.environ.copy()
    env["PYTHONFAULTHANDLER"] = "true"
    env["MPLBACKEND"] = "Agg"
    env["PYTHONPATH"] = f"{PROJECT_ROOT}:{PROJECT_ROOT}/src"

    try:
        result = subprocess.run(
            [sys.executable, PIPELINE_TASK_SCRIPT, "--step", step_key],
            cwd=PROJECT_ROOT,
            env=env,
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )

        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())

        print(f"{task_name} completed successfully!")
        if result.stdout:
            print(result.stdout[-800:])
        return {"status": "success", "output": result.stdout}

    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"{task_name} timed out after 5 minutes") from exc


def pipeline_success(**context):
    """Final success callback."""
    print("Complete Churn Prediction Pipeline finished!")
    print("All tasks executed successfully!")
    return "Pipeline Success"


pipeline_tasks = {}
for step_key, task_name, doc_md in STEP_CONFIGS:
    pipeline_tasks[step_key] = PythonOperator(
        task_id=step_key,
        python_callable=run_pipeline_step,
        op_kwargs={"step_key": step_key, "task_name": task_name},
        dag=dag,
        doc_md=doc_md,
    )

task_pipeline_success = PythonOperator(
    task_id="pipeline_success",
    python_callable=pipeline_success,
    dag=dag,
    doc_md="Final success notification",
)

ordered_tasks = [pipeline_tasks[step_key] for step_key, _, _ in STEP_CONFIGS]
for upstream, downstream in zip(ordered_tasks, ordered_tasks[1:]):
    upstream >> downstream

ordered_tasks[-1] >> task_pipeline_success
