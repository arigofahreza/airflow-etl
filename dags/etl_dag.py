from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pathlib import Path
from scripts.process_csv import process_files

with DAG(
    dag_id="billing_etl_minio",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["billing", "minio"],
):

    def task_process_csv():
        output_path = Path("/opt/airflow/output/billing_output.csv")

        process_files(
            file1="STT1.csv",   # Only the object name in MinIO
            file2="STT2.csv",
            output_path=output_path
        )

    process_task = PythonOperator(
        task_id="process_csv_from_minio",
        python_callable=task_process_csv,
    )

    process_task
