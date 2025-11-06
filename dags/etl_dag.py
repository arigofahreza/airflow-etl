from airflow import DAG
from datetime import datetime

from airflow.providers.standard.operators.python import PythonOperator

from scripts.process_csv import process_folder

with DAG(
        dag_id="billing_etl_minio_postgres_folder",
        start_date=datetime(2025, 1, 1),
        catchup=False,
        tags=["billing"],
):
    def task_process_folder():
        process_folder(folder_name="upload")


    process_task = PythonOperator(
        task_id="process_minio_folder",
        python_callable=task_process_folder,
    )

    process_task
