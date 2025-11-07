from airflow import DAG
import os
from airflow.providers.standard.operators.python import PythonOperator
import sys
from scripts.process_csv import minio_retrieve, transform_data, ingest_data

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)


with DAG(
        dag_id="billing_etl_minio_postgres_folder",
        start_date='@daily',
        catchup=False,
        tags=["billing"],
):
    def task_minio_retrieve():
        minio_retrieve('upload')


    def task_transform_data():
        transform_data()


    def task_ingest_data():
        ingest_data()


    minio_task = PythonOperator(
        task_id="minio_retrieve",
        python_callable=task_minio_retrieve,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=task_transform_data
    )

    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=task_ingest_data
    )

    minio_task >> transform_task >> ingest_task
