import numpy as np
import pandas as pd
from io import BytesIO
from minio import Minio
from pandas import DataFrame
from sqlalchemy import create_engine
import os

from utils.helpers import validate_row
from dotenv import load_dotenv


def minio_retrieve(folder_name: str, ti):
    load_dotenv()
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )

    bucket = os.getenv("MINIO_BUCKET_NAME")

    objects = minio_client.list_objects(bucket, prefix=f"{folder_name}/", recursive=True)

    dfs = []
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            print(f"Reading: {obj.object_name}")
            df = read_csv_from_minio(minio_client, bucket, obj.object_name)
            dfs.append(df)
    if not dfs:
        print("No CSV found in folder")
    ti.xcom_push(key="dfs", value=dfs)


def read_csv_from_minio(minio_client, bucket, object_name):
    """
    Read CSV from Minio and return DataFrame
    """
    response = minio_client.get_object(bucket, object_name)
    data = response.read()
    return pd.read_csv(BytesIO(data))


def ingest_data(ti):
    load_dotenv()
    postgres_engine = create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )
    data = ti.xcom_pull(task_ids="transform_data", key='final')
    data.to_sql(
        name="billing_table",
        con=postgres_engine,
        if_exists="append",
        index=False
    )
    print("Billing result inserted to PostgreSQL successfully.")


def transform_data(ti):
    datas = ti.xcom_pull(task_ids="minio_retrieve", key='dfs')
    df = pd.concat(datas, ignore_index=True)
    df = df.drop_duplicates(subset=["number"], keep="last")

    df = df[df.apply(validate_row, axis=1)]

    debit = df[df['client_type'] == 'C'].groupby(['date', 'client_code']) \
        .agg(stt_count=('number', 'count'),
             debit=('amount', 'sum')) \
        .reset_index()

    credit = df[df['client_type'] == 'V'].groupby(['date', 'client_code']) \
        .agg(stt_count=('number', 'count'),
             credit=('amount', 'sum')) \
        .reset_index()

    final = pd.merge(debit, credit, on=['date', 'client_code'], how='outer')
    final['stt_count'] = final['stt_count_x'].combine_first(final['stt_count_y'])
    final = final[['date', 'client_code', 'stt_count', 'debit', 'credit']]
    final = final.replace({np.nan: None})
    ti.xcom_push(key="final", value=final)


