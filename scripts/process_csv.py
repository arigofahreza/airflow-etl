import pandas as pd
from io import BytesIO
from minio import Minio
from sqlalchemy import create_engine
import os

from utils.helpers import validate_row


def read_csv_from_minio(minio_client, bucket, object_name):
    """
    Read CSV from Minio and return DataFrame
    """
    response = minio_client.get_object(bucket, object_name)
    data = response.read()
    return pd.read_csv(BytesIO(data))


def process_folder(folder_name):
    # Connect MinIO
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )

    bucket = os.getenv("MINIO_BUCKET_NAME")

    # LIST semua file dalam folder
    objects = minio_client.list_objects(bucket, prefix=f"{folder_name}/", recursive=True)

    dfs = []
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            print(f"📥 Reading: {obj.object_name}")
            df = read_csv_from_minio(minio_client, bucket, obj.object_name)
            dfs.append(df)

    if not dfs:
        print("⚠ No CSV found in folder")
        return

    # Gabung semua CSV dalam folder (STT2 replace STT1 by stt_number)
    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["stt_number"], keep="last")

    # Drop incomplete rows
    df = df[df.apply(validate_row, axis=1)]

    # Debit (C)
    debit = df[df['client_type'] == 'C'].groupby(['date', 'client_code']) \
        .agg(stt_count=('stt_number', 'count'),
             debit=('amount', 'sum')) \
        .reset_index()

    # Credit (V)
    credit = df[df['client_type'] == 'V'].groupby(['date', 'client_code']) \
        .agg(stt_count=('stt_number', 'count'),
             credit=('amount', 'sum')) \
        .reset_index()

    final = pd.merge(debit, credit, on=['date', 'client_code'], how='outer')

    # Connect PostgreSQL
    postgres_engine = create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

    # Insert data to Postgres table
    final.to_sql(
        name="billing_table",
        con=postgres_engine,
        if_exists="append",
        index=False
    )

    print("✅ Billing result inserted to PostgreSQL successfully.")
