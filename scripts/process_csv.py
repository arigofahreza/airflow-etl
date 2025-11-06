import pandas as pd
from io import BytesIO
from minio import Minio
from sqlalchemy import create_engine
import os

from utils.helpers import validate_row


def read_csv_from_minio(minio_client, bucket, object_name):
    response = minio_client.get_object(bucket, object_name)
    data = response.read()
    return pd.read_csv(BytesIO(data))


def process_files(file1, file2):
    # Connect to MinIO
    minio_client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )

    bucket = os.getenv("MINIO_BUCKET_NAME")

    # Read CSVs from MinIO
    df1 = read_csv_from_minio(minio_client, bucket, file1)
    df2 = read_csv_from_minio(minio_client, bucket, file2)

    # Merge files and prioritize STT2 over STT1
    df = pd.concat([df1, df2], ignore_index=True)
    df = df.drop_duplicates(subset=["stt_number"], keep="last")

    # Validate rows (drop incomplete data)
    df = df[df.apply(validate_row, axis=1)]

    # Aggregation
    debit = df[df['client_type'] == 'C'].groupby(['date', 'client_code']) \
        .agg(stt_count=('stt_number', 'count'),
             debit=('amount', 'sum')) \
        .reset_index()

    credit = df[df['client_type'] == 'V'].groupby(['date', 'client_code']) \
        .agg(stt_count=('stt_number', 'count'),
             credit=('amount', 'sum')) \
        .reset_index()

    final = pd.merge(debit, credit, on=['date', 'client_code'], how='outer')

    # Insert to Postgres
    postgres_engine = create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}"
        f"@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    )

    final.to_sql(
        name="billing_table",
        con=postgres_engine,
        if_exists="append",      # insert / append
        index=False
    )

    print("✅ Billing result inserted into PostgreSQL")
