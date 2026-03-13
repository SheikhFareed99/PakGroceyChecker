import boto3
import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from botocore.exceptions import ClientError

load_dotenv()

conn = psycopg2.connect(
    dbname="pakgrocery",
    user="postgres",
    password="Snsfan@11",
    host="db.ycaxonvcibqvkhisfevw.supabase.co",
    port="5432"
)
cursor = conn.cursor()

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
ENDPOINT   = os.getenv("ENDPOINT")
BUCKET     = os.getenv("BUCKET")

s3 = boto3.client(
    "s3",
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    endpoint_url=ENDPOINT
)

days_list = [0, 15, 30, 90, 180, 365]  

all_dfs = []

cursor.execute("DELETE FROM date_compare_table")
conn.commit()

for days in days_list:
    key_name = f"raw/file_{(datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')}.csv"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key_name)
        df = pd.read_csv(obj["Body"])
        all_dfs.append(df)
        print(f"Fetched: {key_name}")
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"File not found, skipping: {key_name}")
        else:
            print(f"Error fetching {key_name}: {e}")

if all_dfs:
    combined_df = pd.concat(all_dfs, ignore_index=True)
    combined_df.to_csv("combined_data.csv", index=False)
    insert_query_history = """
        INSERT INTO date_compare_table (name, price, category, store_name, unit, date)
        VALUES %s
    """
    execute_values(cursor, insert_query_history, combined_df.values.tolist(), page_size=500)
    conn.commit()
    print("Inserted combined data into PostgreSQL.")
else:
    print("No CSV files found to insert.")

cursor.close()
conn.close()
