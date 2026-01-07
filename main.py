import csv
import os
from google.cloud import storage
import pymysql


# Environment variables
BUCKET_NAME = os.environ["BUCKET_NAME"]
CSV_FILE_NAME = os.environ["CSV_FILE_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_NAME = os.environ["DB_NAME"]
INSTANCE_CONNECTION_NAME = os.environ["INSTANCE_CONNECTION_NAME"]

def main():
    # 1. Download CSV from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(CSV_FILE_NAME)
    csv_data = blob.download_as_text()

    # 2. Connect to Cloud SQL
    conn = pymysql.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        unix_socket=f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
        database=DB_NAME
    )

    cursor = conn.cursor()

    # 3. Read CSV and insert
    reader = csv.DictReader(csv_data.splitlines())
    insert_sql = "INSERT INTO users (name, email) VALUES (%s, %s)"

    for row in reader:
        cursor.execute(insert_sql, (row["name"], row["email"]))

    conn.commit()
    cursor.close()
    conn.close()

    print("CSV data inserted successfully")

if __name__ == "__main__":
    main()
