import csv
import os

import pymysql
from pymysql.err import IntegrityError
from google.cloud import storage
from google.cloud import secretmanager


def get_secret(secret_id):
    # Automatically provided by Cloud Run / Cloud Run Job
    project_id = os.environ["GOOGLE_CLOUD_PROJECT"]

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"

    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def main():
    # Non-sensitive config (env vars)
    bucket_name = os.environ["BUCKET_NAME"]
    csv_file_name = os.environ["CSV_FILE_NAME"]
    db_name = os.environ["DB_NAME"]
    instance_connection_name = os.environ["INSTANCE_CONNECTION_NAME"]

    # Secrets
    db_user = get_secret("db-user")
    db_password = get_secret("db-password")

    # 1. Download CSV from GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(csv_file_name)
    csv_data = blob.download_as_text()

    # 2. Connect to Cloud SQL
    connection = pymysql.connect(
        user=db_user,
        password=db_password,
        unix_socket=f"/cloudsql/{instance_connection_name}",
        database=db_name
    )

    cursor = connection.cursor()

    # 3. Insert CSV data with duplicate handling
    insert_sql = "INSERT INTO users (name, email) VALUES (%s, %s)"
    reader = csv.DictReader(csv_data.splitlines())

    inserted = 0
    skipped = 0

    for row in reader:
        try:
            cursor.execute(insert_sql, (row["name"], row["email"]))
            inserted += 1
        except IntegrityError as e:
            # MySQL duplicate key error code
            if e.args[0] == 1062:
                skipped += 1
                print(f"Duplicate email skipped: {row['email']}")
            else:
                raise  # unknown DB error → fail job

    connection.commit()
    cursor.close()
    connection.close()

    print(f"Job finished. Inserted={inserted}, Duplicates skipped={skipped}")


if __name__ == "__main__":
    main()
