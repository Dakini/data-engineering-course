import io
import os
import requests
import pandas as pd
from google.cloud import storage

BUCKET = os.environ.get("GCP_GCS_BUCKET", "taxi-db")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


init_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"


def web_to_gcs(year, service):
    for i in range(12):
        print(i)
        # sets the month part of the file_name string
        month = "0" + str(i + 1)
        month = month[-2:]

        # csv file_name
        file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # read it back into a parquet file
        df = pd.read_csv(file_name, compression="gzip")
        print("rawr")
        df["PUlocationID"] = df.PUlocationID.astype("Int64")
        df["DOlocationID"] = df.DOlocationID.astype("Int64")
        df["SR_Flag"] = df.SR_Flag.astype("Int64")

        file_name = file_name.replace(".csv.gz", ".parquet")
        df.to_parquet(file_name, engine="pyarrow")
        # print(f"Parquet: {file_name}")

        # # upload it to gcs
        # upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        # print(f"GCS: {service}/{file_name}")


web_to_gcs("2019", "fhv")
