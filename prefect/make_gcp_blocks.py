from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket
from pathlib import Path

# alternative to creating GCP blocks in the UI
# copy your own service_account_info dictionary which is in the json file you downloaded from google
# IMPORTANT - do not store credentials in a publicly available repository!

# Replace wity your own bucket_name and service_account_file
bucket_name = "kaggle_data_lake_lithe-breaker-385610"  # insert your  GCS bucket name
service_account_file = Path("~/.google/credentials/lithe-breaker-385610-e22a767c96f3.json")  # insert your  GCS credentials file

credentials_block = GcpCredentials(
    service_account_file=service_account_file  # the json file with service account credentials
)
credentials_block.save("de-project-gcs-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials=GcpCredentials.load("de-project-gcs-creds"),
    bucket=bucket_name  
)

bucket_block.save("de-project-gcs", overwrite=True)
