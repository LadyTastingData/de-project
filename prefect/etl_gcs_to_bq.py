from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

# Replace with your own project ID
project_id = "lithe-breaker-385610"

@task(retries=3)
def extract_from_gcs(download_path: str, data_file: str) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"{download_path}/{data_file}.parquet"
    gcs_block = GcsBucket.load("de-project-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"pre: missing Critic Count: {df['Critic_Count'].isna().sum()}")
    df["Critic_Count"].fillna(0, inplace=True)
    print(f"post: missing Critic Count: {df['Critic_Count'].isna().sum()}")
    print(f"pre: missing User Count: {df['User_Count'].isna().sum()}")
    df["User_Count"].fillna(0, inplace=True)
    print(f"post: missing User Count: {df['User_Count'].isna().sum()}")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("de-project-gcs-creds")

    df.to_gbq(
        destination_table="games_data.games",
        project_id=project_id,
        credentials=gcp_credentials_block.get_credentials_from_service_account()
    )
        #chunksize=500_000,
        #if_exists="append",

@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    download_path = "data"
    data_file = "Video_Games_Sales_as_at_22_Dec_2016"

    path = extract_from_gcs(download_path,data_file)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()