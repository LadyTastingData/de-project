from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from kaggle.api.kaggle_api_extended import KaggleApi
import zipfile

@task(retries=3)
def fetch_kaggle_data(kaggle_user: str, dataset_name: str, download_path: str) -> str:
    """Get Video games sales data"""
    api = KaggleApi()
    api.authenticate()
    kaggle_file_url = f"{kaggle_user}/{dataset_name}"
    api.dataset_download_files(kaggle_file_url, path = download_path)
    #zipped_file=Path(f"{download_path}/{dataset_name}.zip")
    zipped_file = download_path + '/' + dataset_name + '.zip'
    return zipped_file

@task()
def unzip_kaggle_data(zipped_file: str, download_path: str) -> None: 
    with zipfile.ZipFile(zipped_file, 'r') as zip_ref:
        zip_ref.extractall(download_path)  

@task(retries=3)
def read_data(download_path: str, data_file: str) -> pd.DataFrame:
    csv_file = download_path + '/' + data_file + '.csv'
    df = pd.read_csv(csv_file)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
     df["Year_of_Release"] = pd.to_datetime(df["Year_of_Release"],format="%Y")
     df["year"] = pd.DatetimeIndex(df["Year_of_Release"]).year
     df["Year_of_Release"] = df["Year_of_Release"].astype('Int64')
     df["Critic_Count"] = df["Critic_Count"].astype('Int64')
     df["User_Count"] = df["User_Count"].astype('Int64')
     print(df.head(2))
     print(f"columns: {df.dtypes}")
     print(f"rows: {len(df)}")
     return df


@task()
def write_local(df: pd.DataFrame, download_path: str, data_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    path=Path(f"{download_path}/{data_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcp_cloud_storage_bucket_block = GcsBucket.load("de-project-gcs")
    gcp_cloud_storage_bucket_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(kaggle_user, dataset_name, download_path, data_file) -> None:
    """The main ETL function"""
    #kaggle_user="sidtwr"
    #dataset_name="videogames-sales-dataset"
    #download_path="data"
    #data_file="Video_Games_Sales_as_at_22_Dec_2016"
    zipped_file=fetch_kaggle_data(kaggle_user, dataset_name, download_path)
    unzip_kaggle_data(zipped_file, download_path)
    df=read_data(download_path, data_file)
    df_clean = clean(df)
    path = write_local(df_clean, download_path, data_file)
    write_gcs(path)

@flow()
def etl_parent_flow(kaggle_user: str = "sidtwr", dataset_name: str = "videogames-sales-dataset", download_path: str = "data", data_file: str = "Video_Games_Sales_as_at_22_Dec_2016"):
    etl_web_to_gcs(kaggle_user, dataset_name, download_path, data_file)


if __name__ == "__main__":
    kaggle_user = "sidtwr"
    dataset_name = "videogames-sales-dataset"
    download_path = "data"
    data_file = "Video_Games_Sales_as_at_22_Dec_2016"
    etl_parent_flow(kaggle_user, dataset_name, download_path, data_file)

