import os
import sys
import pandas as pd
import zipfile 
import json
from datetime import datetime
import numpy as np
from pathlib import Path
import subprocess
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.blocks.system import Secret


@task(log_prints=True, tags=['Extraction'])
def extract_realestate_data() -> str:
    # subprocess.run(["kaggle", "datasets", "download", "-d", "ahmedshahriarsakib/usa-real-estate-dataset"], check=True)
    # subprocess.run(["unzip", "usa-real-estate-dataset.zip", "-d", "/opt/prefect/flows/data/real_estate/"], check=True)

    subprocess.run(["kaggle", "datasets", "download", "-d", "ahmedshahriarsakib/usa-real-estate-dataset"], check=True)
    subprocess.run(["unzip", "usa-real-estate-dataset.zip", "-d", "data/real_estate/"], check=True)

    # csv_path = os.path.join('/opt/prefect/flows/data/real_estate', 'realtor-data.csv')
    csv_path = os.path.join('data/real_estate', 'realtor-data.csv')

    return csv_path

@task(log_prints=True, tags=['Transformation'])
def transform_realestate_data(csv_path: Path, years: list, states: list) -> pd.DataFrame:

    df = pd.read_csv(csv_path)
    print(df.shape)

    df['sold_date'] = pd.to_datetime(df['sold_date'], format='%Y-%m-%d')
    df = df[df['state'].isin(states)]

    years_as_dates = [datetime(year, 1, 1) for year in years]

    df = df[df['sold_date'].between(min(years_as_dates), max(years_as_dates), inclusive=True)]

    print(df.shape)
    print(df.dtypes)

    df[['house_size','bed','bath','zip_code']] = df[['house_size','bed','bath','zip_code']].fillna(0).astype(int)

    print(df.dtypes)
    print(df.isnull().sum())
    os.remove(csv_path)
    return df 


@task(log_prints=True, tags='Load_to_data_lake_gcp')
def write_realestate_to_gcs(df: pd.DataFrame, dataset_file: str ) -> None:

    gcp_bucket = GcsBucket.load("project-bucket")
    now = datetime.now()
    date_string = now.strftime("%m_%d_%Y")

    dataset_file_with_date = f"{dataset_file}_{date_string}"

    # path = Path(f'/opt/prefect/flows/data/real_estate/{dataset_file_with_date}.parquet')
    path = Path(f'data/real_estate/{dataset_file_with_date}.parquet')


    df.to_parquet(path, compression='gzip')

    destination = str(path)
    relative_path = destination.split('data/')[1]
    final_destination = f'data/{relative_path}'
    gcp_bucket.upload_from_path(from_path=destination,to_path=final_destination)
    path.unlink()

    return

@flow
def etl_real_estate_parent_flow(years: list[int], states_list: list[str]) -> None:

    dataset_file = 'real_estate_data'

    csv_path = extract_realestate_data()
    df = transform_realestate_data(csv_path, years, states_list)
    write_realestate_to_gcs(df, dataset_file)

if __name__ == '__main__':
    years = [2021]
    states_list = ['California']
    etl_real_estate_parent_flow(years,states_list)

