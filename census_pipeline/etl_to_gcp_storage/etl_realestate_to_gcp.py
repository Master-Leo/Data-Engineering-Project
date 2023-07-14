import os
import sys
import pandas as pd
import zipfile 
import datetime
import numpy as np
from pathlib import Path
import subprocess
from prefect import flow, task
from google.cloud import storage

from config import project_bucket 

@task(log_prints=True, tags=['Extract'])
def extract_real_estate_data() -> str:

    dataset_name = "ahmedshahriarsakib/usa-real-estate-dataset"
    command = f"kaggle datasets download -d {dataset_name}"
    subprocess.run(command, shell=True)


    zip_path = './usa-real-estate-dataset.zip'
    csv_folder = 'data/real_estate/'

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(csv_folder)

    os.remove(zip_path)
    csv_path = os.path.join(csv_folder, 'realtor-data.csv')
    return csv_path

@task(log_prints=True, tags=['Transformation'])
def transformation(csv_path: Path, years: list, states: list) -> pd.DataFrame:

    df = pd.read_csv(csv_path)
    print(df.shape)

    df['sold_date'] = pd.to_datetime(df['sold_date'], format='%Y-%m-%d')
    df = df[df['state'].isin(states)]

    years_as_dates = [datetime.datetime(year, 1, 1) for year in years]

    df = df[df['sold_date'].between(min(years_as_dates), max(years_as_dates), inclusive=True)]

    print(df.shape)
    print(df.dtypes)

    df[['house_size','bed','bath','zip_code']] = df[['house_size','bed','bath','zip_code']].fillna(0).astype(int)

    print(df.dtypes)
    print(df.isnull().sum())
    return df 

@task(log_prints=True, tags='Load_part_one')
def write_local(df: pd.DataFrame, dataset_file: str):
    path = Path(f'data/real_estate/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints=True, tags='Load_part_two')
def write_gcs(path: Path) -> None:
    bucket_name = project_bucket
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    
    # Construct the destination path in GCS
    destination = f"data/real_estate/{path.name}"
    
    # Upload the file to GCS
    gcs = bucket.blob(destination)
    gcs.upload_from_filename(str(path))

    os.remove('./data/real_estate/real_estate_data.parquet')
    return

@flow
def etl_real_estate_parent_flow() -> None:
    years = [2016,2017,2018,2019,2020,2021]
    states = ['California', 'Florida', 'New York', 'Texas', 'Pennsylvania']

    dataset_file = 'real_estate_data'
    csv_path = extract_real_estate_data()
    df = transformation(csv_path, years, states)
    path = write_local(df, dataset_file)
    write_gcs(path)

if __name__ == '__main__':
    etl_real_estate_parent_flow()
