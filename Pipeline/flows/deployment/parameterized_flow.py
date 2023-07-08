import os
import sys
import pandas as pd
from pathlib import Path 
from census import Census 
# from us import state
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket 
from random import randint 
from prefect.tasks import task_input_hash

# pwd = os.path.join(os.getcwd(), '../../')
# sys.path.append(pwd)
from config_02 import api_key


@task(log_prints=True, tags=['extract'])
def extract_data(year: int):
    # Create Census object
    c = Census(api_key, year=year)

    # Run Census Search to retrieve data on all states
    # Note the addition of "B23025_005E" for unemployment count
    census_data = c.acs5.get(("NAME", "B19013_001E", "B01003_001E", "B01002_001E",
                              "B19301_001E",
                              "B17001_002E",
                              "B23025_005E"), {'for': 'state:*'})

    # Convert to DataFrame
    df = pd.DataFrame(census_data)

    # Column Reordering
    df = df.rename(columns={"B01003_001E": "Population",
                                          "B01002_001E": "Median Age",
                                          "B19013_001E": "Household Income",
                                          "B19301_001E": "Per Capita Income",
                                          "B17001_002E": "Poverty Count",
                                          "B23025_005E": "Unemployment Count",
                                          "NAME": "Name", "state": "State"})
    df['Year'] = year

    return df

@task(log_prints=True)
def transform_data(df: pd.DataFrame):
    # Add in Poverty Rate (Poverty Count / Population)
    df['Poverty Rate'] = 100 * df['Poverty Count'].astype(int) / df['Population'].astype(int)

    # Add in Employment Rate (Employment Count / Population)
    df['Unemployment Rate'] = 100 * df['Unemployment Count'].astype(int) / df['Population'].astype(int)

    # Final DataFrame
    df = df[['Year','State', 'Name', 'Population', 'Median Age', 'Household Income',
                           'Per Capita Income', 'Poverty Count', 'Poverty Rate', 'Unemployment Rate']]
    print(df.head())
    print(f'Columns: {df.dtypes}')
    print(f'Rows: {len(df)}')
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, dataset_file: str) -> Path:
    'Writing DataFrame locally then as a parquet file'
    path = Path(f'data/economic/state/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path 

@task(log_prints=True)
def write_gcs(path: Path) -> None:
    gcp_block = GcsBucket.load("project-bucket")
    gcp_block.upload_from_path(from_path=path, to_path=path)
    return 

@flow()
def etl_api_to_gcs(year: int) -> None:
    dataset_file = f'{year}_economic_data'

    df = extract_data(year)
    df_clean = transform_data(df)
    path = write_local(df_clean, dataset_file)
    write_gcs(path)

@flow
def etl_parent_flow() -> None:
    years = list(range(2021, 2010, -1))
    for year in years:
        etl_api_to_gcs(year)

if __name__ == '__main__':
    etl_parent_flow.run()

