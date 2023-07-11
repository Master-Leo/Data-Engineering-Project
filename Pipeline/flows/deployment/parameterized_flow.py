import os
import sys
import pandas as pd
from pathlib import Path 
from census import Census 
from us import states
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket 
from random import randint 
from prefect.tasks import task_input_hash

# pwd = os.path.join(os.getcwd(), '../../')
# sys.path.append(pwd)
from config import api_key


@task(log_prints=True, tags=['extract'])
def extract_data(year: int, state: str, api_key: str):
    # Create Census object
    c = Census(api_key, year=year)
    variables = [
        'NAME',  # City name
        'B01003_001E',  # Total population
        'B01002_001E',  # Median age
        'B19013_001E',  # Median household income
        'B19301_001E',  # Per capita income
        'B17001_002E',  # Poverty count
        'B23025_005E',  # Unemployment count
        'B23025_004E',  # Employment count      
    ]
    variables_two = [
        'NAME',
        'B01003_001E',
        'B01002_001E',
        'B19013_001E',
        'B19301_001E',
        'B17001_002E',
        'B23025_005E',
        'B23025_004E']
    
    variables_three = [
        'NAME',
        'B01003_001E',
        'B01002_001E',
        'B19013_001E',
        'B19301_001E',
        'B17001_002E',
        'B23025_005E',
        'B23025_004E']
    state_data = c.acs5.get(
        variables, 
        {'for': 'state:*'}
    )

    state_code = states.lookup(state).fips

    city_data = c.acs5.state_place(
        variables_two,
        state_code,
        Census.ALL,
    )

    zip_data = c.acs5.state_zipcode(
        variables_three,
        state_fips=state_code,
        zcta='*',
    )

    # Convert to DataFrame
    state_df = pd.DataFrame(state_data)
    city_df = pd.DataFrame(city_data)
    zip_code_df = pd.DataFrame(zip_data)

    state_df['year']=year
    city_df['year']=year
    zip_code_df['year']=year

    state_columns = {'year': 'year',
                        'NAME': 'name',
                        'state': 'state',
                        'B01003_001E': 'population',
                        'B01002_001E': 'median_age',
                        'B19013_001E': 'household_income',
                        'B19301_001E': 'per_capita_income',
                        'B17001_002E': 'poverty_count',
                        'B23025_005E': 'unemployment_count',
                        'B23025_004E': 'employment_count'}
    
    city_columns = {'year': 'year',
                        'NAME': 'city',
                        'B01003_001E': 'population',
                        'B01002_001E': 'median_age',
                        'B19013_001E': 'household_income',
                        'B19301_001E': 'per_capita_income',
                        'B17001_002E': 'poverty_count',
                        'B23025_005E': 'unemployment_count',
                        'B23025_004E': 'employment_count'}

    zip_columns = {'year': 'year',
                        'NAME': 'zip_code',
                        'B01003_001E': 'population',
                        'B01002_001E': 'median_age',
                        'B19013_001E': 'household_income',
                        'B19301_001E': 'per_capita_income',
                        'B17001_002E': 'poverty_count',
                        'B23025_005E': 'unemployment_count',
                        'B23025_004E': 'employment_count'}
    
    # Column Reordering
    state_df = state_df.rename(columns=state_columns)
    city_df = city_df.rename(columns=city_columns)
    zip_code_df = zip_code_df.rename(columns=zip_columns)
    
    return state_df, city_df, zip_code_df

@task(log_prints=True)
def transform_data(state_df: pd.DataFrame, city_df: pd.DataFrame, zip_code_df : pd.DataFrame) -> pd.DataFrame:

    state_df = state_df[['year', 'name', 'population', 'median_age', 'household_income', 'per_capita_income',
                         'poverty_count', 'unemployment_count', 'employment_count']]
    city_df = city_df[['year', 'city', 'population', 'median_age', 'household_income', 'per_capita_income',
                       'poverty_count', 'unemployment_count', 'employment_count']]
    zip_code_df = zip_code_df[['year', 'zip_code', 'population', 'median_age', 'household_income', 'per_capita_income',
                               'poverty_count', 'unemployment_count', 'employment_count']]

    city_df.insert(1, 'state', city_df['city'].str.split(',', expand=True)[1])
    city_df['city'] = city_df['city'].str.split(',', expand=True)[0]

    print(state_df.head())
    print(city_df.head())
    print(state_df.head())

    print(f'Columns: {state_df.dtypes}')
    print(f'Rows: {len(state_df)}')

    print(f'Columns: {city_df.dtypes}')
    print(f'Rows: {len(city_df)}')

    print(f'Columns: {zip_code_df.dtypes}')
    print(f'Rows: {len(zip_code_df)}')

    return state_df, city_df, zip_code_df




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

