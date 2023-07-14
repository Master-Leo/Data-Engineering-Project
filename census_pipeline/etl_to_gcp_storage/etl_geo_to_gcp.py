import os
import sys
import pandas as pd
from pathlib import Path 
from typing import List
from census import Census 
from us import states
from prefect import flow, task
from google.cloud import storage

from config import api_key, project_bucket
import tenacity

@tenacity.retry(
    wait=tenacity.wait_fixed(5),  # Wait for 5 seconds between retries
    stop=tenacity.stop_after_attempt(3),  # Retry for a maximum of 3 attempts
)
@task(log_prints=True, tags=['extract'])
def extract_geographic_data(year: int, state: str, api_key: str) -> pd.DataFrame: 

    c = Census(api_key, year=year)

    variables = [
        "NAME",  # Geographic name
        "B18101_001E",  # Disability status of the civilian noninstitutionalized population
        "B27001_001E",  # Health insurance coverage by type of coverage and age
        "B07013_001E",  # Geographical mobility in the past year for current residence
        "B08006_001E",  # Means of transportation to work by selected characteristics
        "B28002_001E",  # Presence and types of internet subscriptions in households
        "GEO_ID",  # Geographic identifier code
        # Add more social variables as per your requirements
    ]
    
    # Retrieve the state FIPS code
    state_code = states.lookup(state).fips

    # Perform the Census API query
    state_data = c.acs5.state(
        variables,
        state_code,
        year=year
    )

    city_data = c.acs5.state_place(
        variables,
        state_fips=state_code,
        place='*',
        year=year,
    )

    zip_data = c.acs5.state_zipcode(
        variables,
        state_fips=state_code,
        year=year,
        zcta='*',
    )

    # Create a pandas DataFrame from the retrieved data
    state_df = pd.DataFrame(state_data)
    city_df = pd.DataFrame(city_data)
    zip_code_df = pd.DataFrame(zip_data)

    state_df = state_df.drop(columns=['state'])
    state_df['year']=year
    city_df['year']=year
    zip_code_df['year']=year

    state_columns = {
        'year': 'year',
        'NAME': 'state',
        'B18101_001E': 'disability_status',
        'B27001_001E': 'health_insurance_coverage',
        'B07013_001E': 'geographical_mobility',
        'B08006_001E': 'means_of_transportation_to_work',
        'B28002_001E': 'internet_subscriptions_in_household',
        'GEO_ID': 'geographic_id'
    }

    city_columns = {
        'year': 'year',
        'NAME': 'city',
        'B18101_001E': 'disability_status',
        'B27001_001E': 'health_insurance_coverage',
        'B07013_001E': 'geographical_mobility',
        'B08006_001E': 'means_of_transportation_to_work',
        'B28002_001E': 'internet_subscriptions_in_household',
        'GEO_ID': 'geographic_id'
    }

    zip_columns = {
        'year': 'year',
        'NAME': 'zip_code',
        'B18101_001E': 'disability_status',
        'B27001_001E': 'health_insurance_coverage',
        'B07013_001E': 'geographical_mobility',
        'B08006_001E': 'means_of_transportation_to_work',
        'B28002_001E': 'internet_subscriptions_in_household',
        'GEO_ID': 'geographic_id'
    }

    state_df = state_df.rename(columns=state_columns)
    city_df = city_df.rename(columns=city_columns)
    zip_code_df = zip_code_df.rename(columns=zip_columns)
    
    return state_df, city_df, zip_code_df

@task(log_prints=True)
def transform_data(state_df: pd.DataFrame, city_df: pd.DataFrame, zip_code_df : pd.DataFrame) -> pd.DataFrame:
    
    state_df = state_df[['year','state', 'disability_status', 'health_insurance_coverage', 'geographical_mobility', 'means_of_transportation_to_work', 
                         'internet_subscriptions_in_household']]
    city_df = city_df[['year','city', 'disability_status', 'health_insurance_coverage', 'geographical_mobility', 'means_of_transportation_to_work', 
                       'internet_subscriptions_in_household']]
    zip_code_df = zip_code_df[['year','zip_code', 'disability_status', 'health_insurance_coverage', 'geographical_mobility', 'means_of_transportation_to_work', 
                               'internet_subscriptions_in_household']]

    city_df.insert(1, 'state', city_df['city'].str.split(',', expand=True)[1])
    city_df['city'] = city_df['city'].str.split(',', expand=True)[0]

    zip_code_df['zip_code'] = zip_code_df['zip_code'].str.split(' ', expand=True)[1]

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
def write_local(state_df: pd.DataFrame, city_df: pd.DataFrame, zip_code_df: pd.DataFrame, dataset_state_file: str, dataset_city_file: str, dataset_zip_file: str):
    'Writing DataFrame locally then as a parquet file'
    datasets = [state_df, city_df, zip_code_df]
    filenames = [dataset_state_file, dataset_city_file, dataset_zip_file]
    paths = []

    for df, filename in zip(datasets, filenames):
        path = Path(f'data/geographic/{filename}.parquet')
        df.to_parquet(path, compression='gzip')
        paths.append(path)

    return tuple(paths)

@task(log_prints=True)
def write_gcs(paths: List[Path]) -> None:
    bucket_name = project_bucket
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    
    for path in paths:
        destination = str(path)
        # Extract the relative path within the data directory
        relative_path = destination.split('data/')[1]
        # Construct the final destination path within the bucket
        final_destination = f'data/{relative_path}'
        gcp = bucket.blob(final_destination)
        gcp.upload_from_filename(destination)

    return 

@flow()  
def etl_api_to_gcs(year: int, state: str, api_key: str) -> None:
    dataset_state_file = f'state/{year}_states_geographic_data'
    dataset_city_file = f'city/{year}_{state}_city_geographic_data'
    dataset_zip_file = f'zip_code/{year}_zip_geographic_data'

    state_df, city_df, zip_code_df = extract_geographic_data(year, state, api_key)
    state_df, city_df, zip_code_df  = transform_data(state_df, city_df, zip_code_df )
    path_one, path_two, path_three = write_local(state_df, city_df, zip_code_df, dataset_state_file, dataset_city_file, dataset_zip_file)
    write_gcs([path_one, path_two, path_three])

@flow
def etl_geo_parent_flow() -> None:
    years = list(range(2021, 2015, -1))
    states_list = ['California','Florida','New York','Texas','Pennsylvania']

    for state in states_list:
        for year in years:
            etl_api_to_gcs(year, state, api_key)

if __name__ == '__main__':
    etl_geo_parent_flow()