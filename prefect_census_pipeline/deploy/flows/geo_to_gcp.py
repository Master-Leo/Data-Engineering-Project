import os
import sys
import pandas as pd
from pathlib import Path 
from typing import List, Tuple 
from census import Census 
from us import states
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.blocks.system import Secret

@task(log_prints=True, tags=['extract'])
def extract_geographic_data(year: int, state: str, api_key: str) -> List[pd.DataFrame]:
    c = Census(api_key, year=year)

    variables = [
        "NAME",  # Geographic name
        "B18101_001E",  # Disability status of the civilian noninstitutionalized population
        "B27001_001E",  # Health insurance coverage by type of coverage and age
        "B07013_001E",  # Geographical mobility in the past year for current residence
        "B08006_001E",  # Means of transportation to work by selected characteristics
        "B28002_001E",  # Presence and types of internet subscriptions in households
        "GEO_ID",  # Geographic identifier code
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
def transform_geo_data(state_df: pd.DataFrame, city_df: pd.DataFrame, zip_code_df : pd.DataFrame) -> List[pd.DataFrame]:
    
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


def write_geo_to_gcs(state_df: pd.DataFrame, city_df: pd.DataFrame, zip_code_df: pd.DataFrame, dataset_state_file: str, dataset_city_file: str, dataset_zip_file: str) -> None:
    gcp_bucket = GcsBucket.load("project-bucket")
    datasets = [state_df, city_df, zip_code_df]
    filenames = [dataset_state_file, dataset_city_file, dataset_zip_file]

    for df, filename in zip(datasets, filenames):
        path = Path(f'/opt/prefect/flows/data/geographic/{filename}.parquet')
        df.to_parquet(path, compression='gzip')

        destination = str(path)
        # # Extract the relative path within the data directory
        relative_path = destination.split('data/')[1]

        # Construct the final destination path within the bucket
        final_destination = f'data/{relative_path}'
        gcp_bucket.upload_from_path(from_path=destination, to_path=final_destination)
        path.unlink()

    return

@flow()  
def api_geo_to_gcs(year: int, state: str) -> None:
    api = Secret.load("api-key")
    api_key = api.get()
    dataset_state_file = f'state/{year}_states_geographic_data'
    dataset_city_file = f'city/{year}_{state}_city_geographic_data'
    dataset_zip_file = f'zip_code/{year}_zip_geographic_data'

    state_df, city_df, zip_code_df = extract_geographic_data(year, state, api_key)
    state_df, city_df, zip_code_df  = transform_geo_data(state_df, city_df, zip_code_df)
    write_geo_to_gcs(state_df, city_df, zip_code_df, dataset_state_file, dataset_city_file, dataset_zip_file)

@flow()
def etl_geo_parent_flow(years: list[int],states_list: list[str]) -> None:
    [api_geo_to_gcs(year, state) for state in states_list for year in years]

if __name__ == '__main__':
    years = list(range(2021, 2014, -1))
    states_list = ['California']
    etl_geo_parent_flow(years,states_list)
