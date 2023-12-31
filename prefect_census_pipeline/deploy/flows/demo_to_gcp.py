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
def extract_demographic_data(year: int, state: str, api_key: str) -> List[pd.DataFrame]:

    c = Census(api_key, year=year)

    variables = [
        "NAME",  # Name of the geographic area
        "B01001_001E",  # Total population
        "B01001_024E",  # Population aged 17-19
        "B01001_025E",  # Population aged 20-24
        "B08006_001E",  # Total means of transportation
        "B08006_002E",  # Car, truck, or van
        "B08006_003E",  # Public transportation (excluding taxis)
        "B08006_004E",  # Walked
        "B08006_009E",  # Bicycle
        "B08006_014E",  # Other means of transportation
    ]

    # Retrieve the state FIPS code
    state_code = states.lookup(state).fips

    state_data = c.acs5.state(
        variables,
        state_code,
        year=year,
    )

    city_data = c.acs5.state_place(
        variables,
        state_fips=state_code,
        year=year,
        place='*',
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

    # Select relevant columns for age groups and means of transportation
    state_columns = {
        'year': 'year',
        "NAME": 'state',
        "B01001_001E": 'total_population',
        "B01001_024E": 'population_aged_17_to_19',  
        "B01001_025E": 'population_aged_20_to_24', 
        "B08006_001E": 'total_means_of_transportation', 
        "B08006_002E": 'vehicle_usage', 
        "B08006_003E": 'public_transportation',  
        "B08006_004E": 'walked',  
        "B08006_009E": 'bicycle', 
        "B08006_014E": 'other_means_of_transportation', 
        }

    city_columns = {
        'year': 'year',
        "NAME": 'city',
        "B01001_001E": 'total_population',
        "B01001_024E": 'population_aged_17_to_19',
        "B01001_025E": 'population_aged_20_to_24',
        "B08006_001E": 'total_means_of_transportation',
        "B08006_002E": 'vehicle_usage',  
        "B08006_003E": 'public_transportation', 
        "B08006_004E": 'walked',  
        "B08006_009E": 'bicycle',  
        "B08006_014E": 'other_means_of_transportation'  
        }

    zip_columns = {
        'year': 'year',
        "NAME": 'zip_code',
        "B01001_001E": 'total_population',
        "B01001_024E": 'population_aged_17_to_19',  
        "B01001_025E": 'population_aged_20_to_24', 
        "B08006_001E": 'total_means_of_transportation', 
        "B08006_002E": 'vehicle_usage',  
        "B08006_003E": 'public_transportation',  
        "B08006_004E": 'walked',  
        "B08006_009E": 'bicycle',  
        "B08006_014E": 'other_means_of_transportation',  
        }
    
    state_df = state_df.rename(columns=state_columns)
    city_df = city_df.rename(columns=city_columns)
    zip_code_df = zip_code_df.rename(columns=zip_columns)

    return state_df, city_df, zip_code_df

@task(log_prints=True)
def transform_demo_data(state_df: pd.DataFrame, city_df: pd.DataFrame, zip_code_df: pd.DataFrame) -> List[pd.DataFrame]:
    state_df = state_df[['year', 'state', 'total_population', 'population_aged_17_to_19', 'population_aged_20_to_24', 'total_means_of_transportation', 'vehicle_usage',
                         'public_transportation', 'walked', 'bicycle', 'other_means_of_transportation']]
    city_df = city_df[['year', 'city', 'total_population', 'population_aged_17_to_19', 'population_aged_20_to_24', 'total_means_of_transportation', 'vehicle_usage',
                       'public_transportation', 'walked', 'bicycle', 'other_means_of_transportation']]
    zip_code_df = zip_code_df[['year', 'zip_code', 'total_population', 'population_aged_17_to_19', 'population_aged_20_to_24', 'total_means_of_transportation',
                               'vehicle_usage', 'public_transportation', 'walked', 'bicycle', 'other_means_of_transportation']]

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


def write_demo_to_gcs(state_df: pd.DataFrame, city_df: pd.DataFrame, zip_code_df: pd.DataFrame, dataset_state_file: str, dataset_city_file: str, dataset_zip_file: str) -> None:
    gcp_bucket = GcsBucket.load("project-bucket")
    datasets = [state_df, city_df, zip_code_df]
    filenames = [dataset_state_file, dataset_city_file, dataset_zip_file]

    for df, filename in zip(datasets, filenames):
        path = Path(f'/opt/prefect/flows/data/demographic/{filename}.parquet')
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
def api_demo_to_gcs(year: int, state: str) -> None:
    api = Secret.load("api-key")
    api_key = api.get()
    dataset_state_file = f'state/{year}_states_demographic_data'
    dataset_city_file = f'city/{year}_{state}_city_demographic_data'
    dataset_zip_file = f'zip_code/{year}_zip_demographic_data'

    state_df, city_df, zip_code_df = extract_demographic_data(year, state, api_key)
    state_df, city_df, zip_code_df = transform_demo_data(state_df, city_df, zip_code_df)
    write_demo_to_gcs(state_df, city_df, zip_code_df, dataset_state_file, dataset_city_file, dataset_zip_file)

@flow()
def etl_demo_parent_flow(years: list[int],states_list: list[str]) -> None:
    [api_demo_to_gcs(year, state) for state in states_list for year in years]

if __name__ == '__main__':
    years = list(range(2021, 2014, -1))
    states_list = ['California']
    etl_demo_parent_flow(years,states_list)