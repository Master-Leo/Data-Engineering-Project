# import os
# import sys
# import pandas as pd
# from pathlib import Path 
# from typing import List, Tuple
# from census import Census 
# from us import states
# from prefect import Flow, flow, task
# from prefect_gcp.cloud_storage import GcsBucket
# from prefect.blocks.system import Secret


# print(f'Secrets have been loaded')
# @task(log_prints=True, tags=['extract'])
# def extract_economic_data(year: int, state: str, api_key: str ) -> List[pd.DataFrame]:
#     # Create Census object
#     c = Census(api_key, year=year)
#     variables = [
#         'NAME',  # Location name
#         'B01003_001E',  # Total population
#         'B01002_001E',  # Median age
#         'B19013_001E',  # Median household income
#         'B19301_001E',  # Per capita income
#         'B17001_002E',  # Poverty count
#         'B23025_005E',  # Unemployment count
#         'B23025_004E',  # Employment count      
#     ]

#     state_data = c.acs5.get(
#         variables, 
#         {'for': 'state:*'}
#     )

#     state_code = states.lookup(state).fips

#     city_data = c.acs5.state_place(
#         variables,
#         state_code,
#         Census.ALL,
#     )

#     zip_data = c.acs5.state_zipcode(
#         variables,
#         state_fips=state_code,
#         zcta='*',
#     )

#     # Convert to DataFrame
#     state_df = pd.DataFrame(state_data)
#     city_df = pd.DataFrame(city_data)
#     zip_code_df = pd.DataFrame(zip_data)

#     state_df['year']=year
#     city_df['year']=year
#     zip_code_df['year']=year

#     state_columns = {'year': 'year',
#                         'NAME': 'name',
#                         'state': 'state',
#                         'B01003_001E': 'population',
#                         'B01002_001E': 'median_age',
#                         'B19013_001E': 'household_income',
#                         'B19301_001E': 'per_capita_income',
#                         'B17001_002E': 'poverty_count',
#                         'B23025_005E': 'unemployment_count',
#                         'B23025_004E': 'employment_count'}
    
#     city_columns = {'year': 'year',
#                         'NAME': 'city',
#                         'B01003_001E': 'population',
#                         'B01002_001E': 'median_age',
#                         'B19013_001E': 'household_income',
#                         'B19301_001E': 'per_capita_income',
#                         'B17001_002E': 'poverty_count',
#                         'B23025_005E': 'unemployment_count',
#                         'B23025_004E': 'employment_count'}

#     zip_columns = {'year': 'year',
#                         'NAME': 'zip_code',
#                         'B01003_001E': 'population',
#                         'B01002_001E': 'median_age',
#                         'B19013_001E': 'household_income',
#                         'B19301_001E': 'per_capita_income',
#                         'B17001_002E': 'poverty_count',
#                         'B23025_005E': 'unemployment_count',
#                         'B23025_004E': 'employment_count'}
    
#     # Column Reordering
#     state_df = state_df.rename(columns=state_columns)
#     city_df = city_df.rename(columns=city_columns)
#     zip_code_df = zip_code_df.rename(columns=zip_columns)

#     return state_df, city_df, zip_code_df

# @task(log_prints=True)
# def transform_econ_data(state_df: pd.DataFrame, city_df: pd.DataFrame, zip_code_df : pd.DataFrame) -> List[pd.DataFrame]:
#     def calculate_rates(df: pd.DataFrame) -> pd.DataFrame:
#         df["poverty_rate"] = 100 * df['poverty_count'].astype(int) / df["population"].astype(int)
#         df["unemployment_rate"] = 100 * df["unemployment_count"].astype(int) / df["population"].astype(int)

#         return df

#     state_df = calculate_rates(state_df)
#     city_df = calculate_rates(city_df)
#     zip_code_df = calculate_rates(zip_code_df)
    
#     state_df = state_df[['year', 'state', 'population', 'median_age', 'household_income', 'per_capita_income',
#                         'poverty_count', 'unemployment_count', 'employment_count']]
#     city_df = city_df[['year', 'city', 'population', 'median_age', 'household_income', 'per_capita_income',
#                     'poverty_count', 'unemployment_count', 'employment_count']]
#     zip_code_df = zip_code_df[['year', 'zip_code', 'population', 'median_age', 'household_income', 'per_capita_income',
#                             'poverty_count', 'unemployment_count', 'employment_count']]

#     city_df.insert(1, 'state', city_df['city'].str.split(',', expand=True)[1])
#     city_df['city'] = city_df['city'].str.split(',', expand=True)[0]

#     zip_code_df['zip_code'] = zip_code_df['zip_code'].str.split(' ', expand=True)[1]

#     print(state_df.head())
#     print(city_df.head())
#     print(state_df.head())

#     print(f'Columns: {state_df.dtypes}')
#     print(f'Rows: {len(state_df)}')

#     print(f'Columns: {city_df.dtypes}')
#     print(f'Rows: {len(city_df)}')

#     print(f'Columns: {zip_code_df.dtypes}')
#     print(f'Rows: {len(zip_code_df)}')

#     return state_df, city_df, zip_code_df

    
# def write_econ_to_gcs(state_df: pd.DataFrame, city_df: pd.DataFrame, zip_code_df: pd.DataFrame, dataset_state_file: str, dataset_city_file: str, dataset_zip_file: str) -> None:
#     gcp_bucket = GcsBucket.load("project-bucket")
#     datasets = [state_df, city_df, zip_code_df]
#     filenames = [dataset_state_file, dataset_city_file, dataset_zip_file]

#     for df, filename in zip(datasets, filenames):
#         path = Path(f'data/economic/{filename}.parquet')
#         df.to_parquet(path, compression='gzip')

#         destination = str(path)
#         # # Extract the relative path within the data directory
#         relative_path = destination.split('data/')[1]

#         # Construct the final destination path within the bucket
#         final_destination = f'data/{relative_path}'
#         gcp_bucket.upload_from_path(from_path=destination, to_path=final_destination)
#         path.unlink()

#     return

# @flow()  
# def api_econ_to_gcs(year: int, state: str) -> None:
#     api = Secret.load("api-key")
#     api_key = api.get()
#     dataset_state_file = f'state/{year}_states_economic_data'
#     dataset_city_file = f'city/{year}_{state}_city_economic_data'
#     dataset_zip_file = f'zip_code/{year}_zip_economic_data'

#     state_df, city_df, zip_code_df = extract_economic_data(year, state, api_key)
#     state_df, city_df, zip_code_df  = transform_econ_data(state_df, city_df, zip_code_df)
#     write_econ_to_gcs(state_df, city_df, zip_code_df, dataset_state_file, dataset_city_file, dataset_zip_file)

# @flow()
# def etl_econ_parent_flow(years: list[int],states_list: list[str]) -> None:
#     [api_econ_to_gcs(year, state) for state in states_list for year in years]

# if __name__ == '__main__':
#     years = [2021,2020,2019,2018,2017,2016]
#     states_list = ['California']
#     etl_econ_parent_flow(years,states_list)


