import os
import sys
# import argparase
import pandas as pd
import Census 
from us import states
from sqlalchemy import create_engine
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
path = os.path.join(os.getcwd(), '../../')
sys.path.append(path)
from config import api_key


@task(log_prints=True, tags=['extract'])
def extract_data():
    # Create Census object
    c = Census(api_key, year=2021)

    # Run Census Search to retrieve data on all states
    # Note the addition of "B23025_005E" for unemployment count
    census_data = c.acs5.get(("NAME", "B19013_001E", "B01003_001E", "B01002_001E",
                              "B19301_001E",
                              "B17001_002E",
                              "B23025_005E"), {'for': 'state:*'})

    # Convert to DataFrame
    state_df = pd.DataFrame(census_data)

    # Column Reordering
    state_df = state_df.rename(columns={"B01003_001E": "Population",
                                          "B01002_001E": "Median Age",
                                          "B19013_001E": "Household Income",
                                          "B19301_001E": "Per Capita Income",
                                          "B17001_002E": "Poverty Count",
                                          "B23025_005E": "Unemployment Count",
                                          "NAME": "Name", "state": "State"})
    return state_df

@task(log_prints=True)
def transform_data(state_df: pd.DataFrame):
    # Add in Poverty Rate (Poverty Count / Population)
    state_df["Poverty Rate"] = 100 * state_df["Poverty Count"].astype(int) / state_df["Population"].astype(int)

    # Add in Employment Rate (Employment Count / Population)
    state_df["Unemployment Rate"] = 100 * state_df["Unemployment Count"].astype(int) / state_df["Population"].astype(int)

    # Final DataFrame
    state_df = state_df[["State", "Name", "Population", "Median Age", "Household Income",
                           "Per Capita Income", "Poverty Count", "Poverty Rate", "Unemployment Rate"]]

    return state_df

@task(log_prints=True, retries=1)
def load_data(table_name, state_df):
    
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        state_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        state_df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name='Sublow', log_prints=True)
def log_subflow(table_name: str)
    print(f'Logging Subflow for: {table_name}')

@flow(name='Ingest State Data')
def main_flow(table_name: str = "yellow_taxi_trips"):

    log_subflow(table_name)
    raw = extract_data()
    state_data = transform_data(raw)
    load_data(table_name, state_data)

if __name__ == '__main__':
    main_flow(table_name = "california_economic_data")