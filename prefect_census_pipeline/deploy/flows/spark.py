import os
from prefect import task
from pyspark.sql import SparkSession, Window
from pyspark.sql import DataFrame as SparkDataFrame
import contextlib
import os
from prefect import flow
from pyspark import SparkContext, SparkConf
from prefect.blocks.system import Secret
import pandas as pd
from typing import List

@contextlib.contextmanager
def get_spark_session(json, app_name='economic_data'):
    """
    Function that is wrapped by context manager
    Args:
      - conf(SparkConf): It is the configuration for the Spark session
    """
    json_path = json
    
    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName(app_name) \
        .set("spark.jars", "/Users/og/Downloads/gcs-connector-hadoop2-2.2.15-shaded.jar") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", json_path)

    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", json_path)
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()


    yield spark

def retrieve_gcp_parquet(years: List[int], bucket: str, base_path: str, spark: SparkSession):
    dfs = []
    for year in years:
        file_path = f'{base_path}{year}_California_city_demographic_data.parquet'
        df = spark.read.parquet(f'gs://{bucket}/{file_path}')
        dfs.append(df)

    return dfs


def union_dataframes(dfs: List[SparkDataFrame]) -> SparkDataFrame:
    meta_df = None
    for df in dfs:
        if meta_df is None:
            meta_df = df 
        else:
            meta_df = meta_df.union(df)
    return meta_df

def data_pipeline(json_path):
    bucket = 'de_final_project_bucket'
    base_path = 'data/economic/city/'
    years = range(2020,2021)
    # Setting up the Spark cluster
    with get_spark_session(json=json_path,app_name='test') as spark_session:

        dfs = retrieve_gcp_parquet(years=years,
                                bucket=bucket,
                                base_path=base_path,
                                spark=spark_session)
        meta_df = union_dataframes(dfs)
        
    return meta_df 


json = ''
meta_df = data_pipeline(json)
meta_df.show()