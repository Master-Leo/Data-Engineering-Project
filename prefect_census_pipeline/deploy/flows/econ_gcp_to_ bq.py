import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
import gcsfs
import pandas as pd 
from prefect.blocks.system import Secret

json = Secret.load("json-path")

json_path = json.get()

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('economic_data') \
    .set("spark.jars", "./lib/gcs-connector-hadoop3-2.2.5.jar") \
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


bucket = 'de_final_project_bucket'
base_path = 'data/economic/city/'
years = range(2016,2022)
dfs = []

for year in years:
    file_path = f'{base_path}{year}_California_city_demographic_data.parquet'
    df = spark.read.parquet(f'gs://{bucket}/{file_path}')
    dfs.append(df)


meta_df = None 

for df in dfs:
    if meta_df is None:
        meta_df = df 
    else:
        meta_df = meta_df.union(df)