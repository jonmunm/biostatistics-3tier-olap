"""
Spark application module for discovering raw dimension CSV files 
and transforming/converting them into parquet format in the gold tier.
"""

import logging
import sys
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T

APP_NAME = "Dimension Loading"

# Asegúrate de definir esto ANTES de crear la SparkSession
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DATA_PATH = os.getenv("DATA_PATH")
if not DATA_PATH:
    raise Exception("DATA_PATH not set")

if __name__ == "__main__":
    """
    Main entry point for the dimension loading Spark job. 
    Initializes the Spark session, discovers raw dimension CSV files dynamically, 
    and writes them out as Parquet files into the gold tier storage.
    """

    spark = SparkSession\
        .builder\
        .appName(APP_NAME)\
        .config("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol") \
        .config("mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
        .getOrCreate()

    dimensions = list(glob.glob(f"{DATA_PATH}/raw/dimensions/dim_*.csv"))
    dimensions = list(map(lambda d: d.split("/")[-1].split(".")[0], dimensions))
    
    for dimension in dimensions:
        spark\
            .read\
            .option("header", "true")\
            .option("sep", ";")\
            .csv(f"{DATA_PATH}/raw/dimensions/{dimension}.csv")\
            .write\
            .mode("overwrite")\
            .parquet(f"{DATA_PATH}/gold/dimensions/{dimension}")