"""
Spark application module for transforming raw health discharge data into the silver tier,
including date parsing, null filling, data imputation, filtering, and writing to parquet.
"""

import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from lib import discharges_silver_loader as silver_loader

APP_NAME = "Discharge App - Silver"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

DATA_PATH = os.getenv("DATA_PATH")
if not DATA_PATH:
    raise Exception("DATA_PATH not set")   

if __name__ == "__main__":
    """
    Main entry point for the silver discharge processing job. Initializes the Spark session,
    reads raw discharge data, applies cleaning and transformation utility functions, filters out 
    invalid records based on year, handles specific health insurance adjustments, and writes 
    the final dataframe out as Parquet files to the silver layer.
    """

    spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    cols_to_load = [
        "date", "gender", "age", "health_insurance", "city", 
        "healthcare_service", "facility", "cause_of_death_1", "cause_of_death_2"
    ]

    raw_df = spark.read.parquet(f"{DATA_PATH}/raw/uniformed_discharges").select(*cols_to_load)

    raw_df = raw_df.filter(F.col("date").isNotNull() & (F.trim(F.col("date")) != ""))

    final_df = raw_df.select(
        silver_loader.extract_year("date", "year"),
        silver_loader.fill_null_integers("gender"),
        silver_loader.fill_null_integers("age"),
        silver_loader.fill_null_integers_and_impute("health_insurance", default=9, impute=0),
        silver_loader.fill_null_integers_and_impute("city", default=999999, impute=99999),
        silver_loader.fill_null_integers("healthcare_service"),
        silver_loader.clean_text("cause_of_death_1", default="N/E"),
        silver_loader.clean_text("cause_of_death_2", default="N/E")
    )\
    .filter(
        (F.col("year").isNotNull()) & 
        (F.col("year") > F.lit(2000))
    )\
    .withColumn(
        # Ultima condicion
        "health_insurance",
        F.when(
            (F.col("year") == F.lit(2018)) &
            (F.col("health_insurance") == F.lit(9)),
            F.lit(99)
        ).otherwise(F.col("health_insurance"))
    )

    final_df\
        .coalesce(1) \
        .sortWithinPartitions("year") \
        .write \
        .mode("overwrite") \
        .parquet(f"{DATA_PATH}/silver/discharges")