import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from lib import discharges_raw_loader as raw_loader

APP_NAME = "Discharge App - Raw"

# Asegúrate de definir esto ANTES de crear la SparkSession
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

DATA_PATH = os.getenv("DATA_PATH")
if not DATA_PATH:
    raise Exception("DATA_PATH not set")

if __name__ == "__main__":

    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .getOrCreate()

    group_1_df = raw_loader.load_group_1(spark).select(
        F.col("fecha_egre").alias("date"),
        F.col("ser_salud").alias("healthcare_service"),
        F.col("estab").alias("facility"),
        F.col("sexo").alias("gender"),
        F.col("edad").alias("age"),
        F.col("comuna").alias("city"),
        F.col("diag1").alias("cause_of_death_1"),
        F.col("diag2").alias("cause_of_death_2"),
        F.col("previ").alias("health_insurance"),
    )

    group_2_df = raw_loader.load_group_2(spark).select(
        F.col("FECHA_EGR").alias("date"),
        F.col("SER_SALUD").alias("healthcare_service"),
        F.col("EST").alias("facility"),
        F.col("SEXO").alias("gender"),
        F.col("EDAD").alias("age"),
        F.col("COMUNA").alias("city"),
        F.col("DIAG1").alias("cause_of_death_1"),
        F.col("DIAG2").alias("cause_of_death_2"),
        F.col("PREVI").alias("health_insurance"),
    )

    group_3_df = raw_loader.load_group_3(spark).select(
        F.col("fecha_egre").alias("date"),
        F.col("ser_salud").alias("healthcare_service"),
        F.col("estab").alias("facility"),
        F.col("sexo").alias("gender"),
        F.col("edad").alias("age"),
        F.col("comuna").alias("city"),
        F.col("diag1").alias("cause_of_death_1"),
        F.col("diag2").alias("cause_of_death_2"),
        F.col("previ").alias("health_insurance"),
    )

    group_4_df = raw_loader.load_group_4(spark).select(
        F.col("fecha_egre").alias("date"),
        F.col("ser_salud").alias("healthcare_service"),
        F.col("estab").alias("facility"),
        F.col("sexo").alias("gender"),
        F.col("edad").alias("age"),
        F.col("comuna").alias("city"),
        F.col("diag1").alias("cause_of_death_1"),
        F.col("diag2").alias("cause_of_death_2"),
        F.col("previ").alias("health_insurance"),
    )

    group_5_df = raw_loader.load_group_5(spark).select(
        F.col("fecha_egre").alias("date"),
        F.col("ser_salud").alias("healthcare_service"),
        F.col("estab").alias("facility"),
        F.col("sexo").alias("gender"),
        F.col("edad").alias("age"),
        F.col("comuna").alias("city"),
        F.col("diag1").alias("cause_of_death_1"),
        F.col("diag2").alias("cause_of_death_2"),
        F.col("previ").alias("health_insurance"),
    )

    group_6_df = raw_loader.load_group_6(spark).select(
        F.col("FECHA_EGR").alias("date"),
        F.col("SER_SALUD").alias("healthcare_service"),
        F.col("ESTAB").alias("facility"),
        F.col("SEXO").alias("gender"),
        F.col("EDAD").alias("ager"),
        F.col("COMUNA").alias("city"),
        F.col("DIAG1").alias("cause_of_death_1"),
        F.col("DIAG2").alias("cause_of_death_2"),
        F.col("PREVI").alias("health_insurance"),
    )

    group_7_df = raw_loader.load_group_7(spark).select(
        F.col("FECHA_EGR").alias("date"),
        F.col("ServicioSalud").alias("healthcare_service"),
        F.col("ESTAB").alias("facility"),
        F.col("SEXO").alias("gender"),
        F.col("EDAD").alias("age"),
        F.col("COMUNA").alias("city"),
        F.col("DIAG1").alias("cause_of_death_1"),
        F.col("DIAG2").alias("cause_of_death_2"),
        F.col("PREVI").alias("health_insurance"),
    )
    
    complete_df = group_1_df\
        .union(group_2_df)\
        .union(group_3_df)\
        .union(group_4_df)\
        .union(group_5_df)\
        .union(group_6_df)\
        .union(group_7_df)

    complete_df \
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(f"{DATA_PATH}/raw/uniformed_discharges")