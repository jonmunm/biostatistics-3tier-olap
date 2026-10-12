import logging
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.column import Column
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

APP_NAME = "Discharge App - Gold"

# Asegúrate de definir esto ANTES de crear la SparkSession
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

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName(APP_NAME)\
        .getOrCreate()

    read_csv = lambda path: spark.read.option("header", "true").option("sep", ";").csv(path)

    dim_gender_df = read_csv(f"{DATA_PATH}/raw/dimensions/dim_gender.csv") \
        .select("id")

    dim_age_df = read_csv(f"{DATA_PATH}/raw/dimensions/dim_age.csv") \
        .select(
            (F.col("age_id").cast("int") + 1).alias("real_age"),
            F.col("id").alias("age_id")            
        )

    dim_year_df = read_csv(f"{DATA_PATH}/raw/dimensions/dim_year.csv") \
        .select(
            (F.col("id").cast("int") + 1989).alias("real_year"),
            F.col("id").alias("year_id")            
        )

    dim_health_insurance_df = read_csv(f"{DATA_PATH}/raw/dimensions/dim_health_insurance.csv")\
        .select(
            F.col("id").alias("health_insurance_id"), 
            F.col("health_insurance_code"),
            F.col("valid_from"),
            F.col("valid_to")
        )

    dim_healthcare_service_df = read_csv(f"{DATA_PATH}/raw/dimensions/dim_healthcare_service.csv")\
        .select(
            F.col("id").alias("healthcare_service_id"),
            F.col("healthcare_service_cut"),
            F.col("valid_from"),
            F.col("valid_to")
        )

    dim_city_df = read_csv(f"{DATA_PATH}/raw/dimensions/dim_city.csv")\
        .select(
            F.col("id").alias("city_id"),
            F.col("city_cut"),
            F.col("valid_from").cast("integer"),
            F.col("valid_to").cast("integer")
        )

    cod_recod_df = read_csv(f"{DATA_PATH}/raw/dimensions/cause_of_death.recoded.lookup.csv")

    dim_cause_of_death_df = read_csv(f"{DATA_PATH}/raw/dimensions/dim_cause_of_death.csv")\
        .select(
            F.col("id"),
            F.col("subcategory_code")
        )

    invalid_cuts_df = read_csv(f"{DATA_PATH}/raw/dimensions/city.invalid_cuts.lookup.csv")

    # Gender
    # Year
    # Age
    # Health Insurance
    # HealthCare Service
    discharges_df = spark\
        .read\
        .parquet(f"{DATA_PATH}/silver/discharges")\
        .join(F.broadcast(dim_gender_df), F.col("gender") == F.col("id"), how="left")\
        .join(F.broadcast(dim_year_df), F.col("year") == F.col("real_year"), how="left")\
        .join(F.broadcast(dim_age_df), F.col("age") == F.col("real_age"), how="left")\
        .join(
            F.broadcast(dim_health_insurance_df.alias("hi")),
            (F.col("year") >= F.col("hi.valid_from")) & 
            (F.col("year") <= F.col("hi.valid_to")) &
            (F.col("health_insurance") == F.col("hi.health_insurance_code")),
            how="left"
        )\
        .join(
            F.broadcast(dim_healthcare_service_df.alias("hcs")),
            (F.col("year") >= F.col("hcs.valid_from")) & 
            (F.col("year") <= F.col("hcs.valid_to")) &
            (F.col("healthcare_service") == F.col("hcs.healthcare_service_cut")),
            how="left"
        )\
        .select(
            F.col("year"),
            F.col("hi.health_insurance_id").cast(IntegerType()),
            F.col("year_id").cast(IntegerType()),
            F.coalesce(F.col("id"), F.lit(999999)).cast(IntegerType()).alias("gender_id"),
            F.coalesce(F.col("age_id"), F.lit(999999)).cast(IntegerType()).alias("age_id"),
            F.col("city"),
            F.col("hcs.healthcare_service_id").cast(IntegerType()),
            F.col("hcs.healthcare_service_cut"),
            F.col("cause_of_death_1"),
            F.col("cause_of_death_2")
        )

    # Cause of death
    discharges_df = discharges_df\
    .alias("e")\
    .join(
        F.broadcast(cod_recod_df.alias("cod_1")),
        F.translate(F.col("e.cause_of_death_1"), ",.", "") == F.col("cod_1.code"),
        how="left"
    )\
    .join(
        F.broadcast(cod_recod_df.alias("cod_2")),
        F.translate(F.col("e.cause_of_death_2"), ",.", "") == F.col("cod_2.code"),
        how="left"
    )\
    .select(
        "*",
        F.when(
            F.coalesce(
                F.translate(F.col("cod_1.new_code"), ",.", ""),
                F.translate(F.col("e.cause_of_death_1"), ",.", "")
            ).rlike("^[VWXY]"), 
            F.lit("N/E")
        ).otherwise(
            F.coalesce(
                F.translate(F.col("cod_1.new_code"), ",.", ""),
                F.translate(F.col("e.cause_of_death_1"), ",.", "")
            )
        ).alias("cause_of_death_1_final"),
        F.coalesce(
            F.translate(F.col("cod_2.new_code"), ",.", ""),
            F.translate(F.col("e.cause_of_death_2"), ",.", "")
        ).alias("cause_of_death_2_final")
    )\
    .alias("e")\
    .join(
        F.broadcast(dim_cause_of_death_df.alias("c1")),
        F.col("e.cause_of_death_1_final") == F.col("c1.subcategory_code"),
        how="left"
    )\
    .join(
        F.broadcast(dim_cause_of_death_df.alias("c2")),
        F.col("e.cause_of_death_2_final") == F.col("c2.subcategory_code"),
        how="left"
    )\
    .select(
        F.col("e.year"),
        F.col("e.health_insurance_id"),
        F.col("e.year_id"),
        F.col("e.gender_id"),
        F.col("e.age_id"),
        F.col("e.city"),
        F.col("e.healthcare_service_id"),
        F.coalesce(F.col("c1.id"), F.lit(999999)).cast(IntegerType()).alias("main_cause_of_death_id"),
        F.coalesce(F.col("c2.id"), F.lit(999999)).cast(IntegerType()).alias("external_cause_of_death_id")
    )

    # city
    discharges_df = discharges_df\
    .join(
        F.broadcast(invalid_cuts_df),
        F.col("city") == F.col("CUT"),
        how="left"
    )\
    .join(
        F.broadcast(dim_city_df),
        (F.col("year") >= F.col("valid_from")) & 
        (F.col("year") <= F.col("valid_to")) &
        (F.coalesce(F.when(F.col("CUT").isNotNull(), F.lit(999999)), F.col("city")) == F.col("city_cut")),
        how="left"
    )\
    .select(
        F.col("health_insurance_id"),
        F.col("year_id"),
        F.col("gender_id"),
        F.col("age_id"),
        F.col("city_id").cast(IntegerType()),
        F.col("healthcare_service_id"),
        F.col("main_cause_of_death_id"),
        F.col("external_cause_of_death_id")      
    )

    discharges_df\
        .coalesce(1) \
        .write \
        .mode("overwrite") \
        .parquet(f"{DATA_PATH}/gold/discharges")