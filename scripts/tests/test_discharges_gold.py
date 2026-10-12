import os
import sys
import great_expectations as gx
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DQ_Discharges_Gold").getOrCreate()

def get_ids(dim:str) -> list[int]:
    return [row[0] for row in 
                    spark.read.option("header", "true").option("sep", ";")\
                        .csv(dim)\
                        .select("id")\
                        .distinct()\
                        .collect()
                ]    

def validate_uniformed_discharges():
    context = gx.get_context(project_root_dir="/opt/spark/work-dir/gx")
    
    DATA_PATH = os.getenv("DATA_PATH")

    df = spark.read.parquet(f"{DATA_PATH}/gold/discharges")
    year_ids = get_ids(f"{DATA_PATH}/raw/dimensions/dim_year.csv")
    age_ids = get_ids(f"{DATA_PATH}/raw/dimensions/dim_age.csv")
    hi_ids = get_ids(f"{DATA_PATH}/raw/dimensions/dim_health_insurance.csv")
    city_ids = get_ids(f"{DATA_PATH}/raw/dimensions/dim_city.csv")
    hcs_ids = get_ids(f"{DATA_PATH}/raw/dimensions/dim_healthcare_service.csv")
    cd_ids = get_ids(f"{DATA_PATH}/raw/dimensions/dim_cause_of_death.csv")

    datasource = context.data_sources.add_or_update_spark(name="spark_datasource")
    try:
        asset = datasource.add_dataframe_asset(name="discharges_gold_asset")
    except:
        asset = datasource.get_asset("discharges_asset")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name="discharges_gold_quality_suite"))
    validator = context.get_validator(
        batch_request=asset.build_batch_request(options={"dataframe": df}),
        expectation_suite=suite
    )

    # Schema
    columnas = ["health_insurance_id", "year_id", "gender_id", "age_id", "city_id", "healthcare_service_id", "main_cause_of_death_id", "external_cause_of_death_id"]
    validator.expect_table_columns_to_match_set(column_set=columnas)

    validator.expect_column_values_to_not_be_null("health_insurance_id")
    validator.expect_column_values_to_not_be_null("year_id")
    validator.expect_column_values_to_not_be_null("gender_id")
    validator.expect_column_values_to_not_be_null("age_id")
    validator.expect_column_values_to_not_be_null("city_id")
    validator.expect_column_values_to_not_be_null("healthcare_service_id")
    validator.expect_column_values_to_not_be_null("main_cause_of_death_id")
    validator.expect_column_values_to_not_be_null("external_cause_of_death_id")

    validator.expect_column_values_to_be_of_type("health_insurance_id", "IntegerType")
    validator.expect_column_values_to_be_of_type("year_id", "IntegerType")
    validator.expect_column_values_to_be_of_type("gender_id", "IntegerType")
    validator.expect_column_values_to_be_of_type("age_id", "IntegerType")
    validator.expect_column_values_to_be_of_type("city_id", "IntegerType")
    validator.expect_column_values_to_be_of_type("healthcare_service_id", "IntegerType")
    validator.expect_column_values_to_be_of_type("main_cause_of_death_id", "IntegerType")
    validator.expect_column_values_to_be_of_type("external_cause_of_death_id", "IntegerType")    

    validator.expect_column_values_to_be_in_set("year_id", value_set=year_ids)
    validator.expect_column_values_to_be_in_set("gender_id", value_set=[1, 2, 999999])
    validator.expect_column_values_to_be_in_set("age_id", value_set=age_ids)
    validator.expect_column_values_to_be_in_set("health_insurance_id", value_set=hi_ids)
    validator.expect_column_values_to_be_in_set("city_id", value_set=city_ids)
    validator.expect_column_values_to_be_in_set("healthcare_service_id", value_set=hcs_ids)
    validator.expect_column_values_to_be_in_set("main_cause_of_death_id", value_set=cd_ids)
    validator.expect_column_values_to_be_in_set("external_cause_of_death_id", value_set=cd_ids)

    results = validator.validate()
    
    if not results.success:
        for result in results.results:
            if not result.success:
                kwargs = result.expectation_config.kwargs
                column = kwargs.get("column", "N/A (Table Level)")
                print(f"::: Column: {column}")
                print(f"--> Expectation {result.expectation_config.type}")
                print(result.result)
                print("\n")

    assert results.success, "❌ Suite failed"
    
    print("✅ Success! All checks passed.")

if __name__ == "__main__":
    try:
        validate_uniformed_discharges()
    except AssertionError as e:
        print(str(e))
        sys.exit(1)
    except Exception as e:
        print(str(e))
        sys.exit(1)