"""
Great Expectations data quality module for validating silver tier health discharge datasets,
including schema column matching, null checks, data types, value ranges, and regex pattern matching 
for causes of death.
"""

import os
import sys
import great_expectations as gx
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DQ_Discharges_Silver").getOrCreate()

def validate_uniformed_discharges():
    """
    Initializes a Great Expectations context, loads the silver tier discharge parquet dataset,
    configures datasource assets and expectation suites, executes comprehensive data quality validations
    (nulls, types, ranges, regex for ICD codes), and handles assertion failures by printing out detailed error metadata.
    """
    context = gx.get_context(project_root_dir="/opt/spark/work-dir/gx")
    
    data_path = os.getenv('DATA_PATH', '/opt/spark/data')
    df = spark.read.parquet(f"{data_path}/silver/discharges")

    datasource = context.data_sources.add_or_update_spark(name="spark_datasource")
    try:
        asset = datasource.add_dataframe_asset(name="discharges_silver_asset")
    except:
        asset = datasource.get_asset("discharges_asset")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name="discharges_silver_quality_suite"))
    validator = context.get_validator(
        batch_request=asset.build_batch_request(options={"dataframe": df}),
        expectation_suite=suite
    )

    # Schema
    columnas = ["year", "gender", "age", "health_insurance", "city", "healthcare_service", "cause_of_death_1", "cause_of_death_2"]
    validator.expect_table_columns_to_match_set(column_set=columnas)

    validator.expect_column_values_to_not_be_null("year")
    validator.expect_column_values_to_not_be_null("gender")
    validator.expect_column_values_to_not_be_null("age")
    validator.expect_column_values_to_not_be_null("health_insurance")
    validator.expect_column_values_to_not_be_null("city")
    validator.expect_column_values_to_not_be_null("healthcare_service")
    validator.expect_column_values_to_not_be_null("cause_of_death_1")
    validator.expect_column_values_to_not_be_null("cause_of_death_2")    

    validator.expect_column_values_to_be_of_type("year", "IntegerType")
    validator.expect_column_values_to_be_of_type("gender", "IntegerType")
    validator.expect_column_values_to_be_of_type("age", "IntegerType")
    validator.expect_column_values_to_be_of_type("health_insurance", "IntegerType")
    validator.expect_column_values_to_be_of_type("city", "IntegerType")
    validator.expect_column_values_to_be_of_type("healthcare_service", "IntegerType")
    validator.expect_column_values_to_be_of_type("cause_of_death_1", "StringType")
    validator.expect_column_values_to_be_of_type("cause_of_death_2", "StringType")    

    validator.expect_column_values_to_be_between("year", min_value=2001, max_value=2018)
    validator.expect_column_values_to_be_in_set("gender", [1, 2, 3, 9])
    validator.expect_column_values_to_be_between("age", min_value=0, max_value=130)
    validator.expect_column_values_to_be_between("health_insurance", min_value=0, max_value=99)
    validator.expect_column_values_to_be_between("city", min_value=0, max_value=999999)
    validator.expect_column_values_to_be_between("healthcare_service", min_value=0, max_value=999999)

    partes = [
        r"[A-Z][0-9]{3}", # Ejemplo: A123
        r"[A-Z][0-9]{2}X", # Ejemplo: A12X
        r"[A-Z][0-9]{2,4}", # Ejemplo: A12, A1234
        r"[A-Z][0-9]{3}\+", # Ejemplo: A123+
        r"N/E"
    ]

    regex = r"^(" + "|".join(partes) + r")$"
    validator.expect_column_values_to_match_regex("cause_of_death_1", regex=regex, mostly=0.99)
    validator.expect_column_values_to_match_regex("cause_of_death_2", regex=regex, mostly=0.74)

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