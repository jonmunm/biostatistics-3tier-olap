import os
import sys
import great_expectations as gx
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DQ_Discharges_Raw").getOrCreate()

def validate_uniformed_discharges():
    context = gx.get_context(project_root_dir="/opt/spark/work-dir/gx")
    
    data_path = os.getenv('DATA_PATH', '/opt/spark/data')
    df = spark.read.parquet(f"{data_path}/raw/uniformed_discharges")

    datasource = context.data_sources.add_or_update_spark(name="spark_datasource")
    try:
        asset = datasource.add_dataframe_asset(name="discharges_raw_uniformed_asset")
    except:
        asset = datasource.get_asset("discharges_raw_uniformed_asset")

    suite = context.suites.add_or_update(gx.ExpectationSuite(name="discharges_raw_uniformed_quality_suite"))

    validator = context.get_validator(
        batch_request=asset.build_batch_request(options={"dataframe": df}),
        expectation_suite=suite
    )

    # Schema
    columnas = ["date", "healthcare_service", "facility", "gender", "age", "city", "cause_of_death_1", "cause_of_death_2", "health_insurance"]
    validator.expect_table_columns_to_match_set(column_set=columnas)

    validator.expect_column_values_to_be_between("age", min_value=0, max_value=130)
    validator.expect_column_values_to_be_in_set("gender", [1, 2, 3, 9])

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