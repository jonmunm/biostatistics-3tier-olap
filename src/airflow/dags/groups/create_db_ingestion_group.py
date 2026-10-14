"""
Task group module for handling database ingestion processes, 
including dimensions and fact tables loading into DuckDB.
"""

from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from lib.skippable_python_operator import SkippablePythonOperator

def create_db_ingestion_group(group_id: str) -> TaskGroup:
    """
    Creates and returns an Airflow TaskGroup that orchestrates the data loading 
    pipeline from processed Parquet files into the DuckDB warehouse database.

    :param group_id: Unique identifier for the task group.
    :return: An Airflow TaskGroup instance configured with dimension and fact ingestion tasks.
    """
    with TaskGroup(group_id=group_id) as tg:

        def load_dimensions():
            """
            Discovers dimension CSV files, reads their corresponding Gold Parquet outputs,
            and inserts the records into the respective dimension tables within the DuckDB warehouse.
            """
            import glob
            import duckdb
            import os

            DATA_PATH = os.getenv("DATA_PATH")
            DB_SCHEME = os.getenv("WAREHOUSE_DB_NAME")
            DB_DIR = os.path.join(DATA_PATH, "db")
            DB_PATH = os.path.join(DB_DIR, "db.duckdb")
            os.makedirs(DB_DIR, mode=0o777, exist_ok=True)

            dimensions = list(glob.glob(f"{DATA_PATH}/raw/dimensions/dim_*.csv"))
            dimensions = list(map(lambda d: d.split("/")[-1].split(".")[0], dimensions))

            native_connection = duckdb.connect(DB_PATH)

            try:
                for dimension in dimensions:
                    parquet_pattern = f"{DATA_PATH}/gold/dimensions/{dimension}/*.parquet"
                    parquet_file = glob.glob(parquet_pattern)[0]

                    query = f"""
                        INSERT INTO {DB_SCHEME}.{dimension}
                        SELECT * FROM read_parquet('{parquet_file}');
                    """

                    native_connection.execute(query)
            except Exception as e:
                raise e
            finally:
                native_connection.close()
                
        def load_fact_tables():
            """
            Reads processed Parquet files from the Gold tier and bulk-inserts 
            them by name into the main biostatistics fact table within the DuckDB warehouse.
            """
            import os
            import duckdb

            DB_SCHEME = os.getenv("WAREHOUSE_DB_NAME")
            DATA_PATH = os.getenv("DATA_PATH")
            DB_PATH = os.path.join(DATA_PATH, "db", "db.duckdb")

            copy_sql = f"""
                INSERT INTO {DB_SCHEME}.fact_biostatistics BY NAME
                SELECT * FROM read_parquet('{DATA_PATH}/gold/discharges/*.parquet');
            """

            conn = duckdb.connect(DB_PATH)

            try:
                conn.execute(copy_sql)
            except Exception as e:
                raise e
            finally:
                conn.close()         

        load_dimensions = SkippablePythonOperator(
            python_callable=load_dimensions,
            task_id="load_dimensions", 
            trigger_rule=TriggerRule.ALL_DONE,
            exec_flag="{{ params.db_ingestion.load_dimensions }}",
            global_flag="{{ params.db_ingestion.run }}"
        )

        load_fact_tables = SkippablePythonOperator(
            python_callable=load_fact_tables,
            task_id="load_fact_tables",
            trigger_rule=TriggerRule.ALL_DONE,
            exec_flag="{{ params.db_ingestion.load_fact_tables }}",
            global_flag="{{ params.db_ingestion.run }}"
        )

        load_dimensions >> load_fact_tables

    return tg