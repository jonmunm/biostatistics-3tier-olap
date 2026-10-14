"""
Task group module for handling database initialization and setup in DuckDB.
"""

from airflow.sdk import task
from airflow.utils.task_group import TaskGroup

def create_db_initialization_group(group_id: str) -> TaskGroup:
    """
    Creates and returns an Airflow TaskGroup that handles the creation and initialization 
    of the database schema using an external SQL script.

    :param group_id: Unique identifier for the task group.
    :return: An Airflow TaskGroup instance configured with the initialization task.
    """
    with TaskGroup(group_id=group_id) as tg:
        
        @task(task_id=group_id)
        def initialize_db():
            """
            Reads the initialization SQL script from the schema directory and executes it 
            against the local DuckDB database file to set up schemas and tables.
            """
            import os
            import duckdb

            DATA_PATH = os.getenv("DATA_PATH")
            SCRIPTS_PATH = os.getenv("SCRIPTS_PATH")
            SQL_DIR = f"{SCRIPTS_PATH}/sql_schema/init.sql"
            DB_DIR = os.path.join(DATA_PATH, "db")
            DB_PATH = os.path.join(DB_DIR, "db.duckdb")
            os.makedirs(DB_DIR, mode=0o777, exist_ok=True)

            with open(SQL_DIR, "r", encoding="utf-8") as file:
                sql_script = file.read()

            conn = duckdb.connect(DB_PATH)
            try:
                conn.execute(sql_script)
            except Exception as e:
                raise e
            finally:
                conn.close()
        
        initialize_db()

    return tg