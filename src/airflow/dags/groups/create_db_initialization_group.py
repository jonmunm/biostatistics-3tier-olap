from airflow.sdk import task
from airflow.utils.task_group import TaskGroup

def create_db_initialization_group(group_id: str) -> TaskGroup:
    with TaskGroup(group_id=group_id) as tg:
        
        @task(task_id=group_id)
        def initialize_db():
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