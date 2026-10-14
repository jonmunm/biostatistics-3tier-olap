"""
Airflow DAG module for dropping or cleaning up the database schemas and tables in DuckDB.
"""

from airflow.sdk import dag, task

@dag(dag_id="drop_discharge")
def drop_discharge():
    """
    Defines the 'drop_discharge' DAG workflow, which executes a cleanup SQL script 
    to drop existing tables or schemas within the DuckDB warehouse.
    """
    
    @task(task_id="drop_db_schema")
    def drop_db_schema():
        """
        Reads the drop SQL script from the schema directory and executes it 
        against the local DuckDB database file to clear out existing data structures.
        """
        import duckdb
        import os
        
        scripts_path = os.getenv('SCRIPTS_PATH')
        with open(f"{scripts_path}/sql_schema/drop.sql", "r") as f:
            sql = f.read()
                
        data_path = os.getenv("DATA_PATH")
        db_path = os.path.join(data_path, "db", "db.duckdb")
        
        conn = duckdb.connect(db_path)
        
        try:
            conn.execute(sql)
        except Exception as e:
            raise e
        finally:
            conn.close()

    drop_db_schema()   
   
drop_discharge()