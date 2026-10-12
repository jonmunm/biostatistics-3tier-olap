from airflow.sdk import dag, task

@dag(dag_id="drop_discharge")
def drop_discharge():
    
    @task(task_id="drop_db_schema")
    def drop_db_schema():
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