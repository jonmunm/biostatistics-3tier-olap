import os
from airflow.sdk import task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from lib.skippable_spark_submit_operator import SkippableSparkSubmitOperator

SCRIPTS_PATH = os.getenv("SCRIPTS_PATH")

def create_compute_group(group_id: str) -> TaskGroup:
    with TaskGroup(group_id=group_id) as tg:

        run_raw_transformations = SkippableSparkSubmitOperator(
            task_id="run_raw_transformations",
            application=f"{SCRIPTS_PATH}/spark/load_discharges_raw.py",
            conn_id="compute_engine",
            executor_memory='4g',
            executor_cores=1,
            total_executor_cores=1,
            verbose=True,
            exec_flag="{{ params.compute.run_raw_transformations }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE,
            packages="org.apache.hadoop:hadoop-azure:3.3.4"
        )    

        run_raw_tests = SkippableSparkSubmitOperator(
            task_id="run_raw_tests",
            application=f"{SCRIPTS_PATH}/tests/test_discharges_raw.py",
            conn_id="compute_engine",
            executor_memory='4g',
            executor_cores=1,
            total_executor_cores=1,
            verbose=True,
            exec_flag="{{ params.compute.run_raw_tests }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )     

        run_silver_transformations = SkippableSparkSubmitOperator(
            task_id="run_silver_transformations",
            application=f"{SCRIPTS_PATH}/spark/load_discharges_silver.py",
            conn_id="compute_engine",
            executor_memory='4g',
            executor_cores=1,
            total_executor_cores=1,
            verbose=True,
            exec_flag="{{ params.compute.run_silver_transformations }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )   

        run_silver_tests = SkippableSparkSubmitOperator(
            task_id="run_silver_tests",
            application=f"{SCRIPTS_PATH}/tests/test_discharges_silver.py",
            conn_id="compute_engine",
            executor_memory='4g',
            executor_cores=1,
            total_executor_cores=1,
            verbose=True,
            exec_flag="{{ params.compute.run_silver_tests }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )   

        run_gold_transformations = SkippableSparkSubmitOperator(
            task_id="run_gold_transformations",
            application=f"{SCRIPTS_PATH}/spark/load_discharges_gold.py",
            conn_id="compute_engine",
            executor_memory='4g',
            executor_cores=1,
            total_executor_cores=1,
            verbose=True,
            exec_flag="{{ params.compute.run_gold_transformations }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        ) 

        run_gold_tests = SkippableSparkSubmitOperator(
            task_id="run_gold_tests",
            application=f"{SCRIPTS_PATH}/tests/test_discharges_gold.py",
            conn_id="compute_engine",
            executor_memory='4g',
            executor_cores=1,
            total_executor_cores=1,
            verbose=True,
            exec_flag="{{ params.compute.run_gold_tests }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )  

        run_raw_transformations >> run_raw_tests >> \
        run_silver_transformations >> run_silver_tests >> \
        run_gold_transformations >> run_gold_tests

    return tg