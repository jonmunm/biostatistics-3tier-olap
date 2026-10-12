from airflow.sdk import dag, task, Param
from airflow.utils.trigger_rule import TriggerRule
from groups.create_db_initialization_group import create_db_initialization_group
from groups.create_compute_group import create_compute_group
from groups.create_db_ingestion_group import create_db_ingestion_group
from lib.skippable_empty_operator import SkippableEmptyOperator

@dag(
    dag_id="load_discharge",
    params={
        "db_initialization": Param(
            {
                "run": True
            },
            type="object"
        ),
        "compute": Param(
            {
                "run": True,
                "run_raw_transformations": True,
                "run_raw_tests": True,
                "run_silver_transformations": True,
                "run_silver_tests": True,
                "run_gold_transformations": True,
                "run_gold_tests": True
            },
            type="object"
        ),
        "db_ingestion": Param(
            {
                "run": True,
                "load_dimensions": True,
                "load_fact_tables": False
            },
            type="object"
        )
    }
)
def load_discharge():
   
    start = SkippableEmptyOperator(
        task_id="Start", 
        exec_flag=True,
        global_flag=True
    )
    end = SkippableEmptyOperator(
        task_id="End", 
        exec_flag=True, 
        global_flag=True,
        trigger_rule=TriggerRule.NONE_FAILED
    )

    @task.short_circuit(
        task_id="run_only_db_initialization",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        ignore_downstream_trigger_rules=False
    )
    def run_only_db_initialization(**context):
        params = context["params"]
        return params["db_initialization"].get("run", False)

    @task.short_circuit(
        task_id="run_only_compute",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        ignore_downstream_trigger_rules=False
    )
    def run_only_compute(**context):
        params = context["params"]
        return params["compute"].get("run", False)
    
    @task.short_circuit(
        task_id="run_only_db_ingestion",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        ignore_downstream_trigger_rules=False
    )
    def run_only_db_ingestions(**context):
        params = context["params"]
        return params["db_ingestion"].get("run", False)

    _db_initialization_group = create_db_initialization_group(group_id="db_initialization_group")
    _compute_group = create_compute_group(group_id="compute_group")
    _db_ingestion_group = create_db_ingestion_group(group_id="db_ingestion_group")

    _only_db_initialization = run_only_db_initialization()
    _only_compute = run_only_compute()
    _only_db_ingestion = run_only_db_ingestions()

    start >> _only_db_initialization >> _db_initialization_group
    start >> _only_compute >> _compute_group
    start >> _only_db_ingestion >> _db_ingestion_group

    _db_initialization_group >> _only_compute
    _compute_group >> _only_db_ingestion
    _db_ingestion_group >> end

load_discharge()