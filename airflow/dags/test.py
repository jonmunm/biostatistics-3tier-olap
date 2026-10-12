import os
from airflow.sdk import dag, task, task_group, get_current_context, Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException
from lib.skippable_empty_operator import SkippableEmptyOperator
from lib.skippable_python_operator import SkippablePythonOperator
from lib.skippable_spark_submit_operator import SkippableSparkSubmitOperator

@dag(
    dag_id="test",
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
                "get_dimensions_list": True,
                "load_dimensions": True,
                "load_fact_tables": False
            },
            type="object"
        ),
        "bi_artifacts_deployment": Param(
            {
                "run": True,
                "deploy_dimensional_model": True,
                "deploy_test_files": True,
                "run_cda_tests": True
            },
            type="object"
        )
    }
)
def test():
   
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
    
    @task.short_circuit(
        task_id="run_only_bi_artifacts_deployment",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        ignore_downstream_trigger_rules=False
    )
    def run_only_bi_artifacts_deployment(**context):
        params = context["params"]
        return params["bi_artifacts_deployment"].get("run", False)

    @task_group(group_id="db_initialization_group")
    def db_initialization_group():
        initialize_db = EmptyOperator(
            task_id="initialize_db"
        )

    @task_group(group_id="compute_group")
    def compute_group():

        run_raw_transformations = SkippableEmptyOperator(
            task_id="run_raw_transformations",
            exec_flag="{{ params.compute.run_raw_transformations }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )    

        run_raw_tests = SkippableEmptyOperator(
            task_id="run_raw_tests",
            exec_flag="{{ params.compute.run_raw_tests }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )     

        run_silver_transformations = SkippableEmptyOperator(
            task_id="run_silver_transformations",
            exec_flag="{{ params.compute.run_silver_transformations }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )   

        run_silver_tests = SkippableEmptyOperator(
            task_id="run_silver_tests",
            exec_flag="{{ params.compute.run_silver_tests }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )   

        run_gold_transformations = SkippableEmptyOperator(
            task_id="run_gold_transformations",
            exec_flag="{{ params.compute.run_gold_transformations }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        ) 

        run_gold_tests = SkippableEmptyOperator(
            task_id="run_gold_tests",
            exec_flag="{{ params.compute.run_gold_tests }}",
            global_flag="{{ params.compute.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )  

        run_raw_transformations >> run_raw_tests >> \
        run_silver_transformations >> run_silver_tests >> \
        run_gold_transformations >> run_gold_tests
    
    @task_group(group_id="bi_artifacts_deployment_group")
    def bi_artifacts_deployment_group():

        deploy_dimensional_model = SkippableEmptyOperator(
            task_id="deploy_dimensional_model",
            exec_flag="{{ params.bi_artifacts_deployment.deploy_dimensional_model }}",
            global_flag="{{ params.bi_artifacts_deployment.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )

        deploy_test_files = SkippableEmptyOperator(
            task_id="deploy_test_files",
            exec_flag="{{ params.bi_artifacts_deployment.deploy_test_files }}",
            global_flag="{{ params.bi_artifacts_deployment.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )

        run_cda_tests = SkippableEmptyOperator(
            task_id="run_cda_tests",
            exec_flag="{{ params.bi_artifacts_deployment.run_cda_tests }}",
            global_flag="{{ params.bi_artifacts_deployment.run }}",
            trigger_rule=TriggerRule.ALL_DONE
        )

        deploy_dimensional_model >> deploy_test_files >> run_cda_tests

    @task_group(group_id="db_ingestion_group")
    def db_ingestion_group():

        def load_dimensions():
            pass
                

        def load_fact_tables():
            pass

        load_dimensions = SkippablePythonOperator(
            python_callable=load_dimensions,
            task_id="load_dimensions", 
            trigger_rule=TriggerRule.ALL_DONE,
            exec_flag="{{ params.db_ingestion.load_dimensions }}",
            global_flag="{{ params.db_ingestion.run }}",
        )

        load_fact_tables = SkippablePythonOperator(
            python_callable=load_fact_tables,
            task_id="load_fact_tables",
            trigger_rule=TriggerRule.ALL_DONE,
            exec_flag="{{ params.db_ingestion.load_fact_tables }}",
            global_flag="{{ params.db_ingestion.run }}",
        )

        load_dimensions >> load_fact_tables

    _validate_params = EmptyOperator(task_id="validate_params")
    
    # _validate_params = validate_params()

    _db_initialization_group = db_initialization_group()
    _compute_group = compute_group()
    _db_ingestion_group = db_ingestion_group()
    _bi_artifacts_deployment_group = bi_artifacts_deployment_group()

    _only_db_initialization = run_only_db_initialization()
    _only_compute = run_only_compute()
    _only_db_ingestion = run_only_db_ingestions()
    _only_bi_artifacts_deployment = run_only_bi_artifacts_deployment()

    # Flujo Lineal y Claro
    start >> _validate_params

    # Encadenamos los bloques de control con sus grupos respectivos
    _validate_params >> _only_db_initialization >> _db_initialization_group
    _validate_params >> _only_compute >> _compute_group
    _validate_params >> _only_db_ingestion >> _db_ingestion_group
    _validate_params >> _only_bi_artifacts_deployment >> _bi_artifacts_deployment_group

    # Para asegurar el orden secuencial que buscas:
    _db_initialization_group >> _only_compute
    _compute_group >> _only_db_ingestion
    _db_ingestion_group >> _only_bi_artifacts_deployment
    _bi_artifacts_deployment_group >> end

test()