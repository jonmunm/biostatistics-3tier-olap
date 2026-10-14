from typing import Sequence, Any
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.exceptions import AirflowSkipException

class SkippableSparkSubmitOperator(SparkSubmitOperator):
    """
    Custom Airflow Spark submit operator that allows skipping a task based on 
    global and local flags evaluated dynamically using Jinja templates.
    """
    template_fields: Sequence[str] = tuple(list(SparkSubmitOperator.template_fields) + ["global_flag", "exec_flag"])
    custom_operator_name = "SkippableSparkSubmitOperator"
    ui_color = "#fff2e6" 
    ui_fgcolor = "#000"

    def __init__(self, global_flag: str, exec_flag: str, **kwargs):
        """
        Initializes the Spark submit operator with execution control flags.

        :param global_flag: Global flag that enables or disables entire modules.
        :param exec_flag: Task-specific flag to condition its execution.
        :param kwargs: Additional arguments passed to the SparkSubmitOperator constructor.
        """
        super().__init__(**kwargs)
        self.exec_flag = exec_flag
        self.global_flag = global_flag

    def execute(self, context: Any) -> Any:
        """
        Evaluates the flags during task execution before triggering the Spark job. 
        Raises an AirflowSkipException if any of the flags indicate the task should be skipped.

        :param context: Context dictionary provided by Airflow when executing the operator.
        :return: The result of the parent execute method if not skipped.
        :raises AirflowSkipException: If the global module or the specific task is disabled.
        """
        global_result = str(self.global_flag).lower().strip()
        if global_result in ["false", "0", "no", "n", "none", ""]:
            raise AirflowSkipException(f"Skipping task because the global module is disabled.")

        jinja_result = str(self.exec_flag).lower().strip()
        if jinja_result in ["false", "0", "no", "n", "none", ""]:
            raise AirflowSkipException(f"Skipping task: {jinja_result}")

        return super().execute(context)