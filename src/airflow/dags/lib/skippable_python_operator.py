from typing import Sequence, Any
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

class SkippablePythonOperator(PythonOperator):
    template_fields: Sequence[str] = tuple(list(PythonOperator.template_fields) + ["global_flag", "exec_flag"])
    custom_operator_name = "SkippablePythonOperator"
    ui_color = "#fff2e6" 
    ui_fgcolor = "#000"

    def __init__(self, global_flag: str, exec_flag: str, **kwargs):
        super().__init__(**kwargs)
        self.exec_flag = exec_flag
        self.global_flag = global_flag

    def execute(self, context: Any) -> Any:
        global_result = str(self.global_flag).lower().strip()
        if global_result in ["false", "0", "no", "n", "none", ""]:
            raise AirflowSkipException(f"Skipping task because the global module is disabled.")

        jinja_result = str(self.exec_flag).lower().strip()
        if jinja_result in ["false", "0", "no", "n", "none", ""]:
            raise AirflowSkipException(f"Skipping task: {jinja_result}")

        return super().execute(context)