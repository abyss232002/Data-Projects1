import os
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta

default_args = {
    "start_date": airflow.utils.dates.days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Extract dag id from dag file name
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

dag = DAG(
    DAG_ID,
    default_args=default_args,
    description="liveness monitoring dag",
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=20),
)

# priority_weight has type int in Airflow DB, uses the maximum.
t1 = BashOperator(
    task_id="echo",
    bash_command="echo test",
    dag=dag,
    depends_on_past=False,
    priority_weight=2 ** 31 - 1,
)
