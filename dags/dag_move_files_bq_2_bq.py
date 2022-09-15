from airflow import DAG
from datetime import timedelta
import airflow
from airflow.operators.python_operator import PythonOperator

import my_tasks.task_move_files_bq_2_bq as task_move_files_bq_2_bq
import my_config.config_move_files_bq_2_bq as config
import my_config.config_project as config_project
from utils.bq_utils import get_bq_client as get_bq_client

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


job_variables = {'client': get_bq_client(),
                 'region': config_project.REGION,
                 'dry_run': config.DRY_RUN,
                 }

dag = DAG('move_files_bq_2_bq',
          default_args=default_args,
          start_date=airflow.utils.dates.days_ago(1),
          catchup=False,
          schedule_interval=None)

move_files = PythonOperator(
    task_id='move_files_bq_2_bq',
    python_callable=task_move_files_bq_2_bq.insert_order_records_to_bq,
    op_kwargs=job_variables,
    dag=dag)
