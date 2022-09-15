from airflow import DAG

from datetime import timedelta
import airflow
from airflow.operators.python_operator import PythonOperator
from my_tasks import task_move_files_cs_2_cs
from my_config import config_move_files_cs_2_cs as config

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

move_files_variables = {'source_bucket': config.source_bucket,
                        'source_folder': config.source_folder,
                        'target_bucket': config.target_bucket,
                        'target_folder': config.target_folder,
                        'file_pattern': config.file_pattern
                        }

dag = DAG('move_files_cs_2_cs',
          default_args=default_args,
          start_date=airflow.utils.dates.days_ago(1),
          catchup=False,
          schedule_interval=None)

move_files = PythonOperator(
    task_id='move_files',
    python_callable=task_move_files_cs_2_cs.execute_move_files,
    op_kwargs=move_files_variables,
    dag=dag)
