from airflow import DAG

from datetime import timedelta
import airflow
from airflow.operators.python_operator import PythonOperator
from my_tasks import task_move_files_cs_2_bq
from my_config import config_move_files_cs_2_bq as config

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
                        'file_pattern': config.file_pattern,
                        'bq_dataset': config.bq_database,
                        'bq_table': config.bq_table,
                        'bq_table_disp': config.bq_table_disp,
                        }

dag = DAG('move_files_cs_2_bq',
          default_args=default_args,
          start_date=airflow.utils.dates.days_ago(1),
          catchup=False,
          schedule_interval=None)

move_files = PythonOperator(
    task_id='move_files',
    python_callable=task_move_files_cs_2_bq.load_df_to_bq,
    op_kwargs=move_files_variables,
    dag=dag)
