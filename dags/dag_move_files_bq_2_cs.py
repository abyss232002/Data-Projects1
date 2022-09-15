from airflow import DAG
from datetime import timedelta
import airflow
from airflow.operators.python_operator import PythonOperator

from my_tasks import task_move_files_bq_2_cs
from my_config import config_move_files_bq_2_cs as config
from my_config import config_project
from utils.bq_utils import get_bq_client


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


job_variables = {'client': get_bq_client(),
                 'region': config_project.REGION,
                 'out_bucket': config.OUT_BUCKET,
                 'out_folder': config.OUT_FOLDER,
                 'out_file_name': config.OUT_FILE_NAME
                 }

dag = DAG('move_files_bq_2_cs',
          default_args=default_args,
          start_date=airflow.utils.dates.days_ago(1),
          catchup=False,
          schedule_interval=None)

move_files = PythonOperator(
    task_id='move_files_bq_2_cs',
    python_callable=task_move_files_bq_2_cs.extract_order_records_from_bq,
    op_kwargs=job_variables,
    dag=dag)

