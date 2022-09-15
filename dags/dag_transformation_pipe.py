from airflow import DAG
from datetime import timedelta
import airflow
from airflow.operators.python_operator import PythonOperator

from my_tasks import task_move_files_cs_2_cs
from my_config import config_move_files_cs_2_cs as config_cs_2_cs

from my_tasks import task_move_files_cs_2_bq
from my_config import config_move_files_cs_2_bq as config_cs_2_bq

from my_tasks import task_move_files_bq_2_bq
from my_config import config_move_files_bq_2_bq as config_bq_2_bq

from my_tasks import task_move_files_bq_2_cs
from my_config import config_move_files_bq_2_cs as config_bq_2_cs

from my_config import config_project
from utils.bq_utils import get_bq_client

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

variables_cs_2_cs = {'source_bucket': config_cs_2_cs.source_bucket,
                     'source_folder': config_cs_2_cs.source_folder,
                     'target_bucket': config_cs_2_cs.target_bucket,
                     'target_folder': config_cs_2_cs.target_folder,
                     'file_pattern': config_cs_2_cs.file_pattern
                     }

variables_cs_2_bq = {'source_bucket': config_cs_2_bq.source_bucket,
                     'source_folder': config_cs_2_bq.source_folder,
                     'file_pattern': config_cs_2_bq.file_pattern,
                     'bq_dataset': config_cs_2_bq.bq_database,
                     'bq_table': config_cs_2_bq.bq_table,
                     'bq_table_disp': config_cs_2_bq.bq_table_disp,
                     }

variables_bq_2_bq = {'client': get_bq_client(),
                     'region': config_project.REGION,
                     'dry_run': config_bq_2_bq.DRY_RUN,
                     }

variables_bq_2_cs = {'client': get_bq_client(),
                     'region': config_project.REGION,
                     'out_bucket': config_bq_2_cs.OUT_BUCKET,
                     'out_folder': config_bq_2_cs.OUT_FOLDER,
                     'out_file_name': config_bq_2_cs.OUT_FILE_NAME
                     }

dag = DAG('transformation_pipe',
          default_args=default_args,
          start_date=airflow.utils.dates.days_ago(1),
          catchup=False,
          schedule_interval=None)

task_cs_2_cs = PythonOperator(
    task_id='move_files_cs_2_cs',
    python_callable=task_move_files_cs_2_cs.execute_move_files,
    op_kwargs=variables_cs_2_cs,
    dag=dag)

task_cs_2_bq = PythonOperator(
    task_id='move_files_cs_2_bq',
    python_callable=task_move_files_cs_2_bq.load_df_to_bq,
    op_kwargs=variables_cs_2_bq,
    dag=dag)

task_bq_2_bq = PythonOperator(
    task_id='move_files_bq_2_bq',
    python_callable=task_move_files_bq_2_bq.insert_order_records_to_bq,
    op_kwargs=variables_bq_2_bq,
    dag=dag)

task_bq_2_cs = PythonOperator(
    task_id='move_files_bq_2_cs',
    python_callable=task_move_files_bq_2_cs.extract_order_records_from_bq,
    op_kwargs=variables_bq_2_cs,
    dag=dag)

task_cs_2_cs >> task_cs_2_bq >> task_bq_2_bq >> task_bq_2_cs
