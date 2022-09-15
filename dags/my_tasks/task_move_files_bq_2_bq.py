import logging
from google.cloud.bigquery import WriteDisposition

import my_config.config_move_files_bq_2_bq as config
import utils.bq_utils as utils

code_dir = "/home/airflow/gcs/dags/"
bql_path = code_dir + "bql/"


def insert_order_records_to_bq(client, region, dry_run=True):
    logging.info('Inserting records to BQ table')
    utils.run(client=client,
              location=region,
              query=utils.read_query(
                  sql_query=bql_path + 'insert_us_500_records.sql',
                  replace_dict={
                      '**IN_TABLE**': utils.dataset_table(config.IN_DATASET, config.IN_TABLE),
                      '**STATE**': config.STATE
                  }
              ),
              job_config=utils.job_config_factory(
                  destination=utils.dataset_table(config.OUT_DATASET, config.OUT_TABLE),
                  write_disposition=WriteDisposition.WRITE_TRUNCATE,
                  dry_run=dry_run
              ),
              )
