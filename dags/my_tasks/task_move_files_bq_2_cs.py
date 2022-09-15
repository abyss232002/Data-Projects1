import logging

import my_config.config_move_files_bq_2_bq as config
import utils.bq_utils as utils

code_dir = "/home/airflow/gcs/dags/"
bql_path = code_dir + "bql/"


def extract_order_records_from_bq(client, region, out_bucket, out_folder, out_file_name):
    logging.info('Extracting records from BQ table')
    extracted_df = utils.run(client=client,
                             location=region,
                             query=utils.read_query(
                                 sql_query=bql_path + 'extract_US_500_records.sql',
                                 replace_dict={
                                     '**IN_TABLE**': utils.dataset_table(config.IN_DATASET, config.IN_TABLE),
                                 }
                             ),
                             ).to_dataframe()

    print(extracted_df.head())
    extracted_df.to_csv('gs://' + out_bucket + '/' + out_folder + '/' + out_file_name, index=False)
