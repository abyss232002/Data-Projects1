from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import my_config.config_move_files_cs_2_bq as config
import json

code_dir = "/home/airflow/gcs/dags/"
# schema_file = config.order_schema


def list_my_blobs(bucket_name, folder_name, file_pattern):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"
    file_list = []
    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    # blobs = storage_client.list_blobs(bucket_name, prefix=folder_name + '/' + file_pattern)
    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name)

    for file in blobs:
        if '.' in file.name and file_pattern in file.name:
            file_list.append(file.name)
            print(file.name)

    return file_list


def move_cs_files_to_dataframe(source_bucket, source_folder, my_file_pattern):
    # with open(code_dir + schema_file, "r") as f:
    #     schema = json.load(f)
    #     column_list = [i['name'] for i in schema]
    # final_df = pd.DataFrame(columns=column_list)

    # # Using wildcard character to read files instead of blobs
    # df = pd.read_csv(f'gs://{source_bucket}/{source_folder}/{my_file_pattern}*.csv', header=0, sep=',', encoding='utf-8')
    # return df

    final_df = pd.DataFrame()

    x = list_my_blobs(source_bucket, source_folder, my_file_pattern)
    for blob in x:
        print('gs://' + source_bucket + '/' + blob)
        df = pd.read_csv('gs://' + source_bucket + '/' + blob, header=0, sep=',', encoding='utf-8')
        final_df = final_df.append(df)

    return final_df




def load_df_to_bq(**kwargs):
    source_bucket = kwargs.get('source_bucket')
    source_folder = kwargs.get('source_folder')
    my_file_pattern = kwargs.get('file_pattern')
    dataset = kwargs.get('bq_dataset')
    table = kwargs.get('bq_table')
    table_disp = kwargs.get('bq_table_disp')

    dataframe = move_cs_files_to_dataframe(source_bucket, source_folder, my_file_pattern)

    # # Load data to Bigquery time partitioned table
    # table_id = dataset + '.' + table + '$' + str(load_date.strftime('%Y%m%d'))

    # Load data to non partitioned table
    table_id = dataset + '.' + table

    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(write_disposition=table_disp)

    # No of rows in current dataframe.
    rows = dataframe.shape[0]

    try:
        # Load DataFrame to BQ
        job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)

        # Wait for the job to complete and check no of rows uploaded.
        job.result()
        table_load = client.get_table(table_id)

        if table_load.num_rows != rows:
            raise Exception('Of {} rows, {} rows were loaded to BQ'.format(rows, table_load.num_rows))
        else:
            print("Successfully loaded file to BQ table {} with {} rows and {} columns"
                  .format(table_id, table_load.num_rows, len(table_load.schema)))
    except Exception as err:
        raise Exception('File loading from CS to BQ failed! :{}'.format(err))
