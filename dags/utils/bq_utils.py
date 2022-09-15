from google.cloud import bigquery as bq
from google.cloud.bigquery import WriteDisposition, QueryJobConfig

import my_config.config_project as config
import logging


def get_bq_client():
    return bq.Client()


def job_config_factory(destination=None, write_disposition=WriteDisposition.WRITE_APPEND, dry_run=True):
    return QueryJobConfig(
        destination=destination,
        write_disposition=write_disposition,
        dry_run=dry_run
    )


def dataset_table(dataset, table_name):
    return config.PROJECT + '.' + dataset + '.' + table_name


def run(*args, **kwargs):
    try:
        client = kwargs.get('client')
        query = kwargs.get('query')
        location = kwargs.get('location')
        job_config = kwargs.get('job_config')

        if query is not None and job_config is not None and job_config.dry_run:
            query_job = client.query(query=query, location=location, job_config=job_config)
            logging.info("This query will process {} bytes.".format(human_readable(query_job.total_bytes_processed)))

        if query is not None and job_config is not None and not job_config.dry_run:
            client.query(query=query, location=location, job_config=job_config).result()

        elif query is not None and job_config is None:
            return client.query(query=query, location=location).result()
    except Exception as err:
        raise Exception('Error executing query :{}'.format(err))


def read_query(sql_query, replace_dict):
    with open(sql_query) as f:
        query = f.read()
        for k, v in replace_dict.items():
            query = query.replace(k, v)
    return query


def human_readable(num, suffix='B'):
    # Return human readable size from bytes size
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)
