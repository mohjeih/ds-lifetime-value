
import io
import logging
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def delete_table(dataset_id: str, table_ids: list):
    """

    Delete the table created in the dataset

    :param dataset_id: str, id of the dataset
    :param table_id: list id of table ids
    """

    try:
        storage_client = bigquery.Client()
        for id in table_ids:
            if id != '_sessionId_update':
                table_ref = storage_client.dataset(dataset_id).table(id)
                # API request
                logger.info('Deleting table {}:{}'.format(dataset_id, id))
                storage_client.delete_table(table_ref)
    except Exception:
        raise ValueError('Desired table does not exist')


def export_pandas_to_table(dataset_id, table_id, dataset, project_id, if_exists='replace'):
    """

    Write dataframe to bigquery

    :param: dataset: pandas dataframe
    :param: if_exists: behavior when the destination table exists
    """

    dataset.to_gbq(destination_table=dataset_id+'.'+table_id, project_id=project_id, if_exists=if_exists)


def remove_file_from_google_storage(bucket_name: str, prefix: str):
    """

    Remove files in Google Cloud Storage bucket
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)

    logger.info('Removing {} files from storage'.format(prefix))
    for blob in blobs:
        blob.delete()


def export_table_to_google_storage(dataset_id: str, table_id: str, bucket_name: str, file_name: str, location='US'):
    """

    Export a table to Google Cloud Storage in csv format

    :param location: str, storage location
    """

    storage_client = bigquery.Client()
    destination_uri = 'gs://{}/{}'.format(bucket_name, file_name)
    dataset_ref = storage_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    logger.info('Exporting {} to {}'.format(table_id, destination_uri))

    extract_job = storage_client.extract_table(
        table_ref,
        destination_uri,
            # Location must match that of the source table.
        location=location)  # API request
    extract_job.result()  # Waits for job to complete.


def download_from_storage_to_pandas(bucket_name: str, prefix: str, col_type: dict) -> pd.DataFrame:
    """

    download data files from storage
    """

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blobs = bucket.list_blobs(prefix=prefix)

    list_temp_raw = []
    for file in blobs:
        logger.info('Fetching {}: '.format(bucket_name + '/' + file.name))
        file_contents = file.download_as_string()
        temp = pd.read_csv(io.BytesIO(file_contents), encoding='utf-8', dtype=col_type)
        list_temp_raw.append(temp)

    dataset = pd.concat(list_temp_raw, sort=True)

    return dataset

