
"""
@name: query_task.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

import json
import logging
import os
import pandas as pd
from abc import ABCMeta, ABC, abstractmethod
from google.cloud import bigquery
from typing import Dict
from src.utils.path_helper import get_config
from src.utils.db_engine import get_data_slave

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

if 'GOOGLE_APPLICATION_CREDENTIALS' not in os.environ:
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = (
        str(get_config('ssense-3c92053ad127.json'))
    )


class Connector(ABC):
    """

    Generic abstract class connector


    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def run(self, *args):
        pass


class BigQueryConnector(Connector):
    """

    Big Query connector

    """

    def __init__(self, dataset_id: str):
        super().__init__()
        self.dataset_id = dataset_id
        with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS']) as secret:
            self.project_id = json.loads(secret.read())['project_id']

    def create_table(self, table_id: str, query: str, overwrite: bool):
        """

        Extract data from BigQuery and store it into a table

        :param query
        """

        try:
            assert self.dataset_id != '843777'
            storage_client = bigquery.Client()
            job_config = bigquery.QueryJobConfig()
            table_ref = storage_client.dataset(self.dataset_id).table(table_id)
            job_config.destination = table_ref
            job_config.allow_large_results = True
            if overwrite:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            else:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            # API request - starts the query
            query_job = storage_client.query(
                query, location='US', job_config=job_config)
            # Waits for job to complete.
            logger.info(
                'Loading results into table {}:{}'.format(self.dataset_id, table_id))
            query_job.result()
        except Exception:
            raise ValueError('Unable to load results into table {}:{}'.format(self.dataset_id, table_id))

    def run(self, table_id, query, overwrite=True):

        self.create_table(table_id, query, overwrite=overwrite)


class ConnectorSQL(Connector):
    """

    MySQL connector


    """
    def __init__(self, db_name: str):
        """

        Class initiator

        :param db_name, str, name of the database
        """
        super().__init__()
        self.db = db_name

    def run(self, query: str, cols_list=list()) -> pd.DataFrame:
        """

        Call MySQL db and retrieved data

        :param query: str, a query

        :returns extracted data

        """

        return get_data_slave(self.db, query, cols_list)


class GenericDataClass:
    """

    Class GenericDataClass is a general object that builds a dataframe from any
     extract


    """

    def __init__(self,
                 db_connector: Connector,
                 query_file: str,
                 replace_sql: Dict = {},
                 *args):
        """

        Class contructor

        :param db_connector: query connector
        :param query_file: str, a filename where to find the query
        :param replace_sql: Dict, a dictionnary with values to replace in
                sql query

        """

        self.set_query(query_file, replace_sql)

        self.table = db_connector.run(self.query, *args)

    def set_query(self, query_file: str, replace_sql: Dict = {}):
        """

        Class member accessor

        :param query_file: str, a filename where to find the query
        :param replace_sql: Dict, a dictionary with values to replace in
                sql query

        """

        self.query = open(query_file).read()

        for key, value in replace_sql.items():
            self.query = self.query.replace(str(key), str(value))

    def get(self) -> pd.DataFrame:
        """

        Class member accessor

        :returns a dataframe with the extracted data

        """
        return self.table




