
"""
@name: session_features.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

import datetime
import pandas as pd
from src.data_extraction.task.query_task import BigQueryConnector
from src.utils.path_helper import get_query


class SessionFeat(BigQueryConnector):
    """

    Feature engineering on session features

    """

    def __init__(self, dataset_id, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.last_quarter_date = (pd.to_datetime(self.end_date) - datetime.timedelta(weeks=12)).strftime('%Y-%m-%d')
        self.colnames = colnames

    def query(self):
        return (get_query('session_features.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                .replace('@start_date', self.start_date)
                .replace('@end_date', self.end_date)
                .replace('@last_quarter_date', self.last_quarter_date)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )