
"""
@name: brx_features.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""


from src.data_extraction.task.query_task import BigQueryConnector
from src.utils.path_helper import get_query


class BrxSamples(BigQueryConnector):
    """

    Take a sample of browsing features

    """

    def __init__(self, dataset_id, ext,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames
        self.ext = ext

    def query(self):
        return (get_query('brx_samples.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )