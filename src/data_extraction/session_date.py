
"""
@name: session_date.py

@author: Mohammad Jeihoonian

Created on Jan 2020
"""


from src.data_extraction.task.query_task import BigQueryConnector
from src.utils.path_helper import get_query


class SessDate(BigQueryConnector):
    """

    Keep track of session dates in the period of prediction

    """

    def __init__(self, dataset_id,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames

    def query(self):
        return (get_query('session_date_po.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )