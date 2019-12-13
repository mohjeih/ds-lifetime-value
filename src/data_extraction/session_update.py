
"""
@name: session_update.py

@author: Mohammad Jeihoonian

Created on Dec 2019
"""


from src.data_extraction.task.query_task import BigQueryConnector
from src.utils.path_helper import get_query


class SessUpdate(BigQueryConnector):
    """

    Take a sample of browsing features

    """

    def __init__(self, dataset_id,
                 table_id, colnames, overwrite):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames
        self.overwrite = overwrite

    def query(self):
        return (get_query('sessionId_update.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query(), self.overwrite)
                )