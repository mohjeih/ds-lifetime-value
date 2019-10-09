
"""
@name: brx_features.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""


from src.data_extraction.task.query_task import BigQueryConnector
from src.utils.path_helper import get_query


class BrxFeat(BigQueryConnector):
    """

    Aggregate all features

    """

    def __init__(self, dataset_id, ext,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames
        self.ext = ext

    def query(self):
        if self.ext:
            return (get_query('brx_features_pt.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    )
        else:
            return (get_query('brx_features_po.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )