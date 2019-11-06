
"""
@name: ad_users.py

@author: Mohammad Jeihoonian

Created on Nov 2019
"""


from src.data_extraction.task.query_task import BigQueryConnector
from src.utils.path_helper import get_query


class AdUsers(BigQueryConnector):
    """

    Extract users and ads related data

    """

    def __init__(self, dataset_id, ext,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames
        self.ext = ext

    def query(self):
        if self.ext:
            return (get_query('ad_users_pt.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    )
        else:
            return (get_query('ad_users_po.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )