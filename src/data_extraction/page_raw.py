
"""
@name: page_raw.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

from src.data_extraction.task.query_task import BigQueryConnector
from src.utils.path_helper import get_query


class PageRaw(BigQueryConnector):

    def __init__(self, dataset_id, ext, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.colnames = colnames
        self.ext = ext

    def query(self):
        if self.ext:
            return (get_query('page_raw_pt.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    .replace('@start_date', self.start_date)
                    .replace('@end_date', self.end_date)
                    )
        else:
            return (get_query('page_raw_po.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    .replace('@start_date', self.start_date)
                    .replace('@end_date', self.end_date)
                    )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )

