
"""
@name: carts.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""


from src.data_extraction.task.query_task import GenericDataClass
from src.data_extraction.task.query_task import ConnectorSQL
from src.utils.path_helper import QUERY_DIR


class CartsQuery(GenericDataClass):

    def __init__(self, start_date: str, end_date: str, colnames: str = '*', cols_list=list()):
        super().__init__(
            ConnectorSQL('ssense'),
            QUERY_DIR / 'carts.sql',
            {'@colnames': colnames,
             '@start_date': start_date,
              '@end_date': end_date},
            cols_list
        )