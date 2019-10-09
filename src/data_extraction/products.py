
"""
@name: products.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

from src.data_extraction.task.query_task import GenericDataClass
from src.data_extraction.task.query_task import ConnectorSQL
from src.utils.path_helper import QUERY_DIR


class ProductQuery(GenericDataClass):
    """

    All information about the products. See Products.sql for list of fiels.


    """

    def __init__(self, colnames: str='*', cols_list=list()):
        """

        Class contructor

        :param colnames: str, list of columns to subset on

        """
        super().__init__(
            ConnectorSQL('ssense'),
            QUERY_DIR / 'Products.sql',
            {'@colnames': colnames},
            cols_list
        )
