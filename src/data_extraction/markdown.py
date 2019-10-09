
"""
@name: markdown.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

from src.data_extraction.task.query_task import GenericDataClass
from src.data_extraction.task.query_task import ConnectorSQL
from src.utils.path_helper import QUERY_DIR


class MarkdownQuery(GenericDataClass):

    def __init__(self, start_date: str, end_date: str, colnames: str='*'):
        super().__init__(
            ConnectorSQL('stats'),
            QUERY_DIR / 'markdown.sql',
            {'@colnames': colnames,
             '@start_date': start_date,
              '@end_date': end_date}
        )