
"""
@name: employees.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""


from src.data_extraction.task.query_task import GenericDataClass
from src.data_extraction.task.query_task import ConnectorSQL
from src.utils.path_helper import QUERY_DIR


class EmployeesQuery(GenericDataClass):

    def __init__(self, colnames: str='*', cols_list=list()):
        super().__init__(
            ConnectorSQL('ssense'),
            QUERY_DIR / 'employees.sql',
            {'@colnames': colnames},
            cols_list
        )
