
"""
@name: data_retrieve.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

import logging
from src.utils.resources import normalize, load_project_id
from src.data_extraction.products import ProductQuery
from src.data_extraction.markdown import MarkdownQuery
from src.data_extraction.resellers import ResellerQuery
from src.data_extraction.employees import EmployeesQuery
from src.data_extraction.invoices import TrxInvoice
from src.utils.bq_helper import export_pandas_to_table

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class DataRet(object):

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.dataset_id = 'ds_sessions_value'
        self.bucket_name = 'ga_ltv'
        self.file_name = 'brx_*.csv'
        self.prefix = 'brx_'
        self.project_id = load_project_id()

    def pd_ext(self, table_id):

        logger.info('Extracting products info... ')

        pd_dataset = ProductQuery(colnames=', '.join(['productID', 'prodCreationDate', 'genderT', 'brand',
                                                      'dept', 'category', 'seasonYear', 'priceCD']),
                                  cols_list=['category']).get()

        pd_dataset.brand = pd_dataset.brand.apply(normalize)

        export_pandas_to_table(self.dataset_id, table_id=table_id, dataset=pd_dataset, project_id=self.project_id)

    def md_ext(self, table_id=None, bq=False):

        logger.info('Extracting markdown info... ')

        md_dataset = MarkdownQuery(self.start_date, self.end_date, colnames=', '.join(['date', 'isMD', 'wave', 'season',
                                                                                       'currentYear'])).get()
        if bq:
            export_pandas_to_table(self.dataset_id, table_id, dataset=md_dataset, project_id=self.project_id)
        else:
            return md_dataset

    def rs_ext(self):

        logger.info('Extracting resellers... ')

        return ResellerQuery(colnames='*').get()

    def em_ext(self, table_id):

        logger.info('Extracting internal employees... ')

        em_dataset = EmployeesQuery(colnames='*').get()

        export_pandas_to_table(self.dataset_id, table_id=table_id, dataset=em_dataset, project_id=self.project_id)

    def invoice_ext(self, table_id):

        logger.info('Extracting invoices... ')

        trx_invoice = TrxInvoice(self.dataset_id, self.start_date, self.end_date,
                                 table_id=table_id, colnames='*')

        trx_invoice.run()

    def mid_ext(self, table_id, dataset):

        logger.info('Uploading memberIDs to {} ...'.format(table_id))

        export_pandas_to_table(self.dataset_id, table_id=table_id, dataset=dataset, project_id=self.project_id)

    def ret(self):
        pass
