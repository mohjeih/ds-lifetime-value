
"""
@name: main.py

@author: Mohammad Jeihoonian

Created on Sept 2019
"""

import logging
from datetime import datetime, timedelta
from timeutils import Stopwatch
from src.data_retrieve import DataRet
from src.brx_data_prep import BrxPrep
from src.trx_data_prep import TrxPrep
from src.data_prep import DataPrep
from src.utils.resources import dump
from src.utils.path_helper import get_data_dir

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExt(object):

    def __init__(self, last_n_weeks):

        self.last_n_weeks = last_n_weeks

        self.end_po = datetime.today().strftime('%Y-%m-%d')

        self.end_pt = self.start_po = (datetime.today() -
                                       timedelta(weeks=self.last_n_weeks)).strftime('%Y-%m-%d')

        self.start_pt = (datetime.strptime(self.start_po, '%Y-%m-%d') -
                         timedelta(weeks=last_n_weeks)).strftime('%Y-%m-%d')

    def extract_transform_load(self, brx_threshold=0.01, trx_threshold=0.01, ext=True):

        sw = Stopwatch(start=True)

        file_name = {
            'trx_po': 'trx_po.pkl',
            'trx_pt': 'trx_pt.pkl',
            'brx_po': 'brx_po.pkl',
            'brx_pt': 'brx_pt.pkl'
        }

        # logger.info('Extracting invoice data: {} to {} ...'.format(self.start_po, datetime.today()))
        #
        # DataRet(self.start_po, self.end_po).invoice_ext(table_id='_invoices_po')

        logger.info('Extracting training data: {} to {} ...'.format(self.start_pt, self.end_pt))

        DataRet(self.start_pt, self.end_pt).invoice_ext(table_id='_invoices')

        trx_prep = TrxPrep(self.start_pt, self.end_pt, trx_threshold, ext=ext)
        trx_feats = trx_prep.trx_data_prep()
        dump(trx_feats, get_data_dir(file_name['trx_pt']))

        brx_prep = BrxPrep(self.start_pt, self.end_pt, brx_threshold, ext=ext)
        brx_feats = brx_prep.brx_data_prep()
        dump(brx_feats, get_data_dir(file_name['brx_pt']))

        logger.info('Extracting observation data: {} to {} ...'.format(self.start_po, self.end_po))

        trx_prep = TrxPrep(self.start_po, self.end_po, trx_threshold, ext=False)
        trx_feats = trx_prep.trx_data_prep()
        dump(trx_feats, get_data_dir(file_name['trx_po']))

        data_prep = DataPrep(file_name=file_name, test_size=0.20)
        data_prep.prep()

        logger.info('Elapsed time of ETL job: {}'.format(sw.elapsed.human_str()))


if __name__ == '__main__':

    sw = Stopwatch(start=True)

    data_ext = DataExt(last_n_weeks=52)

    data_ext.extract_transform_load()

    logger.info('Total elapsed time: {}'.format(sw.elapsed.human_str()))


