
"""
@name: etl.py

@author: Mohammad Jeihoonian

Created on Sept 2019
"""

import logging
from datetime import datetime, timedelta
import pandas as pd
from src.data_retrieve import DataRet
from src.brx_data_prep import BrxPrep
from src.trx_data_prep import TrxPrep
from src.data_prep import DataPrep
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExt(object):

    def __init__(self, last_n_weeks, aws_env, calib):

        self.last_n_weeks = last_n_weeks

        self.aws_env = aws_env

        self.calib = calib

        self.end_po = datetime.today().strftime('%Y-%m-%d')

        self.end_pt = self.start_po = (datetime.today() -
                                       timedelta(weeks=self.last_n_weeks)).strftime('%Y-%m-%d')

        self.start_pt = (datetime.strptime(self.start_po, '%Y-%m-%d') -
                         timedelta(weeks=last_n_weeks)).strftime('%Y-%m-%d')

    def extract_transform_load(self, brx_threshold=0.01, trx_threshold=0.01, ext=True):

        sw = Stopwatch(start=True)

        trx_pt = pd.DataFrame()
        brx_pt = pd.DataFrame()
        ads_pt = pd.DataFrame()

        logger.info('Extracting invoice data: {} to {}...'.format(self.start_po, self.end_po))

        DataRet(self.start_po, self.end_po).invoice_ext(table_id='_invoices_po')

        if self.calib:

            logger.info('Extracting training data: {} to {}...'.format(self.start_pt, self.end_pt))

            DataRet(self.start_pt, self.end_pt).invoice_ext(table_id='_invoices')

            trx_prep = TrxPrep(self.start_pt, self.end_pt, trx_threshold, ext=ext)
            trx_pt = trx_prep.trx_data_prep()

            brx_prep = BrxPrep(self.start_pt, self.end_pt, brx_threshold, ext=ext)
            brx_pt, ads_pt, _ = brx_prep.brx_data_prep()

        logger.info('Extracting observation data: {} to {}...'.format(self.start_po, self.end_po))

        trx_prep = TrxPrep(self.start_po, self.end_po, trx_threshold, ext=False)
        trx_po = trx_prep.trx_data_prep()

        brx_prep = BrxPrep(self.start_po, self.end_po, brx_threshold, ext=False)
        brx_po, ads_po, date_po = brx_prep.brx_data_prep()

        data_prep = DataPrep(trx_pt, brx_pt, ads_pt, trx_po, brx_po, ads_po, date_po,
                             test_size=0.20, aws_env=self.aws_env)
        data_prep.prep(self.calib)

        logger.info('Elapsed time of ETL job: {}'.format(sw.elapsed.human_str()))