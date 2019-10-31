
"""
@name: main.py

@author: Mohammad Jeihoonian

Created on Sept 2019
"""

import argparse
import logging
from datetime import datetime, timedelta
from src.data_retrieve import DataRet
from src.brx_data_prep import BrxPrep
from src.trx_data_prep import TrxPrep
from src.data_prep import DataPrep
from src.params.model_params import *
from src.remote_model import RemoteModel
from timeutils import Stopwatch

# from src.utils.resources import dump
# from src.utils.path_helper import get_data_dir

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class DataExt(object):

    def __init__(self, last_n_weeks, aws_env):

        self.last_n_weeks = last_n_weeks

        self.aws_env = aws_env

        self.end_po = datetime.today().strftime('%Y-%m-%d')

        self.end_pt = self.start_po = (datetime.today() -
                                       timedelta(weeks=self.last_n_weeks)).strftime('%Y-%m-%d')

        self.start_pt = (datetime.strptime(self.start_po, '%Y-%m-%d') -
                         timedelta(weeks=last_n_weeks)).strftime('%Y-%m-%d')

    def extract_transform_load(self, brx_threshold=0.01, trx_threshold=0.01, ext=True):

        sw = Stopwatch(start=True)

        # file_name = {
        #     'trx_pt': 'trx_pt.pkl',
        #     'trx_po': 'trx_po.pkl',
        #     'brx_pt': 'brx_pt.pkl',
        #     'brx_po': 'brx_po.pkl'
        # }

        logger.info('Extracting invoice data: {} to {}...'.format(self.start_po, datetime.today()))

        DataRet(self.start_po, self.end_po).invoice_ext(table_id='_invoices_po')

        logger.info('Extracting training data: {} to {}...'.format(self.start_pt, self.end_pt))

        DataRet(self.start_pt, self.end_pt).invoice_ext(table_id='_invoices')

        trx_prep = TrxPrep(self.start_pt, self.end_pt, trx_threshold, ext=ext)
        trx_pt = trx_prep.trx_data_prep()
        # dump(trx_pt, get_data_dir(file_name['trx_pt']))

        brx_prep = BrxPrep(self.start_pt, self.end_pt, brx_threshold, ext=ext)
        brx_pt = brx_prep.brx_data_prep()
        # dump(brx_pt, get_data_dir(file_name['brx_pt']))

        logger.info('Extracting observation data: {} to {}...'.format(self.start_po, self.end_po))

        trx_prep = TrxPrep(self.start_po, self.end_po, trx_threshold, ext=False)
        trx_po = trx_prep.trx_data_prep()
        # dump(trx_po, get_data_dir(file_name['trx_po']))

        brx_prep = BrxPrep(self.start_po, self.end_po, brx_threshold, ext=False)
        brx_po = brx_prep.brx_data_prep()
        # dump(brx_po, get_data_dir(file_name['brx_po']))

        data_prep = DataPrep(trx_pt, brx_pt, trx_po, brx_po, test_size=0.30, aws_env=self.aws_env)
        imb_ratio = data_prep.prep()

        logger.info('Elapsed time of ETL job: {}'.format(sw.elapsed.human_str()))

        return imb_ratio


class BoostingModel(object):

    def __init__(self, model_name, imb_ratio, aws_env):

        self.model_name = model_name
        self.imb_ratio = imb_ratio
        self.aws_env = aws_env

    def booster(self):

        if self.model_name == 'clf':

            ml_obj = RemoteModel(self.model_name, self.aws_env, params=CLF_PARAM, imb_ratio=self.imb_ratio)

            ml_obj.train()

            y_pred = ml_obj.predict()

        else:

            ml_obj = RemoteModel(self.model_name, self.aws_env, params=REG_PARAM, imb_ratio=None)

            ml_obj.train()

            y_pred = ml_obj.predict()

        return y_pred


def get_args():
    """
    Call input arguments

    """

    parser = argparse.ArgumentParser(description="Building CLTV predictive model",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--aws_env', action='store', help="AWS environment", dest='aws_env', type=str,
                        default='ssense-recommendation-prod')  # ssense-cltv-qa

    parser.add_argument('--clf_model', action='store', help="Name of classifier", dest='clf_model', type=str,
                        default='clf')

    parser.add_argument('--reg_model', action='store', help="Name of regressor", dest='reg_model', type=str,
                        default='reg')

    return parser.parse_args()


if __name__ == '__main__':

    sw = Stopwatch(start=True)

    args = get_args()

    # data_ext = DataExt(last_n_weeks=52, aws_env=args.aws_env)

    # imb_ratio = data_ext.extract_transform_load()

    # clf_ml = BoostingModel(model_name=args.clf_model, imb_ratio=40.9, aws_env=args.aws_env)
    #
    # y_pred = clf_ml.booster()

    reg_ml = BoostingModel(model_name=args.reg_model, imb_ratio=40.9, aws_env=args.aws_env)

    y_pred = reg_ml.booster()

    logger.info('Total elapsed time: {}'.format(sw.elapsed.human_str()))


