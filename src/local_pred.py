
"""
@name: local_pred.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

import argparse
import logging
import xgboost as xgb
from src.etl import DataExt
from src.utils.path_helper import *
from src.utils.resources import *
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class LocalPred(object):

    def __init__(self, model_name):

        self.model_name = model_name

    @staticmethod
    def fetch_pred_data(filename):

        logger.info('Fetching prediction data...')

        X_pred = load(get_data_dir(filename))

        return xgb.DMatrix(data=X_pred.values)

    def load_model(self):

        logger.info('Loading {} model...'.format(self.model_name))

        model = load(get_model_dir(self.model_name + '-model'))

        return model

    def predict(self):

        dpred = LocalPred.fetch_pred_data(filename='X_pred.pkl')

        model = self.load_model()

        logger.info('Prediction...')

        y_pred = model.predict(dpred)

        logger.info('y_pred shape: {}'.format(y_pred.shape[0]))

        return y_pred


def get_args():
    """
    Return input arguments

    """

    parser = argparse.ArgumentParser(description="Predicting probabilities and monetary values",
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--last_n_weeks', action='store', help="The number of weeks", dest='last_n_weeks', type=int,
                            default=52)

    parser.add_argument('--aws_env', action='store', help="AWS environment", dest='aws_env', type=str,
                            default='ssense-cltv-qa')

    parser.add_argument('--clf_model', action='store', help="Name of classifier", dest='clf_model', type=str,
                            default='clf')

    parser.add_argument('--reg_model', action='store', help="Name of regressor", dest='reg_model', type=str,
                            default='reg')

    return parser.parse_args()


if __name__ == '__main__':

        sw = Stopwatch(start=True)

        args = get_args()

        data_ext = DataExt(last_n_weeks=args.last_n_weeks, aws_env=args.aws_env, calib=False)

        data_ext.extract_transform_load()

        ml_pred = LocalPred(model_name=args.clf_model)

        y_prob = ml_pred.predict()

        ml_pred = LocalPred(model_name=args.reg_model)

        y_val = ml_pred.predict()

        logger.info('Total elapsed time: {}'.format(sw.elapsed.human_str()))