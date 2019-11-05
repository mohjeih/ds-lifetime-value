
"""
@name: local_pred.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

import logging
import xgboost as xgb
from src.utils.path_helper import *
from src.utils.resources import *

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