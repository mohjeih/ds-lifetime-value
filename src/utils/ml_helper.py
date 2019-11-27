
"""
@name: ml_helper.py

@author: Mohammad Jeihoonian

Created on Nov 2019
"""

import logging
import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix, mean_absolute_error

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def eval_metric(val, pred, agg=False):

    if not agg:
        mae = np.round(mean_absolute_error(val, pred), 2)
        mape = np.round(np.mean(np.abs(val - pred) / val), 2)
        logger.info('MAE: {}'.format(mae))
        logger.info('MAPE: {}'.format(mape))
        return mae, mape
    else:
        mae = np.round(mean_absolute_error(val, pred), 2)
        logger.info('MAE: {}'.format(mae))
        return mae


def conf_matrix(val, pred, normalize=True):

    cm = confusion_matrix(val, pred, labels=[1, 0])

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]

    return pd.DataFrame(
        data=cm,
        index=['Actual:active', 'Actual:inactive'],
        columns=['Prediction:active', 'Prediction:inactive']
    )