
"""
@name: local_train.py

@author: Mohammad Jeihoonian

Created on Nov 2019
"""

from __future__ import print_function

import argparse
import logging
import numpy as np
import os
import pandas as pd
import sys
import traceback
import xgboost as xgb

from sklearn.metrics import f1_score, mean_squared_error
from src.utils.path_helper import *
from src.utils.resources import *
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

prefix = '/aws_opt/aws_ml/'

input_path = prefix + 'input/data'
output_path = os.path.join(prefix, 'output')
model_path = os.path.join(prefix, 'model')

train_channel = 'train'
training_path = input_path + '/' + train_channel + '/'

validation_channel = 'validation'
validation_path = input_path + '/' + validation_channel + '/'


def fetch_data(model_name):

    logger.info('Fetching data...')

    train_file = training_path + model_name + '_train.csv'
    train_data = pd.read_csv(train_file, header=None)

    val_file = validation_path + model_name + '_val.csv'
    val_data = pd.read_csv(val_file, header=None)

    return train_data, val_data


def eval(data, model, model_name):

    y_pred = model.predict(data)

    if model_name == 'clf':
        return np.where(y_pred >= 0.5, 1, 0)
    else:
        return y_pred


def val_metric(y_true, y_est, model_name):

        if model_name == 'clf':
            logger.info('f1 score: {}'.format(f1_score(y_true, y_est)))
        else:
            logger.info('mae score: {}'.format(mean_squared_error(y_true, y_est)))


def train(model_name):

    logger.info('Starting the training...')

    params = dict()

    if model_name == 'clf':
        params['objective'] = ['binary:logistic']
        params['scale_pos_weight'] = load_param_json(get_params_dir('imb_ratio.json'))['imb_ratio']
    else:
        params['objective'] = ['reg:linear']

    train_data, val_data = fetch_data(model_name)

    # training data
    X_train = train_data.iloc[:, 1:].values
    y_train = train_data.iloc[:, 0].values
    dtrain = xgb.DMatrix(data=X_train, label=y_train)

    # validation data
    X_val = val_data.iloc[:, 1:].values
    y_val = val_data.iloc[:, 0].values

    model = xgb.train(params=params, dtrain=dtrain, num_boost_round=params['num_round'])

    # with open(os.path.join(model_path, model_name + '-model.pkl'), 'w') as out:
    #     pickle.dump(model, out)

    logger.info('Calculating the evaluation metric...')

    y_train_val = eval(xgb.DMatrix(data=X_train), model, model_name)

    val_metric(y_train, y_train_val, model_name)

    y_pred_val = eval(xgb.DMatrix(data=X_val), model, model_name)

    val_metric(y_val, y_pred_val, model_name)


def get_args():
    """
    Get input arguments

    """

    parser = argparse.ArgumentParser(description="Executing local training",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--model', action='store', help="Name of model", dest='model', type=str,
                        default='reg')

    return parser.parse_args()


if __name__ == '__main__':

    sw = Stopwatch(start=True)

    args = get_args()

    train(model_name=args.model)

    logger.info('Total elapsed time: {}'.format(sw.elapsed.human_str()))
