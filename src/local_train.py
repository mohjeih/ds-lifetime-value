
"""
@name: local_train.py

@author: Mohammad Jeihoonian

Created on Nov 2019
"""

import logging
import numpy as np
import sys
import xgboost as xgb
from sklearn.metrics import f1_score, classification_report
from src.params.model_params import *
from src.utils.path_helper import get_data_dir, get_model_dir, get_params_dir
from src.utils.ml_helper import conf_matrix, eval_metric
from src.utils.resources import dump, FlushFile, load, load_param_json
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class LocalTrain(object):

    def __init__(self, model_name):

        self.model_name = model_name
        self.params = self._get_params()

    def _get_params(self):
        if self.model_name == 'clf':
            return CLF_PARAM
        else:
            return REG_PARAM

    def load_data(self):

        logger.info('Loading data to train {} model...'.format(self.model_name))

        train = load(get_data_dir(self.model_name + '_train.pkl'))

        val = load(get_data_dir(self.model_name + '_val.pkl'))

        return train, val

    def creat_data(self, dataset):

        X = dataset.drop(columns=['LTV_active'] if self.model_name == 'clf' else ['LTV_52W'], axis=1)

        y = dataset['LTV_active'] if self.model_name == 'clf' else dataset['LTV_52W']

        return X, y

    def prep_data(self):

        train, val = self.load_data()

        logger.info('Creating train set... ')

        X_train, y_train = self.creat_data(train)

        logger.info('X_train and y_train shapes: {}, {}'.format(X_train.shape, y_train.shape))

        logger.info('Creating validation set... ')

        X_val, y_val = self.creat_data(val)

        logger.info('X_val and y_val shapes: {}, {}'.format(X_val.shape, y_val.shape))

        return X_train, y_train, X_val, y_val

    def validation(self, X_val, y_val, model):

        y_pred = model.predict(xgb.DMatrix(data=X_val.values), ntree_limit=model.best_ntree_limit)

        if self.model_name == 'clf':
            y_label = np.where(y_pred >= 0.5, 1, 0)
            f1score = np.round(f1_score(y_val.values, y_label), 2)
            logger.info('f1 score: {}'.format(f1score))
            logger.info('classification report: \n {}'.format(classification_report(y_val.values, y_label)))
            logger.info('confusion matrix: \n {}'.format(conf_matrix(y_val.values, y_label)))
            return y_pred, f1score, None, None, None
        else:
            back_y_pred = np.expm1(y_pred)
            logger.info('User level log scale...')
            mae_log, mape_log = eval_metric(y_val, y_pred, agg=False)
            logger.info('User level back transformation...')
            mae, mape = eval_metric(np.expm1(y_val.values), back_y_pred, agg=False)
            return np.expm1(y_val.values), mae, mape, mae_log, mape_log

    def fit_model(self):

        def mape_eval(y_pred, dval):
            y_val = dval.get_label()
            mape_score = np.mean(np.abs(y_val - y_pred) / y_val)
            return 'mape_score', mape_score

        def f1_eval(y_pred, dval):
            y_true = dval.get_label()
            f1score = f1_score(y_true, np.round(y_pred))
            return 'f1score', f1score

        X_train, y_train, X_val, y_val = self.prep_data()

        dtrain = xgb.DMatrix(data=X_train.values, label=y_train.values)
        dval = xgb.DMatrix(data=X_val.values, label=y_val.values)

        watch_list = [(dval, 'test')]

        if self.model_name == 'clf':
            self.params['scale_pos_weight'] = load_param_json(get_params_dir('imb_ratio.json'))['imb_ratio']

        func_mapping = {
            'reg': mape_eval,
            'clf': f1_eval}

        f_eval = func_mapping[self.model_name]

        old_stdout = sys.stdout

        sw = Stopwatch(start=True)

        logger.info('Training {} model...'.format(self.model_name))

        sys.stdout = open(str(get_model_dir(self.model_name+'.log')), 'w')
        sys.stdout = FlushFile(sys.stdout)

        model = xgb.train(params=self.params, dtrain=dtrain, num_boost_round=self.params['num_round'],
                          evals=watch_list, feval=f_eval, maximize=True if self.model_name == 'clf' else False,
                          early_stopping_rounds=100, verbose_eval=True)

        sys.stdout = old_stdout

        logger.info('best_ntree_limit: {}'.format(model.best_ntree_limit))

        logger.info('Elapsed time of training {} model: {}'.format(self.model_name, sw.elapsed.human_str()))

        y_pred, metric_1, metric_2, metric_3, metric_4 = self.validation(X_val, y_val, model)

        dump(model, get_model_dir(self.model_name + '-model'))

        return y_pred, metric_1, metric_2, metric_3, metric_4

