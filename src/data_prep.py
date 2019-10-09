
"""
@name: data_prep.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

import logging
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from src.utils.dataframe import join_datasets
from src.utils.resources import load, dump
from src.utils.path_helper import get_data_dir

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class DataPrep(object):

    def __init__(self, file_name, test_size):

        self.file_name = file_name
        self.test_size = test_size

    def load_data(self):
        """

        Load data files
        """

        trx_pt = load(get_data_dir(self.file_name['trx_pt']))
        trx_pt.drop(columns=['start_date', 'end_date'], axis=1, inplace=True)

        brx_pt = load(get_data_dir(self.file_name['brx_pt']))
        brx_pt.drop(columns=['start_date', 'end_date'], axis=1, inplace=True)

        trx_po = load(get_data_dir(self.file_name['trx_po']))
        trx_po.drop(columns=['start_date', 'end_date'], axis=1, inplace=True)

        return trx_pt, brx_pt, trx_po

    def pt_x(self, trx_pt, brx_pt):
        """

        Prepare the features for training
        """

        logger.info('trx_pt shape: {}'.format(trx_pt.shape))
        logger.info('brx_pt shape: {}'.format(brx_pt.shape))

        logger.info('Joining trx and brx for period of training (pt)...')

        brx_trx_pt = join_datasets(trx_pt, brx_pt, how='right', key=None)
        logger.info('pt_brx_trx shape: {}'.format(brx_trx_pt.shape))

        # filling missing values
        brx_trx_pt = pd.concat(
            [brx_trx_pt.filter(regex='^((?!std|ratio).)*$', axis=1).fillna(0),
             brx_trx_pt.filter(regex='(std|ratio)', axis=1)],
            axis=1
        )

        logger.info('Checking NA in pt_brx_trx:  {}'.format(brx_trx_pt.isna().sum().any()))

        brx_trx_pt.drop(columns=['flagReseller'], inplace=True)

        X = brx_trx_pt

        logger.info('X is ready...')

        return X

    def po_y(self, trx_po, X):
        """

        Prepare the label for training
        """

        logger.info('trx_po shape: {}'.format(trx_po.shape[0]))

        logger.info('Building target values for period of training (pt)...')

        # create Y
        Y = trx_po[['marginCAD_sum_cart']].rename(columns={'marginCAD_sum_cart': 'LTV_52W'})\
            .assign(LTV_active=lambda x: x.LTV_52W >= 1)

        Y_w_pt = join_datasets(Y, X[['marginCAD_sum_cart']], how='right', key=None)

        Y_w_pt.rename(columns={'marginCAD_sum_cart': 'LTV_pt'}, inplace=True)

        Y_w_pt.fillna(value={'LTV_active': False, 'LTV_52W': 0}, inplace=True)

        Y_w_pt.loc[Y_w_pt.LTV_52W < 1, 'LTV_52W'] = 0

        logger.info('Y shape: {}'.format(Y_w_pt.shape))

        # assert right join
        pd.testing.assert_index_equal(X.index, Y_w_pt.index)

        # y_label: determine if user is active, flag as 0/1
        y_label = Y_w_pt['LTV_active'].astype('int8')

        # Y_value: for all user what is the net margin
        y_value = Y_w_pt['LTV_52W'].apply(lambda x: np.log1p(x))

        logger.info('y_label and y_value are ready...')

        return y_label, y_value

    def prep(self):

        trx_pt, brx_pt, trx_po = self.load_data()

        X = self.pt_x(trx_pt, brx_pt)

        y_label, y_value = self.po_y(trx_po, X)

        logger.info('Building train and test splits per each model...')

        # Build train and test sets
        logger.info('X shape: {}'.format(X.shape))

        indP = (y_value >= np.log1p(1))

        X_clf_train, X_clf_test, y_clf_train, y_clf_test = train_test_split(
            X, y_label, test_size=self.test_size, random_state=42, stratify=y_label)

        clf_train = pd.concat([y_clf_train, X_clf_train], axis=1)
        clf_test = pd.concat([y_clf_test, X_clf_test], axis=1)

        logger.info('X_clf_train and X_clf_test shapes: {}, {}'.format(X_clf_train.shape[0], X_clf_test.shape[0]))
        logger.info('y_clf_train and y_clf_test shapes: {}, {}'.format(y_clf_train.shape[0], y_clf_test.shape[0]))

        dump(clf_train, get_data_dir('clf_train.pkl'))
        dump(clf_test, get_data_dir('clf_test.pkl'))

        X_reg_train = X[indP & X.index.isin(X_clf_train.index)]
        X_reg_test = X[indP & X.index.isin(X_clf_test.index)]

        y_reg_train = y_value[indP & y_value.index.isin(y_clf_train.index)]
        y_reg_test = y_value[indP & y_value.index.isin(y_clf_test.index)]

        reg_train = pd.concat([y_reg_train, X_reg_train], axis=1)
        reg_test = pd.concat([y_reg_test, X_reg_test], axis=1)

        logger.info('X_reg_train and X_reg_test shapes: {}, {}'.format(X_reg_train.shape[0], X_reg_test.shape[0]))
        logger.info('y_reg_train and y_reg_test shapes: {}, {}'.format(y_reg_train.shape[0], y_reg_test.shape[0]))

        dump(reg_train, get_data_dir('reg_train.pkl'))
        dump(reg_test, get_data_dir('reg_test.pkl'))








