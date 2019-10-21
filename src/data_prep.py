
"""
@name: data_prep.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

import logging
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from src.utils.dataframe import join_datasets, get_feature_name
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

        brx_pt = load(get_data_dir(self.file_name['brx_pt']))

        trx_po = load(get_data_dir(self.file_name['trx_po']))

        brx_po = load(get_data_dir(self.file_name['brx_po']))

        return trx_pt, brx_pt, trx_po, brx_po

    @staticmethod
    def pt_x(trx_pt, brx_pt):
        """

        Prepare features for training
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

        logger.info('Checking NA in pt_brx_trx: {}'.format(brx_trx_pt.isna().sum().any()))

        brx_trx_pt.drop(columns=['flagReseller'], inplace=True)

        features_df = get_feature_name(brx_trx_pt.columns)

        highly_skewed_var = brx_trx_pt[features_df[features_df.type == 'num']['feature_name']].filter(
            regex=r'^((?!bin|std|ratio).)*$').skew().abs() >= 2

        ind = highly_skewed_var[highly_skewed_var.values==True].index

        logger.info('The number of highly skewed features: {}'.format(ind.shape))

        brx_trx_pt[ind] = brx_trx_pt[ind].apply(np.log1p)

        logger.info('X shape: {}'.format(brx_trx_pt.shape))

        return brx_trx_pt

    @staticmethod
    def po_y(trx_po, X):
        """

        Prepare labels for training
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

        return y_label, y_value

    @staticmethod
    def po_x(trx_po, brx_po):
        """

        Prepare features for prediction
        """

        logger.info('trx_po shape: {}'.format(trx_po.shape))
        logger.info('brx_po shape: {}'.format(brx_po.shape))

        logger.info('Joining trx and brx for period of observation (po)...')

        brx_trx_po = join_datasets(trx_po, brx_po, how='right', key=None)
        logger.info('brx_trx_po shape: {}'.format(brx_trx_po.shape))

        # filling missing values
        brx_trx_po = pd.concat(
            [brx_trx_po.filter(regex='^((?!std|ratio).)*$', axis=1).fillna(0),
             brx_trx_po.filter(regex='(std|ratio)', axis=1)],
            axis=1
        )

        logger.info('Checking NA in pt_brx_trx:  {}'.format(brx_trx_po.isna().sum().any()))

        brx_trx_po.drop(columns=['flagReseller'], inplace=True)

        features_df = get_feature_name(brx_trx_po.columns)

        highly_skewed_var = brx_trx_po[features_df[features_df.type == 'num']['feature_name']].filter(
            regex=r'^((?!bin|std|ratio).)*$').skew().abs() >= 2

        ind = highly_skewed_var[highly_skewed_var.values == True].index

        logger.info('The number of highly skewed features: {}'.format(ind.shape))

        brx_trx_po[ind] = brx_trx_po[ind].apply(np.log1p)

        logger.info('X_pred shape: {}'.format(brx_trx_po.shape))

        return brx_trx_po

    def prep(self):

        trx_pt, brx_pt, trx_po, brx_po = self.load_data()

        X = DataPrep.pt_x(trx_pt, brx_pt)

        y_label, y_value = DataPrep.po_y(trx_po, X)

        X_pred = DataPrep.po_x(trx_po, brx_po)

        logger.info('Building train and test splits per each model...')

        # Build train and test sets

        indP = (y_value >= np.log1p(1))

        X_clf_train, X_clf_test, y_clf_train, y_clf_test = train_test_split(
            X, y_label, test_size=self.test_size, random_state=42, stratify=y_label)

        clf_train = pd.concat([y_clf_train, X_clf_train], axis=1)
        clf_test = pd.concat([y_clf_test, X_clf_test], axis=1)

        logger.info('X_clf_train and X_clf_test shapes: {}, {}'.format(X_clf_train.shape, X_clf_test.shape))
        logger.info('y_clf_train and y_clf_test shapes: {}, {}'.format(y_clf_train.shape, y_clf_test.shape))

        dump(clf_train, get_data_dir('clf_train.pkl'))
        dump(clf_test, get_data_dir('clf_test.pkl'))

        X_reg_train = X[indP & X.index.isin(X_clf_train.index)]
        X_reg_test = X[indP & X.index.isin(X_clf_test.index)]

        y_reg_train = y_value[indP & y_value.index.isin(y_clf_train.index)]
        y_reg_test = y_value[indP & y_value.index.isin(y_clf_test.index)]

        reg_train = pd.concat([y_reg_train, X_reg_train], axis=1)
        reg_test = pd.concat([y_reg_test, X_reg_test], axis=1)

        logger.info('X_reg_train and X_reg_test shapes: {}, {}'.format(X_reg_train.shape, X_reg_test.shape))
        logger.info('y_reg_train and y_reg_test shapes: {}, {}'.format(y_reg_train.shape, y_reg_test.shape))

        dump(reg_train, get_data_dir('reg_train.pkl'))
        dump(reg_test, get_data_dir('reg_test.pkl'))

        pres_cols = X.columns.intersection(X_pred.columns)

        logger.info('Columns Intersection: {}'.format(pres_cols.shape[0]))

        X_pred = X_pred[pres_cols]

        dump(X_pred, get_data_dir('X_pred.pkl'))








