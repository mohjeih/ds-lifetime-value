
"""
@name: data_prep.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

# import boto3
import logging
import numpy as np
# import os
import pandas as pd
from sklearn.model_selection import train_test_split
from src.utils.dataframe import join_datasets, get_feature_name
# from src.utils.db_engine import s3_aws_engine
from src.utils.path_helper import *
from src.utils.resources import *
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class DataPrep(object):

    def __init__(self, trx_pt, brx_pt, ads_pt, trx_po, brx_po, ads_po, test_size, aws_env):

        self.trx_pt = trx_pt
        self.brx_pt = brx_pt
        self.ads_pt = ads_pt
        self.trx_po = trx_po
        self.brx_po = brx_po
        self.ads_po = ads_po
        self.test_size = test_size
        self.aws_env = aws_env

    @staticmethod
    def dump_data(dataset, filename, pkl_format=True, train_dir=True):

        logger.info('Dumping {}...'.format(filename))

        if pkl_format:
            dump(dataset, get_data_dir(filename))
        else:
            if train_dir:
                dataset.to_csv(get_train_dir(filename), header=False, index=False)
            else:
                dataset.to_csv(get_val_dir(filename), header=False, index=False)

    @staticmethod
    def flooring(dseries):

        dseries = dseries.copy()

        dseries = np.where(dseries <= dseries.quantile(0.2), dseries.quantile(0.2), dseries)

        return dseries

    @staticmethod
    def capping(dseries):

        dseries = dseries.copy()

        dseries = np.where(dseries >= dseries.quantile(0.999), dseries.quantile(0.999), dseries)

        return dseries

    @staticmethod
    def reg_prep(X, y, indP, X_clf, y_clf):

        X_reg = X[indP & X.index.isin(X_clf.index)]

        y_reg = y[indP & y.index.isin(y_clf.index)]
        y_reg = y_reg.assign(raw_LTV_52W=lambda x: np.expm1(x.LTV_52W))
        y_reg.raw_LTV_52W = DataPrep.flooring(y_reg['raw_LTV_52W'])
        y_reg.raw_LTV_52W = DataPrep.capping(y_reg['raw_LTV_52W'])
        y_reg = y_reg.assign(LTV_52W=lambda x: np.log1p(x.raw_LTV_52W))
        y_reg.drop(columns=['raw_LTV_52W'], axis=1, inplace=True)

        return X_reg, y_reg

    @staticmethod
    def concat_datasets(X, y):

        return pd.concat([y, X], axis=1)


    # def _push_to_s3(self, local_path=str(S3_DIR)+'/'):
    #     """
    #
    #     Upload local files in folder to s3 bucket
    #
    #     :param local_path: local path
    #     """
    #
    #     sw = Stopwatch(start=True)
    #
    #     # Get s3 client
    #     s3_bucket, id, secret = s3_aws_engine(name=self.aws_env)
    #
    #     s3c = boto3.client('s3', aws_access_key_id=id, aws_secret_access_key=secret)
    #
    #     for root, dirs, files in os.walk(local_path):
    #
    #         for file in files:
    #             local_file_path = os.path.join(root, file)
    #             s3_file_path = root[len(local_path):] + '/' + file
    #             logger.info("Uploading {} to s3://{}/{}".format(local_file_path, s3_bucket, s3_file_path))
    #             s3c.upload_file(local_file_path, s3_bucket, s3_file_path)
    #
    #     logger.info('Elapsed time of files upload to S3 bucket: {}'.format(sw.elapsed.human_str()))

    def pt_x(self):
        """

        Prepare features for training
        """

        logger.info('trx_pt shape: {}'.format(self.trx_pt.shape))
        logger.info('brx_pt shape: {}'.format(self.brx_pt.shape))

        logger.info('Joining trx and brx for period of training (pt)...')

        brx_trx_pt = join_datasets(self.trx_pt, self.brx_pt, how='right', key=None)
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

        ind = highly_skewed_var[highly_skewed_var.values == True].index

        logger.info('The number of highly skewed features: {}'.format(ind.shape))

        brx_trx_pt[ind] = brx_trx_pt[ind].apply(np.log1p)

        logger.info('brx_trx_pt shape: {}'.format(brx_trx_pt.shape))

        return brx_trx_pt

    def po_y(self, margin_val, index_val):
        """

        Prepare labels for training
        """

        logger.info('trx_po shape: {}'.format(self.trx_po.shape[0]))

        logger.info('Building target values for period of training (pt)...')

        # create Y
        Y = self.trx_po[['marginCAD_sum_cart']].rename(columns={'marginCAD_sum_cart': 'LTV_52W'})\
            .assign(LTV_active=lambda x: x.LTV_52W >= 20)

        Y_w_pt = join_datasets(Y, margin_val, how='right', key=None)

        # Y_w_pt.rename(columns={'marginCAD_sum_cart': 'LTV_pt'}, inplace=True)

        Y_w_pt.fillna(value={'LTV_active': False, 'LTV_52W': 0}, inplace=True)

        Y_w_pt.loc[Y_w_pt.LTV_52W < 20, 'LTV_52W'] = 0

        logger.info('Y shape: {}'.format(Y_w_pt.shape))

        # assert right join
        pd.testing.assert_index_equal(index_val, Y_w_pt.index)

        # y_label: determine if user is active, flag as 0/1
        y_label = Y_w_pt['LTV_active'].astype('int8')

        # Y_value: for all user what is the net margin
        y_value = Y_w_pt[['LTV_52W']].apply(lambda x: np.log1p(x))

        return y_label, y_value

    def po_x(self):
        """

        Prepare features for prediction
        """

        logger.info('trx_po shape: {}'.format(self.trx_po.shape))
        logger.info('brx_po shape: {}'.format(self.brx_po.shape))

        logger.info('Joining trx and brx for period of observation (po)...')

        brx_trx_po = join_datasets(self.trx_po, self.brx_po, how='right', key=None)
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

        logger.info('brx_trx_po shape: {}'.format(brx_trx_po.shape))

        return brx_trx_po

    def prep(self, calib):

        sw = Stopwatch(start=True)

        X_pred = self.po_x()

        if not calib:
            pres_cols = load(get_data_dir('features.pkl'))
        else:
            pres_cols = np.zeros(shape=X_pred.shape[1])

        if calib:

            X = self.pt_x()

            y_label, y_value = self.po_y(margin_val=X[['marginCAD_sum_cart']], index_val=X.index)

            pres_cols = X.columns.intersection(X_pred.columns)

            logger.info('Columns Intersection: {}'.format(pres_cols.shape[0]))

            X = X[pres_cols]

            X = X.fillna(-9999)

            logger.info('X shape: {}'.format(X.shape))

            np.savetxt(get_data_dir('features.txt'), X.columns.ravel(), fmt='%s')

            DataPrep.dump_data(X.columns.ravel(), 'features.pkl')

            logger.info('Building train and test splits per each model...')

            # Build train and test sets

            indP = (y_value['LTV_52W'].values >= np.log1p(20))

            X_clf_train, X_clf_val, y_clf_train, y_clf_val = train_test_split(
                X, y_label, test_size=self.test_size, random_state=42, stratify=y_label)

            clf_train = DataPrep.concat_datasets(X_clf_train, y_clf_train)
            clf_val = DataPrep.concat_datasets(X_clf_val, y_clf_val)

            imb_ratio = float(np.sum(y_clf_train == 0) / np.sum(y_clf_train == 1))

            param_dict = dict()
            param_dict['imb_ratio'] = imb_ratio

            dump_param_json(param_dict, get_params_dir('imb_ratio.json'))

            logger.info('The balance of positive and negative rates: {}'.format(param_dict['imb_ratio']))

            logger.info('X_clf_train and X_clf_val shapes: {}, {}'.format(X_clf_train.shape, X_clf_val.shape))
            logger.info('y_clf_train and y_clf_val shapes: {}, {}'.format(y_clf_train.shape, y_clf_val.shape))

            DataPrep.dump_data(clf_train, 'clf_train.pkl')
            DataPrep.dump_data(clf_val, 'clf_val.pkl')

            # DataPrep.dump_data(clf_train, 'clf_train.csv', pkl_format=False)
            # DataPrep.dump_data(clf_val, 'clf_val.csv', pkl_format=False, train_dir=False)

            X_reg_train, y_reg_train = DataPrep.reg_prep(X, y_value, indP, X_clf_train, y_clf_train)
            X_reg_val, y_reg_val = DataPrep.reg_prep(X, y_value, indP, X_clf_val, y_clf_val)

            reg_train = DataPrep.concat_datasets(X_reg_train, y_reg_train)
            reg_val = DataPrep.concat_datasets(X_reg_val, y_reg_val)

            logger.info('X_reg_train and X_reg_val shapes: {}, {}'.format(X_reg_train.shape, X_reg_val.shape))
            logger.info('y_reg_train and y_reg_val shapes: {}, {}'.format(y_reg_train.shape, y_reg_val.shape))

            DataPrep.dump_data(reg_train, 'reg_train.pkl')
            DataPrep.dump_data(reg_val, 'reg_val.pkl')

            # DataPrep.dump_data(reg_train, 'reg_train.csv', pkl_format=False)
            # DataPrep.dump_data(reg_val, 'reg_val.csv', pkl_format=False, train_dir=False)

            # self._push_to_s3(local_path=str(S3_DIR)+'/')

            DataPrep.dump_data(self.ads_pt, 'ads_pt.pkl')

        X_pred = X_pred[pres_cols]

        X_pred = X_pred.fillna(-9999)

        logger.info('X_pred shape: {}'.format(X_pred.shape))

        dump(X_pred, get_data_dir('X_pred.pkl'))

        DataPrep.dump_data(self.ads_po, 'ads_po.pkl')

        logger.info('Elapsed time of preparing data: {}'.format(sw.elapsed.human_str()))
