
"""
@name: agg_helper.py

@author: Mohammad Jeihoonian

Created on Nov 2019
"""

import logging
import numpy as np
import pandas as pd
import xgboost as xgb
from src.utils.dataframe import join_datasets
from src.utils.path_helper import get_data_dir, get_model_dir
from src.utils.resources import load
from src.utils.ml_helper import eval_metric

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


mapping_dict = {'cak_level': ['campaignId', 'adGroupId', 'criteriaId'], 'ca_level': ['campaignId', 'adGroupId'],
                'c_level': ['campaignId'], 'cak_col': ['campaignId', 'adGroupId', 'criteriaId', 'users', 'AOV'],
                'ca_col': ['campaignId', 'adGroupId', 'users', 'AOV'], 'c_col': ['campaignId', 'users', 'AOV']}


def load_agg_data():

    clf_val = load(get_data_dir('clf_val.pkl'))
    X_val = clf_val.drop(columns=['LTV_active'], axis=1)
    y_clf_val = clf_val[['LTV_active']]

    val_index = X_val.index

    reg_val = load(get_data_dir('reg_val.pkl'))
    y_reg_val = reg_val[['LTV_52W']]
    y_reg_val = y_reg_val.assign(raw_LTV_52W=lambda x: np.expm1(x.LTV_52W))

    y_val = join_datasets(y_clf_val, y_reg_val, how='left', key=None)
    y_val = y_val.assign(LTV_val=lambda x: np.where(x.LTV_active == 1, x.LTV_52W, 0),
                         raw_LTV_val=lambda x: np.where(x.LTV_active == 1, x.raw_LTV_52W, 0))
    y_val.drop(columns=['LTV_active', 'raw_LTV_52W', 'LTV_52W'], axis=1, inplace=True)

    return X_val, y_val, val_index


def load_ads(calib=True):

    if calib:
        return load(get_data_dir('ads_pt.pkl'))
    else:
        return load(get_data_dir('ads_po.pkl'))


def agg_func(ads_data, index, y, level, val_col, count_col):

    dy = pd.Series(data=y, index=index, name='LTV')

    dataset = join_datasets(ads_data[level], dy, how='inner', key=None)
    dataset.reset_index(inplace=True)
    level.insert(0, 'ID')
    dataset.drop_duplicates(subset=level, inplace=True)
    dataset = dataset.assign(counts=lambda x: x.groupby(['ID'])['ID'].transform('size')).assign(
        LTV_pct=lambda x: x.LTV / x.counts)
    level.pop(0)

    dataset = dataset.groupby(level).agg({'ID': [(count_col, 'size')],
                                          'LTV_pct': [(val_col, 'mean')]})
    dataset.columns = dataset.columns.droplevel(level=0)
    dataset.reset_index(inplace=True)

    return dataset


def merge_agg(val_dataset, pred_dataset, cols, level, val_col, count_col, filename):

    val_dataset = val_dataset[cols].copy()
    val_dataset.drop_duplicates(inplace=True)
    val_dataset.rename(columns={val_col: val_col + '_val'}, inplace=True)

    pred_dataset = pred_dataset[cols].copy()
    pred_dataset.drop_duplicates(inplace=True)
    pred_dataset.rename(columns={val_col: val_col + '_pred'}, inplace=True)
    pred_dataset.drop(columns=[count_col], inplace=True)

    assert val_dataset.shape[0] == pred_dataset.shape[0]

    dataset = pd.merge(val_dataset, pred_dataset, how='inner', on=level)

    if filename is not None:
        dataset.to_csv(get_data_dir(filename), index=False)

    return dataset


def merge_pred(pred_proba):

    X_val, y_val, val_index = load_agg_data()

    clf_pred_df = pd.DataFrame(data=pred_proba, index=val_index, columns=['pred_proba'])

    reg_model = load(get_model_dir('reg-model'))
    reg_pred = reg_model.predict(xgb.DMatrix(data=X_val.values), ntree_limit=reg_model.best_ntree_limit)
    reg_pred_df = pd.DataFrame(data=np.expm1(reg_pred), index=val_index, columns=['pred_value'])

    pred_df = join_datasets(clf_pred_df, reg_pred_df, how='inner', key=None)
    pred_df = pred_df.assign(pred=lambda x: np.round(x.pred_proba * x.pred_value, 4))

    merge_df = join_datasets(pred_df, y_val[['raw_LTV_val']], how='inner', key=None)

    logger.info('Level: user')

    mae = eval_metric(merge_df.raw_LTV_val, merge_df.pred, agg=True)

    return merge_df, val_index, mae


def agg_res(merge_df, val_index, level, cols, file_name, value_col='AOV', count_col='users', calib=True):

    ads_data = load_ads(calib=calib)

    logger.info('Level: {}'.format(level))

    val = agg_func(ads_data, val_index, merge_df['raw_LTV_val'], level, value_col, count_col)
    pred = agg_func(ads_data, val_index, merge_df['pred'], level, value_col, count_col)

    agg_df = merge_agg(val, pred, cols, level, value_col, count_col, filename=file_name)

    return eval_metric(agg_df.AOV_val, agg_df.AOV_pred, agg=True)



