
"""
@name: brx_data_prep.py

@author: Mohammad Jeihoonian

Created on Sept 2019
"""

import logging
import pandas as pd
import re
from src.brx_data_retrieve import BrxRet
from src.utils.dataframe import join_datasets, str_encode

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class BrxPrep(object):

    def __init__(self, start_date, end_date, threshold, ext):
        self.start_date = start_date
        self.end_date = end_date
        self.threshold = threshold
        self.ext = ext

    def cat_feats(self, dataset):

        logger.info('One-hot-encoding of categorical features... ')

        cols = dataset.columns.ravel()
        dataset.columns = [re.sub("_CAT_", ":", col) for col in cols]

        cat_vars = ['deviceCategory', 'operatingSystem', 'browser',
                    'deviceLanguage', 'deviceCountry', 'country',
                    'subContinent', 'channel', 'mobileDeviceBranding',
                    'session_md_wave', 'session_md_season', 'session_md_year',
                    'pdp_category',  'pdp_brand', 'UserListName',
                    'adChannelType', 'adWord'
                    ]

        dataset_cat = dataset[['ID']]

        for cat_var in cat_vars:
            logger.info('Categorical features encoding: ' + str(cat_var))
            if cat_var != 'adWord':
                df_onehot = str_encode(dataset[cat_var], threshold=self.threshold)
            else:
                df_onehot = str_encode(dataset[cat_var], threshold=0.1*self.threshold)
            dataset_cat = pd.concat([dataset_cat, df_onehot], axis=1)

        # subset non-categorical features
        dataset.set_index('ID', inplace=True)
        dataset_num = dataset.loc[:, ~(dataset.dtypes == 'object')]
        dataset_num.reset_index(inplace=True)

        # Join numerical and categorical features
        dataset = join_datasets(dataset_cat, dataset_num, how='inner', key='ID')

        assert dataset.shape[1] == dataset_cat.shape[1] + dataset_num.shape[1] - 1
        assert dataset.shape[0] == dataset_cat.shape[0] == dataset.shape[0]

        return dataset

    def merge_feats(self, dataset):

        logger.info('Aggregating all features at visitor level... ')

        # Fill na on non-std/ratio variables
        dataset = pd.concat(
            [dataset.filter(regex='^((?!std|ratio).)*$', axis=1).fillna(0),
             dataset.filter(regex='(std|ratio)', axis=1)],
            axis=1
        )

        dataset['start_date'] = self.start_date
        dataset['end_date'] = self.end_date
        dataset.set_index('ID', inplace=True)
        col2float = dataset.columns[~(dataset.dtypes == 'object')]
        dataset[col2float] = dataset[col2float].astype('float64')

        logger.info('brx shape: {}'.format(dataset.shape))

        return dataset

    def brx_data_prep(self):

        brx_dataset = BrxRet(self.start_date, self.end_date, self.ext).ret()

        brx_dataset.drop(columns=['conversion', 'conversion_po'], axis=1, inplace=True)

        brx_feats = self.cat_feats(brx_dataset)

        brx_feats = self.merge_feats(brx_feats)

        return brx_feats

