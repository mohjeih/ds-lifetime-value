
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
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class BrxPrep(object):

    def __init__(self, start_date, end_date, threshold, ext, non_adj):
        self.start_date = start_date
        self.end_date = end_date
        self.threshold = threshold
        self.ext = ext
        self.non_adj = non_adj

    @staticmethod
    def user_gens(dataset, chunk_size=50000):

        logger.info('Creating users generator... ')

        user_list = dataset.ID.tolist()

        return (user_list[i:i + chunk_size] for i in range(0, len(user_list), chunk_size))

    @staticmethod
    def post_feats(dataset):

        logger.info('Processing features... ')

        dataset = dataset.copy()

        dataset.reset_index(drop=True, inplace=True)

        # Fill na on non-std/ratio variables
        dataset = pd.concat(
            [dataset.filter(regex='^((?!std|ratio).)*$', axis=1).fillna(0),
             dataset.filter(regex='(std|ratio)', axis=1)],
            axis=1
        )

        dataset.set_index('ID', inplace=True)

        col2float = dataset.columns[~(dataset.dtypes == 'object')]
        dataset[col2float] = dataset[col2float].astype('float64')

        logger.info('brx shape: {}'.format(dataset.shape))

        return dataset

    @staticmethod
    def ads_prep(dataset):

        logger.info('Processing adwords data... ')

        dataset = dataset.copy()

        dataset = dataset[['ID', 'campaignId', 'adGroupId', 'criteriaId']]

        dataset.set_index('ID', inplace=True)
        
        return dataset

    def cat_feats(self, dataset):

        logger.info('One-hot-encoding of categorical features... ')

        cols = dataset.columns.ravel()
        dataset.columns = [re.sub("_CAT_", ":", col) for col in cols]

        cat_vars = ['deviceCategory', 'operatingSystem', 'browser',
                    'deviceLanguage', 'deviceCountry', 'country',
                    'subContinent', 'channel', 'mobileDeviceBranding',
                    'session_md_wave', 'session_md_season',
                    'pdp_category', 'pdp_brand', 'UserListName',
                    'adChannelType', 'adWord'
                    ]  # 'session_md_year',

        dataset_cat = dataset[['ID']]
        dataset_cat.reset_index(drop=True, inplace=True)

        for cat_var in cat_vars:
            logger.info('Categorical features encoding: {}'.format(cat_var))
            if cat_var == 'adWord':
                df_onehot = str_encode(dataset[cat_var], threshold=0.5 * self.threshold)
            else:
                df_onehot = str_encode(dataset[cat_var], threshold=self.threshold)
            dataset_cat = pd.concat([dataset_cat, df_onehot], axis=1)

        # subset non-categorical features
        dataset.set_index('ID', inplace=True)
        dataset_num = dataset.loc[:, ~(dataset.dtypes == 'object')]
        dataset_num.reset_index(inplace=True)

        # Join numerical and categorical features
        dataset = join_datasets(dataset_cat, dataset_num, how='inner', key='ID')

        assert dataset.shape[1] == dataset_cat.shape[1] + dataset_num.shape[1] - 1
        assert dataset.shape[0] == dataset_cat.shape[0] == dataset_num.shape[0]

        return dataset

    def brx_data_prep(self, chunk_size=50000):

        sw = Stopwatch(start=True)

        date_dataset = pd.DataFrame()

        if self.ext:
            brx_dataset, ads_dataset, _ = BrxRet(self.start_date, self.end_date, self.ext, self.non_adj).ret()
        else:
            brx_dataset, ads_dataset, date_dataset = BrxRet(self.start_date, self.end_date, self.ext, self.non_adj).ret()

        ads_dataset = BrxPrep.ads_prep(ads_dataset)

        if self.ext:
            brx_dataset.drop(columns=['conversion_po'], axis=1, inplace=True)

        user_gen = BrxPrep.user_gens(brx_dataset, chunk_size)

        brx_feats = pd.DataFrame()

        count = 0

        for Ids in user_gen:

            count += 1

            logger.info('The number of users in sublist {}: {}'.format(count, len(Ids)))

            brx_pt_subset = brx_dataset.pipe(lambda x: x[x.ID.isin(Ids)])

            brx_pt_subset = self.cat_feats(brx_pt_subset)

            brx_feats = brx_feats.append(brx_pt_subset, sort=False)

        brx_feats = BrxPrep.post_feats(brx_feats)

        if self.ext:
            logger.info('Elapsed time of brx ETL (pt): {}'.format(sw.elapsed.human_str()))
        else:
            logger.info('Elapsed time of brx ETL (po): {}'.format(sw.elapsed.human_str()))

        return brx_feats, ads_dataset, date_dataset
