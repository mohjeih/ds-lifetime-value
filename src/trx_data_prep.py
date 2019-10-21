
"""
@name: trx_data_prep.py

@author: Mohammad Jeihoonian

Created on Sep 2019
"""

import datetime
import logging
import numpy as np
from src.trx_data_retrieve import TrxRet
from src.utils.dataframe import *

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class TrxPrep(object):

    def __init__(self, start_date, end_date, threshold, ext):
        self.start_date = start_date
        self.end_date = end_date
        self.threshold = threshold
        self.ext = ext

    def data_cart(self, dataset):

        """

        Cart level feature engineering

        :param dataset
        :return: dataframe
        """

        logger.info('Creating cart level features... ')

        df_cart_info = dataset[['ID', 'cartID', 'cartDate',
                                'cartDate_MD_flag', 'cartDate_MD_season',
                                'cartDate_MD_wave'
                                ]].drop_duplicates()  # , 'cartDate_MD_year'

        df_cart_info.reset_index(inplace=True, drop=True)

        # Categorical features
        cat_vars = ['cartDate_MD_season', 'cartDate_MD_wave']  # , 'cartDate_MD_year'
        cart_cat = df_cart_info[['ID']]
        features_cart_cat_list = list()
        for cat_var in cat_vars:
            df_onehot = one_hot_encode(df_cart_info[cat_var], threshold=self.threshold)
            temp = pd.concat([cart_cat, df_onehot], axis=1)
            temp = temp.groupby(['ID']).agg('sum')
            sum_row = temp.sum(axis=1)
            temp_pct = temp.div(sum_row, axis=0)
            temp_pct.rename(columns=lambda x: 'pct_' + x, inplace=True)
            features_cart_cat_list.append(pd.concat([temp, temp_pct], axis=1))

        features_cart_cat = pd.concat(features_cart_cat_list, axis=1)

        # Numerical features
        end_date = pd.to_datetime(self.end_date)

        features_cart_num = df_cart_info \
            .assign(cartDate_ordinal=lambda x: pd.to_datetime(x.cartDate).astype(np.int64) / 10 ** 9) \
            .groupby('ID') \
            .agg({
                'cartID': [('cart_count', 'count')],
                'cartDate': [
                    ('cart_date_min', 'min'),
                    ('cart_date_max', 'max')
                ],
                'cartDate_MD_flag': [
                    ('carts_inMD_period', 'sum'),
                    ('carts_inMD_period_ratio', 'mean')
                ],
                'cartDate_ordinal': [
                    ('cartDate_recent', 'max'),
                    ('cartDate_min', 'min'),
                    ('cartDate_avg', 'mean'),
                    ('cartDate_std', 'std'),
                    ('cartDate_median', 'median'),
                    ('cartDate_q25', lambda x: np.percentile(x, 25)),
                    ('cartDate_q75', lambda x: np.percentile(x, 75))
                ]
            })

        features_cart_num.columns = features_cart_num.columns.droplevel(level=0)

        features_cart_num['cart_date_min'] = features_cart_num['cart_date_min'].astype('datetime64[ns]')
        features_cart_num['cart_date_max'] = features_cart_num['cart_date_max'].astype('datetime64[ns]')

        features_cart_num = features_cart_num.assign(
            day_since_last_year_first_order=lambda x: (end_date - x.cart_date_min).dt.days,
            day_since_last_order=lambda x: (end_date - x.cart_date_max).dt.days
        )

        features_cart_num.drop(columns=['cart_date_min', 'cart_date_max'], axis=1, inplace=True)

        # Concat all features
        features_cart_info = pd.concat(
            [features_cart_cat,
             features_cart_num],
            axis=1
        )

        return features_cart_info

    def data_net_item(self, dataset):

        """

        Item net level feature engineering

        :param dataset
        :return: dataframe
        """

        logger.info('Creating item net level features... ')

        df_net_item = dataset.copy()

        # Categorical features
        cat_vars = ['genderT', 'dept', 'category', 'brand', 'season']
        item_cat_stats = df_net_item[['ID', 'nNet']]
        features_item_cat_list = list()
        for cat_var in cat_vars:
            df_onehot = one_hot_encode(df_net_item[cat_var], threshold=self.threshold)
            temp = pd.concat([item_cat_stats, df_onehot], axis=1)

            temp = temp.loc[temp.index.repeat(temp['nNet'].astype('int64'))]
            temp.drop(columns='nNet', inplace=True)

            temp = temp.groupby(['ID']).agg('sum')
            sum_row = temp.sum(axis=1)
            temp_pct = temp.div(sum_row, axis=0)
            temp_pct.rename(columns=lambda x: 'pct_' + x, inplace=True)
            features_item_cat_list.append(pd.concat([temp, temp_pct], axis=1))

        features_item_cat = pd.concat(features_item_cat_list, axis=1)

        # Numerical features
        features_item_num = df_net_item.groupby(['ID']) \
            .agg({
                'nNet': [('items_count', 'sum')],
                'marginCAD_net': [('marginCAD_item_avg', 'mean')],
                'weightedMarkdown': [
                    ('markdown_item_ratio', lambda x: np.mean(x)),
                    ('markdown_item_count', 'sum')
                ],
                'weightedsalePercentage': [
                    ('sale_percentage_avg', 'mean'),
                    ('sale_percentage_max', 'max'),
                    ('sale_percentage_min', 'min'),
                    ('sale_percentage_std', 'std'),
                    ('sale_percentage_median', 'median'),
                    ('sale_percentage_q25', lambda x: np.percentile(x, 25)),
                    ('sale_percentage_q75', lambda x: np.percentile(x, 75))
                ],
                'marginCAD_net_unit': [
                    ('marginCAD_item_unit_avg', 'mean'),
                    ('marginCAD_item_unit_max', 'max'),
                    ('marginCAD_item_unit_min', 'min'),
                    ('marginCAD_item_unit_std', 'std'),
                    ('marginCAD_item_unit_median', 'median'),
                    ('marginCAD_item_unit_q25', lambda x: np.percentile(x, 25)),
                    ('marginCAD_item_unit_q75', lambda x: np.percentile(x, 75))
                ]
            })

        features_item_num.columns = features_item_num.columns.droplevel(level=0)

        # Get value for dept_md/fp specific
        features_item_num_md_ = df_net_item \
            .assign(markdown=lambda x: ['md' if mdflag else 'fp' for mdflag in x.markdownFlag]) \
            .assign(dept_md=lambda x: ':' + x.dept + '_' + x.markdown.astype('str')) \
            .groupby(['ID', 'dept_md']) \
            .agg({
                'nNet': 'sum',
                'marginCAD_net': ['sum', 'mean'],
                'marginCAD_net_unit': 'mean'
            })

        features_item_num_md = features_item_num_md_.unstack(fill_value=0)
        features_item_num_md.columns = ['_'.join(str(s).strip() for s in col) for col in features_item_num_md.columns]

        features_item = pd.concat(
            [features_item_cat,
             features_item_num,
             features_item_num_md],
            axis=1)

        # last quarter features
        end_date = pd.to_datetime(self.end_date)
        last_quarter_date = (end_date - datetime.timedelta(weeks=12)).strftime('%Y-%m-%d')

        df_net_item_lq = df_net_item[df_net_item.cartDate.astype('str') >= last_quarter_date]

        features_item_lq_num = df_net_item_lq.groupby(['ID']) \
            .agg({
                'nNet': [('items_count_lq', 'sum')],
                'marginCAD_net': [('marginCAD_item_avg_lq', 'mean')],
                'weightedMarkdown': [
                    ('markdown_item_ratio_lq', lambda x: np.mean(x)),
                    ('markdown_item_count_lq', 'sum')
                ],
                'marginCAD_net_unit': [
                    ('marginCAD_item_unit_avg_lq', 'mean'),
                    ('marginCAD_item_unit_max_lq', 'max'),
                    ('marginCAD_item_unit_min_lq', 'min'),
                    ('marginCAD_item_unit_std_lq', 'std'),
                    ('marginCAD_item_unit_median_lq', 'median'),
                    ('marginCAD_item_unit_q25_lq', lambda x: np.percentile(x, 25)),
                    ('marginCAD_item_unit_q75_lq', lambda x: np.percentile(x, 75))
                ]
            })
        features_item_lq_num.columns = features_item_lq_num.columns.droplevel(level=0)

        features = join_datasets(features_item, features_item_lq_num, how='left', key=None)

        pd.testing.assert_index_equal(features.index, features_item.index)

        features_item = features.copy()

        features_item['na_transaction_lq'] = features_item['items_count_lq'].isna().astype('float64')

        # Fill missing values
        na_fill_dict = {
            'items_count_lq': 0,
            'marginCAD_item_avg_lq': 0,
            'markdown_item_count_lq': 0,
            'marginCAD_item_unit_avg_lq': 0,
            'marginCAD_item_unit_max_lq': 0,
            'marginCAD_item_unit_min_lq': 0,
            'marginCAD_item_unit_median_lq': 0,
            'marginCAD_item_unit_q25_lq': 0,
            'marginCAD_item_unit_q75_lq': 0
        }

        features_item = features_item.fillna(na_fill_dict)

        return features_item

    def data_net_cart(self, dataset):

        """

        Cart net level feature engineering

        :param dataset
        :return: dataframe
        """

        logger.info('Creating cart net level features... ')

        df_net_cart = dataset.groupby(['ID', 'cartID', 'cartDate',
                                       'cartDate_MD_flag', 'cartDate_MD_season',
                                       'cartDate_MD_wave'
                                       ]) \
            .agg({
                'nNet': [('nItems_inCart', 'sum')],
                'valueCAD_net': [('valueCAD_cart', 'sum')],
                'marginCAD_net': [('marginCAD_cart', 'sum')],
                'marginCAD_net_unit': [
                    ('marginCAD_item_unit_avg', 'mean'),
                    ('marginCAD_item_unit_median', 'median')
                ]
            })  # , 'cartDate_MD_year'

        df_net_cart.columns = df_net_cart.columns.droplevel(level=0)
        df_net_cart.reset_index(inplace=True)

        # Numeric features
        end_date = pd.to_datetime(self.end_date)

        features_cart_num = df_net_cart \
            .assign(cartDate_ordinal=lambda x: pd.to_datetime(x.cartDate).astype(np.int64) / 10 ** 9) \
            .groupby('ID') \
            .agg({
                'cartID':  [('cart_count_net', 'count')],
                'cartDate': [
                    ('cart_date_min', 'min'),
                    ('cart_date_max', 'max')
                ],
                'cartDate_MD_flag': [
                    ('carts_inMD_period_net', 'sum'),
                    ('carts_inMD_period_ratio_net', 'mean')
                ],
                'nItems_inCart': [
                    ('item_avg_cart', 'mean'),
                    ('item_std_cart', 'std'),
                    ('item_max_cart', 'max'),
                    ('item_median_cart', 'median'),
                    ('item_q25_cart', lambda x: np.percentile(x, 25)),
                    ('item_q75_cart', lambda x: np.percentile(x, 75))
                ],
                'valueCAD_cart': [('valueCAD_sum_cart', 'sum')],
                'marginCAD_cart': [
                    ('marginCAD_sum_cart', 'sum'),
                    ('marginCAD_avg_cart', 'mean'),
                    ('marginCAD_std_cart', 'std'),
                    ('marginCAD_max_cart', 'max'),
                    ('marginCAD_min_cart', 'min'),
                    ('marginCAD_median_cart', 'median'),
                    ('marginCAD_q25_cart', lambda x: np.percentile(x, 25)),
                    ('marginCAD_q75_cart', lambda x: np.percentile(x, 75))
                ],
                'marginCAD_item_unit_avg': [
                    ('marginCAD_item_unit_avg_min_cart', 'min'),
                    ('marginCAD_item_unit_avg_max_cart', 'max'),
                    ('marginCAD_item_unit_avg_mean_cart', 'mean')
                ],
                'marginCAD_item_unit_median': [
                    ('marginCAD_item_unit_median_min_cart', 'min'),
                    ('marginCAD_item_unit_median_max_cart', 'max'),
                    ('marginCAD_item_unit_median_mean_cart', 'mean')
                ],
                'cartDate_ordinal': [
                    ('cartDate_recent_net', 'max'),
                    ('cartDate_min_net', 'min'),
                    ('cartDate_avg_net', 'mean'),
                    ('cartDate_std_net', 'std'),
                    ('cartDate_median_net', 'median'),
                    ('cartDate_q25_net', lambda x: np.percentile(x, 25)),
                    ('cartDate_q75_net', lambda x: np.percentile(x, 75))
                ]
            })

        features_cart_num.columns = features_cart_num.columns.droplevel(level=0)

        features_cart_num['cart_date_min'] = features_cart_num['cart_date_min'].astype('datetime64[ns]')
        features_cart_num['cart_date_max'] = features_cart_num['cart_date_max'].astype('datetime64[ns]')

        features_cart_num = features_cart_num.assign(
            day_since_last_year_first_order_net=lambda x: (end_date - x.cart_date_min).dt.days,
            day_since_last_order_net=lambda x: (end_date - x.cart_date_max).dt.days
        )

        features_cart_num.drop(columns=['cart_date_min', 'cart_date_max'], axis=1, inplace=True)

        # Calculate card volumes per md and fp
        features_cart_num_md_ = dataset \
            .assign(markdown=lambda x: ['MD' if flag else 'FP' for flag in x.markdownFlag]) \
            .groupby(['ID', 'markdown']) \
            .agg({
                'marginCAD_net': 'sum'
            })

        features_cart_num_md = features_cart_num_md_.unstack(fill_value=0)
        features_cart_num_md.columns = ['_'.join(str(s).strip() for s in col) for col in features_cart_num_md.columns]

        features_cart = pd.concat(
            [features_cart_num,
             features_cart_num_md],
            axis=1)

        # last quarter features
        last_quarter_date = (end_date - datetime.timedelta(weeks=12)).strftime('%Y-%m-%d')

        df_net_cart_lq = df_net_cart[df_net_cart.cartDate.astype('str') >= last_quarter_date]

        features_cart_lq_num = df_net_cart_lq.groupby(['ID'])\
            .agg({
                'cartID': [('cart_count_net_lq', 'count')],
                'nItems_inCart': [
                    ('item_avg_cart_lq', 'mean'),
                    ('item_std_cart_lq', 'std'),
                    ('item_max_cart_lq', 'max'),
                    ('item_median_cart_lq', 'median'),
                    ('item_q25_cart_lq', lambda x: np.percentile(x, 25)),
                    ('item_q75_cart_lq', lambda x: np.percentile(x, 75))
                ],
                'valueCAD_cart': [('valueCAD_sum_cart_lq', 'sum')],
                'marginCAD_cart': [
                    ('marginCAD_sum_cart_lq', 'sum'),
                    ('marginCAD_avg_cart_lq', 'mean'),
                    ('marginCAD_std_cart_lq', 'std'),
                    ('marginCAD_max_cart_lq', 'max'),
                    ('marginCAD_min_cart_lq', 'min'),
                    ('marginCAD_median_cart_lq', 'median'),
                    ('marginCAD_q25_cart_lq', lambda x: np.percentile(x, 25)),
                    ('marginCAD_q75_cart_lq', lambda x: np.percentile(x, 75))
                ],
                'marginCAD_item_unit_avg': [
                    ('marginCAD_item_unit_avg_min_cart_lq', 'min'),
                    ('marginCAD_item_unit_avg_max_cart_lq', 'max'),
                    ('marginCAD_item_unit_avg_mean_cart_lq', 'mean')
                ]
            })
        features_cart_lq_num.columns = features_cart_lq_num.columns.droplevel(level=0)

        features = join_datasets(features_cart, features_cart_lq_num, how='left', key=None)

        pd.testing.assert_index_equal(features.index, features_cart.index)

        features_cart = features.copy()

        # Fill missing values
        na_fill_dict = {
            'cart_count_net_lq': 0,
            'item_avg_cart_lq': 0,
            'item_max_cart_lq': 0,
            'item_median_cart_lq': 0,
            'item_q25_cart_lq': 0,
            'item_q75_cart_lq': 0,
            'valueCAD_sum_cart_lq': 0,
            'marginCAD_sum_cart_lq': 0,
            'marginCAD_avg_cart_lq': 0,
            'marginCAD_max_cart_lq': 0,
            'marginCAD_min_cart_lq': 0,
            'marginCAD_median_cart_lq': 0,
            'marginCAD_q25_cart_lq': 0,
            'marginCAD_q75_cart_lq': 0,
            'marginCAD_item_unit_avg_min_cart_lq': 0,
            'marginCAD_item_unit_avg_max_cart_lq': 0,
            'marginCAD_item_unit_avg_mean_cart_lq': 0
        }

        features_cart = features_cart.fillna(na_fill_dict)

        return features_cart

    def data_return(self, dataset):

        """

        Return feature engineering

        :param dataset
        :return: dataframe
        """

        logger.info('Creating return-related features... ')

        # returned items but not fraud
        df_ret = dataset.copy()

        # Categorical features
        cat_vars = ['return_reason']

        features_return_cat_stats = df_ret[['ID', 'nR']]
        features_return_cat_list = list()
        for cat_var in cat_vars:
            df_onehot = one_hot_encode(df_ret[cat_var], threshold=self.threshold)
            temp = pd.concat([features_return_cat_stats, df_onehot], axis=1)

            temp = temp.loc[temp.index.repeat(temp['nR'].astype('int64'))]
            temp.drop(columns='nR', inplace=True)

            temp = temp.groupby(['ID']).agg('sum')
            sum_row = temp.sum(axis=1)
            temp_pct = temp.div(sum_row, axis=0)
            temp_pct.rename(columns=lambda x: 'pct_' + x, inplace=True)
            features_return_cat_list.append(pd.concat([temp, temp_pct], axis=1))

        features_return_cat = pd.concat(features_return_cat_list, axis=1)

        # Numerical features
        features_return_num = df_ret.groupby(['ID']) \
            .agg({
                'cartID': [('cart_count_ret', 'nunique')],
                'nR': [('items_count_ret', 'sum')],
                'valueCAD_R': [
                    ('valueCAD_R_sum', 'sum'),
                    ('valueCAD_R_avg', 'mean'),
                    ('valueCAD_R_median', 'median'),
                    ('valueCAD_R_min', 'min'),
                    ('valueCAD_R_max', 'max')
                ]
            })

        features_return_num.columns = features_return_num.columns.droplevel(level=0)

        features_return = pd.concat([features_return_cat,
                                     features_return_num],
                                    axis=1)

        return features_return

    @staticmethod
    def flag_res(dataset):

        """

        Return resellers

        :param dataset
        :return: dataframe
        """

        logger.info('Introducing resellers... ')

        df_res = dataset[['ID', 'flagReseller']].drop_duplicates(subset='ID').copy()

        df_res.set_index('ID', inplace=True)

        return df_res

    @staticmethod
    def merge_feats(dict_dataset: dict):

        """

        Merge trx features

        :param dict_dataset
        :return: dataframe
        """

        logger.info('Merging trx features...')

        features_concat = pd.concat(
            objs=dict_dataset.values(),
            axis=1,
            sort=False
        )

        # Fill na on non-std/ratio variables
        dataset = pd.concat(
            [features_concat.filter(regex='^((?!std|ratio).)*$', axis=1).fillna(0),
             features_concat.filter(regex='(std|ratio)', axis=1)],
            axis=1
        )

        dataset = dataset.assign(
            return_ratio_items=lambda x: x.items_count_ret/(x.items_count_ret + x.items_count),
            return_ratio_valueCAD=lambda x: x.valueCAD_R_sum/(x.valueCAD_R_sum + x.valueCAD_sum_cart)
        )

        return dataset

    @staticmethod
    def post_res(dataset):

        dataset = dataset.copy()

        # Treat resellers
        non_rs_items_count = dataset[dataset.flagReseller == 0].items_count.max()
        non_rs_valueCAD_sum_cart = dataset[dataset.flagReseller == 0].valueCAD_sum_cart.max()
        non_rs_valueCAD_sum_cart_lq = dataset[dataset.flagReseller == 0].valueCAD_sum_cart_lq.max()
        non_rs_valueCAD_R_sum = dataset[dataset.flagReseller == 0].valueCAD_R_sum.max()
        non_rs_marginCAD_sum_cart = dataset[dataset.flagReseller == 0].marginCAD_sum_cart.max()
        non_rs_marginCAD_sum_cart_lq = dataset[dataset.flagReseller == 0].marginCAD_sum_cart_lq.max()

        dataset.loc[dataset.flagReseller == 1, 'items_count'] = np.where(
            dataset[dataset.flagReseller == 1].items_count > non_rs_items_count, non_rs_items_count,
            dataset.loc[dataset.flagReseller == 1, 'items_count'])
        dataset.loc[dataset.flagReseller == 1, 'valueCAD_sum_cart'] = np.where(
            dataset[dataset.flagReseller == 1].valueCAD_sum_cart > non_rs_valueCAD_sum_cart, non_rs_valueCAD_sum_cart,
            dataset.loc[dataset.flagReseller == 1, 'valueCAD_sum_cart'])
        dataset.loc[dataset.flagReseller == 1, 'valueCAD_sum_cart_lq'] = np.where(
            dataset[dataset.flagReseller == 1].valueCAD_sum_cart_lq > non_rs_valueCAD_sum_cart_lq,
            non_rs_valueCAD_sum_cart_lq,
            dataset.loc[dataset.flagReseller == 1, 'valueCAD_sum_cart_lq'])
        dataset.loc[dataset.flagReseller == 1, 'valueCAD_R_sum'] = np.where(
            dataset[dataset.flagReseller == 1].valueCAD_R_sum > non_rs_valueCAD_R_sum, non_rs_valueCAD_R_sum,
            dataset.loc[dataset.flagReseller == 1, 'valueCAD_R_sum'])
        dataset.loc[dataset.flagReseller == 1, 'marginCAD_sum_cart'] = np.where(
            dataset[dataset.flagReseller == 1].marginCAD_sum_cart > non_rs_marginCAD_sum_cart, non_rs_marginCAD_sum_cart,
            dataset.loc[dataset.flagReseller == 1, 'marginCAD_sum_cart'])
        dataset.loc[dataset.flagReseller == 1, 'marginCAD_sum_cart_lq'] = np.where(
            dataset[dataset.flagReseller == 1].marginCAD_sum_cart_lq > non_rs_marginCAD_sum_cart_lq,
            non_rs_marginCAD_sum_cart_lq,
            dataset.loc[dataset.flagReseller == 1, 'marginCAD_sum_cart_lq'])

        return dataset

    def trx_data_prep(self):

        trx_dataset = TrxRet(self.start_date, self.end_date, self.ext).ret()

        cart_feats = self.data_cart(trx_dataset)

        trx_dataset_net = trx_dataset[trx_dataset.nNet > 0].copy()

        net_item_feats = self.data_net_item(trx_dataset_net)

        net_cart_feats = self.data_net_cart(trx_dataset_net)

        ret_dataset = trx_dataset[trx_dataset.nR > 0].copy()

        return_feats = self.data_return(ret_dataset)

        res_dataset = TrxPrep.flag_res(trx_dataset)

        dict_df = {
            'cart_feats': cart_feats,
            'net_item_feats': net_item_feats,
            'net_cart_feats': net_cart_feats,
            'return_feats': return_feats,
            'resellers_flag': res_dataset
        }

        trx_feats = TrxPrep.merge_feats(dict_df)

        trx_feats = TrxPrep.post_res(trx_feats)

        logger.info('trx shape: {}'.format(trx_feats.shape))

        return trx_feats

