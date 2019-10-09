
"""
@name: trx_data_retrieve.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

import numpy as np
from src.data_retrieve import DataRet
from src.utils.resources import normalize
from src.utils.dataframe import *
from src.data_extraction.carts import CartsQuery
from src.utils.bq_helper import *


class TrxRet(DataRet):

    def __init__(self, start_date, end_date, ext):
        super().__init__(start_date, end_date)
        self.ext = ext

    def sync(self, table_id):

        logger.info('Downloading invoices... ')

        remove_file_from_google_storage(bucket_name=self.bucket_name, prefix=self.prefix)

        export_table_to_google_storage(dataset_id=self.dataset_id, table_id=table_id,
                                       bucket_name=self.bucket_name, dir_name=self.dir_name,
                                       file_name=self.file_name)

        invoice_dataset = download_from_storage_to_pandas(bucket_name=self.bucket_name, prefix=self.prefix,
                                                          col_type={'fullVisitorId': 'str'})

        return invoice_dataset

    def carts_ext(self):

        logger.info('Extracting transactional records... ')
        
        trx_dataset = CartsQuery(self.start_date, self.end_date, colnames='*', cols_list=['category']).get()

        # trx_dataset.brand = trx_dataset.brand.apply(normalize)

        return trx_dataset

    @staticmethod
    def flag_resellers(left_dataset, resellers_dataset, how, key):

        logger.info('Flagging resellers... ')

        dataset = join_datasets(left_dataset, resellers_dataset, how=how, key=key)
        dataset['flagReseller'].fillna(0, inplace=True)

        return dataset

    @staticmethod
    def markdown_info(left_dataset, markdown_dataset, how, key):

        logger.info('Merging markdown info... ')

        markdown_dataset.rename(columns={
            'date': 'cartDate',
            'isMD': 'cartDate_MD_flag',
            'season': 'cartDate_MD_season',
            'wave': 'cartDate_MD_wave',
            'currentYear': 'cartDate_MD_year'
        }, inplace=True)

        columns = ['cartDate_MD_season', 'cartDate_MD_wave', 'cartDate_MD_year']
        for col in columns:
            markdown_dataset[col] = cast_type(markdown_dataset, col, 'str')

        dataset = join_datasets(left_dataset, markdown_dataset, how=how, key=key)

        return dataset

    def net_feats(self, dataset):

        logger.info('Adding net features... ')

        dataset = dataset.copy()

        dataset = dataset.assign(
            nNet=lambda x: x.nP - x.nR,
            valueCAD_net=lambda x: (x.valueCAD_P - x.valueCAD_R),
            marginCAD_net=lambda x: np.where(x.valueCAD_net - (x.cogsCAD_P - x.cogsCAD_R) > 0,
                                             x.valueCAD_net - (x.cogsCAD_P - x.cogsCAD_R),
                                             0),
            marginCAD_net_unit=lambda x: x.marginCAD_net / x.nNet,
            weightedMarkdown=lambda x: x.markdownFlag * x.nNet,
            weightedsalePercentage=lambda x: x.salePercentage * x.nNet,
            season=lambda x: x.seasonYear.str.slice(0, 2)
        )

        dataset = dataset[dataset.fullVisitorId.notnull()].copy()

        # Treat resellers
        non_rs_nNet = dataset[dataset.flagReseller == 0].nNet.max()
        non_rs_valueCAD_net = dataset[dataset.flagReseller == 0].valueCAD_net.max()
        non_rs_valueCAD_R = dataset[dataset.flagReseller == 0].valueCAD_R.max()
        non_rs_marginCAD_net = dataset[dataset.flagReseller == 0].marginCAD_net.max()

        dataset.loc[dataset.flagReseller == 1, 'nNet'] = np.where(
            dataset[dataset.flagReseller == 1].nNet > non_rs_nNet, non_rs_nNet,
            dataset.loc[dataset.flagReseller == 1, 'nNet'])
        dataset.loc[dataset.flagReseller == 1, 'valueCAD_net'] = np.where(
            dataset[dataset.flagReseller == 1].valueCAD_net > non_rs_valueCAD_net, non_rs_valueCAD_net,
            dataset.loc[dataset.flagReseller == 1, 'valueCAD_net'])
        dataset.loc[dataset.flagReseller == 1, 'valueCAD_R'] = np.where(
            dataset[dataset.flagReseller == 1].valueCAD_R > non_rs_valueCAD_R, non_rs_valueCAD_R,
            dataset.loc[dataset.flagReseller == 1, 'valueCAD_R'])
        dataset.loc[dataset.flagReseller == 1, 'marginCAD_net'] = np.where(
            dataset[dataset.flagReseller == 1].marginCAD_net > non_rs_marginCAD_net, non_rs_marginCAD_net,
            dataset.loc[dataset.flagReseller == 1, 'marginCAD_net'])

        # dataset.fullVisitorId = cast_type(dataset, 'fullVisitorId', 'int64')
        if self.ext:
            mid_dataset = dataset[['memberID', 'fullVisitorId']].copy()
            mid_dataset.sort_values(by='memberID', inplace=True)
            mid_dataset.drop_duplicates(subset=['fullVisitorId'], inplace=True)
            self.mid_ext(table_id='_users', dataset=mid_dataset)

        dataset.rename(columns={'memberID': 'ID'}, inplace=True)

        return dataset

    def ret(self):

        if self.ext:
            invoice_dataset = self.sync(table_id='_invoices')
        else:
            invoice_dataset = self.sync(table_id='_invoices_po')

        trx_dataset = self.carts_ext()

        resellers_dataset = self.rs_ext()

        markdown_dataset = self.md_ext()

        # Flag resellers
        dataset = TrxRet.flag_resellers(trx_dataset, resellers_dataset, how='left', key='memberID')

        # Join markdown info
        dataset = TrxRet.markdown_info(dataset, markdown_dataset, how='inner', key='cartDate')

        # Join fullVisitorId
        dataset = join_datasets(dataset, invoice_dataset[['fullVisitorId', 'invoiceID']], how='left', key='invoiceID')

        logger.info('Shape of trx data before removing users: {}'.format(dataset.shape))

        # dump(dataset, get_data_dir('trx_df_po.pkl'))

        dataset = self.net_feats(dataset)

        logger.info('Shape of trx data after removing users: {}'.format(dataset.shape))

        return dataset



