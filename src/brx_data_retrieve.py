
"""
@name: brx_data_retrieve.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

from src.data_retrieve import DataRet
from src.data_extraction.bq_extractors import *
from src.utils.bq_helper import *


class BrxRet(DataRet):

    def __init__(self, start_date, end_date, ext, non_adj):
        super().__init__(start_date, end_date)
        self.ext = ext
        self.non_adj = non_adj

    def ad_ext(self, table_id):

        logger.info('Extracting adwords... ')

        ad_raw = AdRaw(self.dataset_id, table_id=table_id, colnames='*')

        ad_raw.run()

    def aud_ext(self, table_id):

        logger.info('Extracting audiences...')

        aud_raw = AudRaw(self.dataset_id, table_id=table_id, colnames='*')

        aud_raw.run()

    def session_ext(self, table_id):

        logger.info('Extracting session history while removing overnight ones...')

        session_raw = SessionRaw(self.dataset_id, self.start_date, self.end_date,
                                 table_id=table_id, colnames='*')

        session_raw.run()

    def app_session_ext(self, table_id):

        logger.info('Extracting mobile-app session history...')

        session_raw = AppSessionRaw(self.dataset_id, self.start_date, self.end_date,
                                    table_id=table_id, colnames='*')

        session_raw.run()

    def paid_session_update(self, table_id):

        logger.info('Updating paid sessions table...')

        session_update = SessUpdate(self.dataset_id, table_id=table_id,
                                    colnames='*', overwrite=False)

        session_update.run()

    def session_md_ext(self, table_id):

        logger.info('Integrating session history with markdown info...')

        session_md = SessionMd(self.dataset_id, self.ext, self.start_date, self.end_date,
                               table_id=table_id, colnames='*')

        session_md.run()

    def app_session_md_ext(self, table_id):

        logger.info('Integrating mobile-app session history with markdown info...')

        session_md = AppSessionMd(self.dataset_id, self.ext, self.start_date, self.end_date,
                                  table_id=table_id, colnames='*')

        session_md.run()

    def paid_session_date(self, table_id):

        logger.info('Updating sessions date...')

        p_session_date = SessDate(self.dataset_id, table_id=table_id, colnames='*')

        p_session_date.run()

    def session_md(self, table_id):

        logger.info('Updating session history with markdown info...')

        session_md_po = SessMdpo(self.dataset_id, table_id=table_id, colnames='*')

        session_md_po.run()

    def app_session_md(self, table_id):

        logger.info('Updating mobile-app session history with markdown info...')

        session_md_po = AppSessMdpo(self.dataset_id, table_id=table_id, colnames='*')

        session_md_po.run()

    def page_ext(self, table_id):

        logger.info('Extracting page history...')

        page_raw = PageRaw(self.dataset_id, self.ext, self.start_date, self.end_date,
                           table_id=table_id, colnames='*')

        page_raw.run()

    def app_page_ext(self, table_id):

        logger.info('Extracting mobile-app page history...')

        page_raw = AppPageRaw(self.dataset_id, self.start_date, self.end_date,
                              table_id=table_id, colnames='*')

        page_raw.run()

    def app_bq_union(self, table_id, query_filename):

        logger.info('Merging bq and mobile app data...')

        page_raw = UnionData(self.dataset_id, table_id=table_id, query_filename=query_filename, colnames='*')

        page_raw.run()

    def page_feat_ext(self, table_id):

        logger.info('Generating page related features...')

        page_feat = PageFeat(self.dataset_id, self.start_date, self.end_date,
                             table_id=table_id, colnames='*')

        page_feat.run()

    def pdp_feat_ext(self, table_id):

        logger.info('Generating pdp related features...')

        pdp_feat = PdpFeat(self.dataset_id, self.start_date, self.end_date,
                           table_id=table_id, colnames='*')

        pdp_feat.run()

    def session_feat_ext(self, table_id):

        logger.info('Generating session related features...')

        session_feat = SessionFeat(self.dataset_id, self.start_date, self.end_date,
                                   table_id=table_id, colnames='*')

        session_feat.run()

    def brx_feat_agg(self, table_id):

        logger.info('Aggregating all features at ID level...')

        brx_feat = BrxFeat(self.dataset_id, self.ext, table_id=table_id, colnames='*')

        brx_feat.run()

    def brx_feat_samples(self, table_id):

        logger.info('Sampling features at visitor level...')

        brx_samples = BrxSamples(self.dataset_id, table_id=table_id, colnames='*')

        brx_samples.run()

    def ad_users_ext(self, table_id):

        logger.info('Extracting users and ads related data...')

        ad_users = AdUsers(self.dataset_id, self.ext, table_id=table_id, colnames='*')

        ad_users.run()

    def sync(self, table_id):

        logger.info('Downloading data from {}...'.format(table_id))

        remove_file_from_google_storage(bucket_name=self.bucket_name, prefix=self.prefix)

        export_table_to_google_storage(dataset_id=self.dataset_id, table_id=table_id,
                                       bucket_name=self.bucket_name,
                                       file_name=self.file_name)

        return download_from_storage_to_pandas(bucket_name=self.bucket_name, prefix=self.prefix,
                                               col_type={'ID': 'str'})

    def ret(self):

        self.pd_ext(table_id='_products')

        self.em_ext(table_id='_employees')

        self.ad_ext(table_id='_adwords')

        self.aud_ext(table_id='_audiences')

        self.md_ext(table_id='_markdown', bq=True)

        self.session_ext(table_id='_session_raw_agg')

        self.app_session_ext(table_id='_app_session_raw_agg')

        if self.ext:
            self.session_md_ext(table_id='_session_md')
            self.app_session_md_ext(table_id='_app_session_md')
        else:
            if self.non_adj:
                self.session_md_ext(table_id='_session_md')
                self.app_session_md_ext(table_id='_app_session_md')
            else:
                self.paid_session_update(table_id='_sessionId_update')  # Do not delete _sessionId_update table
                self.session_md_ext(table_id='_session_md_po')
                self.app_session_md_ext(table_id='_app_session_md_po')
                self.session_md(table_id='_session_md')
                self.app_session_md(table_id='_app_session_md')

        self.app_bq_union(table_id='_session_md', query_filename='session_md_union.sql')

        if not self.ext:
            if not self.non_adj:
                self.paid_session_date(table_id='_session_date_po')

        self.page_ext(table_id='_page_raw')

        self.app_page_ext(table_id='_app_page_raw')

        self.app_bq_union(table_id='_page_raw', query_filename='page_union.sql')

        self.page_feat_ext(table_id='_page_features')

        self.pdp_feat_ext(table_id='_pdp_features')

        self.session_feat_ext(table_id='_session_features')

        tables_to_delete = ['_products']
            # , '_employees', '_adwords', '_audiences', '_markdown', '_session_raw_agg',
            #                 '_session_md', '_page_raw', '_page_features', '_pdp_features', '_session_features',
            #                 '_users', '_app_session_raw_agg', '_app_session_md', '_app_page_raw']

        date_dataset = pd.DataFrame()

        if self.ext:

            self.brx_feat_agg(table_id='_brx_features_pt')

            self.brx_feat_samples(table_id='_brx_sample')

            self.ad_users_ext(table_id='_ad_users_pt')

            brx_dataset = self.sync(table_id='_brx_sample')

            ads_dataset = self.sync(table_id='_ad_users_pt')

            ext_tables_to_delete = tables_to_delete
                                   # + ['_brx_features_pt', '_brx_sample', '_ad_users_pt',
                                   #                     '_invoices']

            delete_table(dataset_id=self.dataset_id, table_ids=ext_tables_to_delete)

        else:

            self.brx_feat_agg(table_id='_brx_features_po')

            self.ad_users_ext(table_id='_ad_users_po')

            brx_dataset = self.sync(table_id='_brx_features_po')

            ads_dataset = self.sync(table_id='_ad_users_po')

            if not self.non_adj:
                date_dataset = self.sync(table_id='_session_date_po')
                date_dataset.reset_index(inplace=True, drop=True)

            ext_tables_to_delete = tables_to_delete + ['_brx_features_po', '_ad_users_po', '_invoices_po']

            if not self.non_adj:
                ext_tables_to_delete = ext_tables_to_delete + ['_session_md_po', '_app_session_md_po',
                                                               '_session_date_po']

            delete_table(dataset_id=self.dataset_id, table_ids=ext_tables_to_delete)

        brx_dataset.reset_index(inplace=True, drop=True)

        ads_dataset.reset_index(inplace=True, drop=True)

        logger.info('Shape of brx data: {}'.format(brx_dataset.shape))

        logger.info('Shape of ads data: {}'.format(ads_dataset.shape))

        return brx_dataset, ads_dataset, date_dataset
