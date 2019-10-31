
"""
@name: brx_data_retrieve.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

from src.data_retrieve import DataRet
from src.data_extraction.session_agg import SessionRaw
from src.data_extraction.session_agg_md import SessionMd
from src.data_extraction.audiences import AudRaw
from src.data_extraction.adwords import AdRaw
from src.data_extraction.page_raw import PageRaw
from src.data_extraction.page_features import PageFeat
from src.data_extraction.pdp_features import PdpFeat
from src.data_extraction.session_features import SessionFeat
from src.data_extraction.brx_features import BrxFeat
from src.data_extraction.brx_samples import BrxSamples
from src.utils.bq_helper import *


class BrxRet(DataRet):

    def __init__(self, start_date, end_date, ext):
        super().__init__(start_date, end_date)
        self.ext = ext

    def ad_ext(self, table_id):

        logger.info('Extracting adwords... ')

        ad_raw = AdRaw(self.dataset_id, table_id=table_id, colnames='*')

        ad_raw.run()

    def aud_ext(self, table_id):

        logger.info('Extracting audiences... ')

        aud_raw = AudRaw(self.dataset_id, table_id=table_id, colnames='*')

        aud_raw.run()

    def session_ext(self, table_id):

        logger.info('Extracting session history while removing overnight ones... ')

        session_raw = SessionRaw(self.dataset_id, self.start_date, self.end_date,
                                 table_id=table_id, colnames='*')

        session_raw.run()

    def session_md_ext(self, table_id):

        logger.info('Integrating session history with markdown info... ')

        session_md = SessionMd(self.dataset_id, self.ext, self.start_date, self.end_date,
                               table_id=table_id, colnames='*')

        session_md.run()

    def page_ext(self, table_id):

        logger.info('Extracting page history... ')

        page_raw = PageRaw(self.dataset_id, self.start_date, self.end_date,
                           table_id=table_id, colnames='*')

        page_raw.run()

    def page_feat_ext(self, table_id):

        logger.info('Generating page related features... ')

        page_feat = PageFeat(self.dataset_id, self.start_date, self.end_date,
                             table_id=table_id, colnames='*')

        page_feat.run()

    def pdp_feat_ext(self, table_id):

        logger.info('Generating pdp related features... ')

        pdp_feat = PdpFeat(self.dataset_id, self.start_date, self.end_date,
                           table_id=table_id, colnames='*')

        pdp_feat.run()

    def session_feat_ext(self, table_id):

        logger.info('Generating session related features... ')

        session_feat = SessionFeat(self.dataset_id, self.start_date, self.end_date,
                                   table_id=table_id, colnames='*')

        session_feat.run()

    def brx_feat_agg(self, table_id):

        logger.info('Aggregating all features at visitor level... ')

        brx_feat = BrxFeat(self.dataset_id, self.ext, table_id=table_id, colnames='*')

        brx_feat.run()

    def brx_feat_samples(self, table_id):

        logger.info('Sampling features at visitor level... ')

        brx_samples = BrxSamples(self.dataset_id, self.ext, table_id=table_id, colnames='*')

        brx_samples.run()

    def sync(self, table_id):

        logger.info('Downloading features at visitor level... ')

        remove_file_from_google_storage(bucket_name=self.bucket_name, prefix=self.prefix)

        export_table_to_google_storage(dataset_id=self.dataset_id, table_id=table_id,
                                       bucket_name=self.bucket_name,
                                       file_name=self.file_name)

        return download_from_storage_to_pandas(bucket_name=self.bucket_name, prefix=self.prefix,
                                               col_type={'ID': 'str'})

    def ret(self):

        if self.ext:

            self.pd_ext(table_id='_products')

            self.em_ext(table_id='_employees')

            self.ad_ext(table_id='_adwords')

            self.aud_ext(table_id='_audiences')

        self.md_ext(table_id='_markdown', bq=True)

        self.session_ext(table_id='_session_raw_agg')

        self.session_md_ext(table_id='_session_md')

        self.page_ext(table_id='_page_raw')

        self.page_feat_ext(table_id='_page_features')

        self.pdp_feat_ext(table_id='_pdp_features')

        self.session_feat_ext(table_id='_session_features')

        self.brx_feat_agg(table_id='_brx_features')

        if self.ext:

            self.brx_feat_samples(table_id='_brx_sample')

            dataset = self.sync(table_id='_brx_sample')

        else:

            dataset = self.sync(table_id='_brx_features')

        # delete_table(self.dataset_id, table_id=['_adwords', '_audiences', '_employees', '_invoices',
        #                                         '_markdown', '_page_features', '_page_raw', '_pdp_features',
        #                                         '_products', '_session_features', '_session_raw_agg',
        #                                         '_session_raw_agg', '_users'])

        # if self.ext:
            # delete_table(self.dataset_id, table_id=['_invoices'])
        # else:
            # delete_table(self.dataset_id, table_id=['_invoices_po'])

        dataset.reset_index(inplace=True, drop=True)

        logger.info('Shape of brx data: {}'.format(dataset.shape))

        return dataset



