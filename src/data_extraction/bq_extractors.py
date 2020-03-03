
import datetime
import pandas as pd
from src.data_extraction.task.query_task import BigQueryConnector
from src.utils.path_helper import get_query


class AdUsers(BigQueryConnector):
    """

    Extract users and ads related data

    """

    def __init__(self, dataset_id, ext,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames
        self.ext = ext

    def query(self):
        if self.ext:
            return (get_query('ad_users_pt.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    )
        else:
            return (get_query('ad_users_po.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class AdRaw(BigQueryConnector):

    def __init__(self, dataset_id,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames

    def query(self):
        return (get_query('adwords.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class AudRaw(BigQueryConnector):

    def __init__(self, dataset_id,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames

    def query(self):
        return (get_query('audiences.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class BrxFeat(BigQueryConnector):
    """

    Aggregate all features

    """

    def __init__(self, dataset_id, ext,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames
        self.ext = ext

    def query(self):
        if self.ext:
            return (get_query('brx_features_pt.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    )
        else:
            return (get_query('brx_features_po.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class BrxSamples(BigQueryConnector):
    """

    Take a sample of browsing features

    """

    def __init__(self, dataset_id,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames

    def query(self):
        return (get_query('brx_sample.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class TrxInvoice(BigQueryConnector):
    """

    Extract invoices

    """

    def __init__(self, dataset_id, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.colnames = colnames

    def query(self):
        return (get_query('invoices.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                .replace('@start_date', self.start_date)
                .replace('@end_date', self.end_date)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class PageFeat(BigQueryConnector):
    """

    Feature engineering on page features

    """

    def __init__(self, dataset_id, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.colnames = colnames

    def query(self):
        return (get_query('page_features.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                .replace('@start_date', self.start_date)
                .replace('@end_date', self.end_date)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class PageRaw(BigQueryConnector):

    def __init__(self, dataset_id, ext, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.colnames = colnames
        self.ext = ext

    def query(self):
        if self.ext:
            return (get_query('page_raw_pt.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    .replace('@start_date', self.start_date)
                    .replace('@end_date', self.end_date)
                    )
        else:
            return (get_query('page_raw_po.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    .replace('@start_date', self.start_date)
                    .replace('@end_date', self.end_date)
                    )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class PdpFeat(BigQueryConnector):
    """

    Feature engineering on page views related to pdp

    """

    def __init__(self, dataset_id, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.last_quarter_date = (pd.to_datetime(self.end_date) - datetime.timedelta(weeks=12)).strftime('%Y-%m-%d')
        self.colnames = colnames

    def query(self):
        return (get_query('pdp_features.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                .replace('@start_date', self.start_date)
                .replace('@end_date', self.end_date)
                .replace('@last_quarter_date', self.last_quarter_date)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class SessionRaw(BigQueryConnector):

    def __init__(self, dataset_id, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.colnames = colnames

    def query(self):
        return (get_query('session_raw_agg.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                .replace('@start_date', self.start_date)
                .replace('@end_date', self.end_date)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class SessionMd(BigQueryConnector):

    def __init__(self, dataset_id, ext, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.colnames = colnames
        self.ext = ext

    def query(self):
        if self.ext:
            return (get_query('session_md_pt.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    .replace('@start_date', self.start_date)
                    .replace('@end_date', self.end_date)
                    )
        else:
            return (get_query('session_md_po.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    .replace('@start_date', self.start_date)
                    .replace('@end_date', self.end_date)
                    )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class SessDate(BigQueryConnector):
    """

    Keep track of session dates in the period of prediction

    """

    def __init__(self, dataset_id,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames

    def query(self):
        return (get_query('session_date_po.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class SessionFeat(BigQueryConnector):
    """

    Feature engineering on session features

    """

    def __init__(self, dataset_id, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.last_quarter_date = (pd.to_datetime(self.end_date) - datetime.timedelta(weeks=12)).strftime('%Y-%m-%d')
        self.colnames = colnames

    def query(self):
        return (get_query('session_features.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                .replace('@start_date', self.start_date)
                .replace('@end_date', self.end_date)
                .replace('@last_quarter_date', self.last_quarter_date)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class SessMdpo(BigQueryConnector):

    def __init__(self, dataset_id, table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames

    def query(self):
        return (get_query('session_md_po_update.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class SessUpdate(BigQueryConnector):
    """

    Take a sample of browsing features

    """

    def __init__(self, dataset_id,
                 table_id, colnames, overwrite):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames
        self.overwrite = overwrite

    def query(self):
        return (get_query('sessionId_update.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query(), self.overwrite)
                )


class AppSessionRaw(BigQueryConnector):

    def __init__(self, dataset_id, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.colnames = colnames

    def query(self):
        return (get_query('app_session_raw_agg.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                .replace('@start_date', self.start_date)
                .replace('@end_date', self.end_date)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class AppSessionMd(BigQueryConnector):

    def __init__(self, dataset_id, ext, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.colnames = colnames
        self.ext = ext

    def query(self):
        if self.ext:
            return (get_query('app_session_md_pt.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    .replace('@start_date', self.start_date)
                    .replace('@end_date', self.end_date)
                    )
        else:
            return (get_query('app_session_md_po.sql')
                    .open()
                    .read()
                    .replace('@colnames', self.colnames)
                    .replace('@start_date', self.start_date)
                    .replace('@end_date', self.end_date)
                    )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class AppPageRaw(BigQueryConnector):

    def __init__(self, dataset_id, start_date, end_date,
                 table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.start_date = start_date
        self.end_date = end_date
        self.colnames = colnames

    def query(self):
        return (get_query('app_page_raw.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                .replace('@start_date', self.start_date)
                .replace('@end_date', self.end_date)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )


class UnionData(BigQueryConnector):
    """

    Union bq and mobile app data

    """

    def __init__(self, dataset_id, table_id, query_filename, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.query_filename = query_filename
        self.colnames = colnames

    def query(self):
        return (get_query(self.query_filename)
                .open()
                .read()
                .replace('@colnames', self.colnames)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query(), overwrite=True)
                )


class AppSessMdpo(BigQueryConnector):

    def __init__(self, dataset_id, table_id, colnames):
        super().__init__(dataset_id)
        self.table_id = table_id
        self.colnames = colnames

    def query(self):
        return (get_query('app_session_md_po_update.sql')
                .open()
                .read()
                .replace('@colnames', self.colnames)
                )

    def run(self, *args):
        return (super()
                .run(self.table_id, self.query())
                )
