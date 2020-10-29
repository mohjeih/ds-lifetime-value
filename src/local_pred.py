
"""
@name: local_pred.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

import argparse
import datetime
import logging
import numpy as np
import pandas as pd
import xgboost as xgb
from pathlib import Path
from src.etl import DataExt
from src.utils.path_helper import get_data_dir, get_model_dir
from src.utils.resources import load
from src.utils.bq_helper import export_pandas_to_table
from src.utils.resources import load_project_id
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class LocalPred(object):

    def __init__(self, file_dict: dict, dataset: pd.DataFrame):

        self.file_dict = file_dict
        self.dataset = dataset

    # @staticmethod
    # def load_pred_data(filename):
    def load_pred_data(self):

        logger.info('Loading prediction data...')

        # X_pred = load(get_data_dir(filename))
        X_pred = self.dataset

        X_date = load(get_data_dir('date_po.pkl'))

        return xgb.DMatrix(data=X_pred.values), X_pred.index.values, X_date

    @staticmethod
    def upload_pred(dataset):

        logger.info('Uploading metric values to BigQuery...')

        export_pandas_to_table(dataset_id='ds_sessions_value', table_id='ltv_scores', dataset=dataset,
                               project_id=load_project_id(), if_exists='append')

    def load_model(self, model_name):

        logger.info('Loading {} model...'.format(model_name))

        model = load(get_model_dir(self.file_dict[model_name]))

        return model

    def predict(self):

        # dpred, ID, X_date = LocalPred.load_pred_data(filename='X_pred.pkl')
        dpred, ID, X_date = self.load_pred_data()

        y_pred_dict = dict()

        timestamp = datetime.date.today().strftime('%Y-%m-%d')

        y_pred_dict.update({'timestamp': np.repeat(timestamp, ID.shape[0])})

        y_pred_dict.update({'ID': ID})

        for key, value in self.file_dict.items():

            model = self.load_model(key)

            logger.info('{} model prediction...'.format(key))

            y_pred = model.predict(dpred)

            logger.info('y_pred shape: {}'.format(y_pred.shape[0]))

            y_pred_dict.update({key: y_pred})

        y_pred = pd.DataFrame(y_pred_dict).rename(columns={'clf': 'proba', 'reg': 'value'})

        y_pred['value'] = np.expm1(y_pred['value'].values) # + 0.5 * np.var(y_pred['value'].values))

        y_pred = y_pred.assign(expected_value=lambda x: np.round(x.proba*x.value, 4))

        y_pred = pd.merge(y_pred, X_date, how='left', on='ID')

        y_pred = y_pred[['timestamp', 'date', 'ID', 'proba', 'value', 'expected_value']]

        LocalPred.upload_pred(y_pred)

        for f in ['X_pred.pkl', 'ads_po.pkl', 'date_po.pkl']:
            if Path(get_data_dir(f)).is_file():
                Path.unlink(get_data_dir(f))

        return y_pred


def get_args():
    """
    Get input arguments

    """

    parser = argparse.ArgumentParser(description="Predicting probabilities and monetary values",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--last_n_weeks', action='store', help="The number of weeks", dest='last_n_weeks', type=int,
                        default=52)

    parser.add_argument('--aws_env', action='store', help="AWS environment", dest='aws_env', type=str,
                        default='ssense-cltv-qa')

    parser.add_argument('--clf_model', action='store', help="Name of classifier", dest='clf_model', type=str,
                        default='clf')

    parser.add_argument('--reg_model', action='store', help="Name of regressor", dest='reg_model', type=str,
                        default='reg')

    return parser.parse_args()


if __name__ == '__main__':

        sw = Stopwatch(start=True)

        args = get_args()

        data_ext = DataExt(last_n_weeks=args.last_n_weeks, aws_env=args.aws_env, calib=False, non_adj=False)

        _, _, _, _, X_pred = data_ext.extract_transform_load()

        file_dict = {args.clf_model: 'clf-model',
                     args.reg_model: 'reg-model'}

        y_pred = LocalPred(file_dict=file_dict, dataset=X_pred).predict()

        logger.info('Total elapsed time: {}'.format(sw.elapsed.human_str()))