
"""
@name: calibration.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

# USAGE
# python calibration.py --last_n_weeks 52 --clf_model clf --reg_model reg

import argparse
import datetime
import logging
import pandas as pd
from src.etl import DataExt
from src.local_train import LocalTrain
from src.utils.agg_helper import agg_res, merge_pred, mapping_dict
from src.utils.bq_helper import export_pandas_to_table
from src.utils.resources import load_project_id
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def model_calib(clf_model, reg_model):

    metrics = dict()

    timestamp = datetime.date.today().strftime('%Y-%m-%d')
    metrics.update({'timestamp': [timestamp]})

    clf_calib = LocalTrain(model_name=clf_model)

    y_clf_pred, f1_score, _, _, _ = clf_calib.fit_model()

    metrics.update({'f1_score': [f1_score]})

    reg_calib = LocalTrain(model_name=reg_model)

    y_reg_pred, mae, mape, log_mae, log_mape = reg_calib.fit_model()

    metrics.update({'log_scale_mae_score': [log_mae], 'log_scale_mape_score': [log_mape],
                    'mae_score': [mae], 'mape_score': [mape]})

    merge_df, val_index, exp_mae = merge_pred(y_clf_pred)

    metrics.update({'exp_mae': [exp_mae]})

    exp_mae = agg_res(merge_df, val_index, level=mapping_dict['cak_level'],
                      cols=mapping_dict['cak_col'], file_name='agg_cak.csv')

    metrics.update({'camp_addg_crit_exp_mae': [exp_mae]})

    exp_mae = agg_res(merge_df, val_index, level=mapping_dict['ca_level'],
                      cols=mapping_dict['ca_col'], file_name='agg_ca.csv')

    metrics.update({'camp_addg_exp_mae': [exp_mae]})

    exp_mae = agg_res(merge_df, val_index, level=mapping_dict['c_level'],
                      cols=mapping_dict['c_col'], file_name='agg_c.csv')

    metrics.update({'camp_exp_mae': [exp_mae]})

    metrics_df = pd.DataFrame(metrics)

    logger.info('Uploading metric values to BigQuery...')

    export_pandas_to_table(dataset_id='ds_sessions_value', table_id='cron_tracker', dataset=metrics_df,
                           project_id=load_project_id(), if_exists='append')


def get_args():
    """
    Get input arguments

    """

    parser = argparse.ArgumentParser(description="Building CLTV predictive model",
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

    data_ext = DataExt(last_n_weeks=args.last_n_weeks, aws_env=args.aws_env, calib=True, non_adj=True)

    data_ext.extract_transform_load()

    model_calib(clf_model=args.clf_model, reg_model=args.reg_model)

    logger.info('Total elapsed time: {}'.format(sw.elapsed.human_str()))


