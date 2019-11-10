
"""
@name: train.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

# USAGE
# python train.py --last_n_weeks 52 --aws_env ssense-cltv-qa --clf_model clf --reg_model reg

import argparse
import logging
from src.etl import DataExt
from src.remote_train import RemoteTrain
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def get_args():
    """
    Return input arguments

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

    data_ext = DataExt(last_n_weeks=args.last_n_weeks, aws_env=args.aws_env, calib=True)

    data_ext.extract_transform_load()

    clf_tr = RemoteTrain(model_name=args.clf_model, aws_env=args.aws_env)

    clf_tr.train()

    reg_tr = RemoteTrain(model_name=args.reg_model, aws_env=args.aws_env)

    reg_tr.train()

    logger.info('Total elapsed time: {}'.format(sw.elapsed.human_str()))

