
"""
@name: main.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

# USAGE
# python main.py --aws_env ssense-cltv-qa --model clf

import argparse
import boto3
import logging
import sagemaker
from pprint import pprint
from sagemaker.amazon.amazon_estimator import get_image_uri
from sagemaker.estimator import Estimator
from sagemaker.tuner import IntegerParameter, ContinuousParameter, HyperparameterTuner
from src.etl import DataExt
from src.utils.db_engine import s3_aws_engine, get_aws_role
from src.utils.path_helper import *
from src.utils.resources import *
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class ModelTune(object):

    def __init__(self, model_name, aws_env):

        self.model_name = model_name
        self.aws_env = aws_env
        self.role = self._get_role()

    @staticmethod
    def _aws_s3_path(s3_bucket):
        logger.info('Getting S3 bucket path...')

        s3_bucket_path = s3_bucket + '/'

        return 's3://' + s3_bucket_path

    @staticmethod
    def _boto_session(id, secret):

        logger.info('Creating boto session...')

        return boto3.Session(aws_access_key_id=id, aws_secret_access_key=secret, region_name='us-east-2')

    def _get_imb_ratio(self):
        if self.model_name == 'clf':
            return load_param_json(get_params_dir('imb_ratio.json'))

    def _get_role(self):
        if '-qa' in self.aws_env:
            return get_aws_role('ssense-role-qa')
        else:
            return get_aws_role('ssense-role-prod')

    def fetch_data(self, s3_path):

        logger.info('Creating pointers to the files in S3...')

        train_path = s3_path + 'train/' + self.model_name + '_train.csv'

        s3_input_train = sagemaker.s3_input(s3_data=train_path, content_type='csv')

        val_path = s3_path + 'val/' + self.model_name + '_val.csv'

        s3_input_val = sagemaker.s3_input(s3_data=val_path, content_type='csv')

        return s3_input_train, s3_input_val

    def post_tune(self, sage_sess, tuner):

        tuning_job_res = sage_sess.wait_for_tuning_job(job=tuner.latest_tuning_job.job_name)

        logger.info('The status of tuning job is: {}'.format(tuning_job_res['HyperParameterTuningJobStatus']))

        tuner_df = tuner.analytics().dataframe()

        tuner_df.sort_values(by='FinalObjectiveValue', ascending=True, inplace=True)

        tuner_df.to_csv(get_model_dir(self.model_name + '_tuner_df.csv'), index=False)

        logger.info('The objective is to {}: {}'.format(
            tuning_job_res['HyperParameterTuningJobConfig']['HyperParameterTuningJobObjective']['Type'],
            tuning_job_res['HyperParameterTuningJobConfig']['HyperParameterTuningJobObjective']['MetricName']))

        if tuning_job_res.get('BestTrainingJob', None):
            pprint(tuning_job_res['BestTrainingJob'])

        tuned_dict = dict()
        tuned_dict[self.model_name + '_params'] = {key: float(val) for key, val in
                                                  tuning_job_res['BestTrainingJob']['TunedHyperParameters'].items()}

        logger.info('Best values: {}'.format(tuned_dict))

        dump_param_json(tuned_dict, get_params_dir('tuned_' + self.model_name + '_params.json'))

    def tuning(self):

        s3_bucket, id, secret = s3_aws_engine(name=self.aws_env)

        s3_path = ModelTune._aws_s3_path(s3_bucket)

        boto_sess = ModelTune._boto_session(id, secret)

        logger.info('Getting algorithm image URI...')

        container = get_image_uri(boto_sess.region_name, 'xgboost', repo_version='0.90-1')

        logger.info('Creating sagemaker session...')

        sage_sess = sagemaker.Session(boto_sess)

        s3_input_train, s3_input_val = self.fetch_data(s3_path)

        logger.info('Creating sagemaker estimator to train using the supplied {} model...'.format(self.model_name))

        est = Estimator(container,
                        role=self.role,
                        train_instance_count=1,
                        train_instance_type='ml.m5.2xlarge',
                        output_path=s3_path + 'tuning_' + self.model_name + '/',
                        sagemaker_session=sage_sess,
                        base_job_name=self.model_name + '-tuning-job')

        logger.info('Setting hyper-parameters...')

        hyperparameter_ranges = {'num_round': IntegerParameter(1, 4000),
                                 'eta': ContinuousParameter(0, 0.5),
                                 'max_depth': IntegerParameter(1, 10),
                                 'min_child_weight': ContinuousParameter(0, 120),
                                 'subsample': ContinuousParameter(0.5, 1),
                                 'colsample_bytree': ContinuousParameter(0.5, 1),
                                 'gamma': ContinuousParameter(0, 5),
                                 'lambda': ContinuousParameter(0, 1000),
                                 'alpha': ContinuousParameter(0, 1000)
                                 }

        if self.model_name == 'clf':
            est.set_hyperparameters(objective='reg:logistic',
                                    scale_pos_weight=self._get_imb_ratio()['imb_ratio'])
            objective_metric_name = 'validation:f1'
            objective_type = 'Maximize'
        else:
            est.set_hyperparameters(objective='reg:linear')
            objective_metric_name = 'validation:rmse'
            objective_type = 'Minimize'

        if est.hyperparam_dict is None:
            raise ValueError('Hyper-parameters are missing')
        else:
            logger.info(est.hyperparam_dict)

        tuner = HyperparameterTuner(estimator=est,
                                    objective_metric_name=objective_metric_name,
                                    hyperparameter_ranges=hyperparameter_ranges,
                                    objective_type=objective_type,
                                    max_jobs=100,
                                    max_parallel_jobs=10)

        sw = Stopwatch(start=True)

        tuner.fit({'train': s3_input_train, 'validation': s3_input_val})

        self.post_tune(sage_sess, tuner)

        logger.info('Elapsed time of tuning: {}'.format(sw.elapsed.human_str()))


def get_args():
    """
    Get input arguments

    """

    parser = argparse.ArgumentParser(description="Executing hyper-parameters tuning",
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--last_n_weeks', action='store', help="The number of weeks", dest='last_n_weeks', type=int,
                        default=52)

    parser.add_argument('--aws_env', action='store', help="AWS environment", dest='aws_env', type=str,
                        default='ssense-cltv-qa')

    parser.add_argument('--model', action='store', help="Name of model", dest='model', type=str,
                        default='reg')

    return parser.parse_args()


if __name__ == '__main__':

    sw = Stopwatch(start=True)

    args = get_args()

    data_ext = DataExt(last_n_weeks=args.last_n_weeks, aws_env=args.aws_env, calib=True)

    data_ext.extract_transform_load()

    ml_tune = ModelTune(model_name=args.model, aws_env=args.aws_env)

    ml_tune.tuning()

    logger.info('Total elapsed time: {}'.format(sw.elapsed.human_str()))
