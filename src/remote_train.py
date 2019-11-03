
"""
@name: remote_train.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

import boto3
import botocore
import datetime
import logging
import pandas as pd
import numpy as np
import sagemaker
import shutil
import xgboost as xgb
from sagemaker.amazon.amazon_estimator import get_image_uri
from sagemaker.estimator import Estimator
from sklearn.metrics import f1_score, mean_squared_error
from src.utils.db_engine import s3_aws_engine, get_aws_role
from src.params.model_params import *
from src.utils.path_helper import *
from src.utils.resources import *
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class RemoteTrain(object):

    def __init__(self, model_name, aws_env):

        self.model_name = model_name
        self.aws_env = aws_env
        self.params = self.get_params()
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
            return load_param_json(get_params_dir('imb_ratio.py'))
        else:
            return None

    def get_params(self):
        if self.model_name == 'clf':
            return CLF_PARAM
        else:
            return REG_PARAM

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

    def dump_model(self, boto_sess, s3_bucket, job_name):

        s3r = boto_sess.resource('s3')

        bucket = s3r.Bucket(s3_bucket)

        for obj in bucket.objects.filter(Prefix='model_' + self.model_name):
            try:
                if obj.key.rsplit('/')[1] == job_name:
                    logger.info('Dumping {} model under job name: {}'.format(self.model_name, job_name))
                    bucket.download_file(obj.key, str(get_model_dir(obj.key.rsplit('/')[-1])))
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    logger.info('The object does not exist!')
                else:
                    raise

        # bucket.objects.filter(Prefix='model_' + self.model_name + '/').delete()

    def extract_model(self):

        logger.info('Extracting {} model...'.format(self.model_name))

        shutil.unpack_archive(str(get_model_dir('model.tar.gz')), MODEL_DIR)

        if Path(get_model_dir(self.model_name + '-model')).is_file():
            Path.unlink(get_model_dir(self.model_name + '-model'))

        Path.rename(get_model_dir('xgboost-model'), get_model_dir(self.model_name + '-model'))

    def load_model(self):

        logger.info('Loading {} model...'.format(self.model_name))

        model = load(get_model_dir(self.model_name + '-model'))

        # model.dump_model(str(get_model_dir(self.model_name + '_model.txt')))

        return model

    def fetch_data_val(self, var):

        logger.info('Fetching validation data...')

        val_df = load(get_data_dir(self.model_name + '_val.pkl'))
        X_val = val_df.drop(columns=var, axis=1).values
        y_val = val_df[var].values

        return X_val, y_val

    def val_fit(self, X_val, y_val):

        logger.info('Fitting the trained model on validation data...')

        model = self.load_model()

        d_val = xgb.DMatrix(data=X_val, label=y_val)

        if self.model_name == 'clf':
            y_est = model.predict(d_val)
            return np.where(y_est >= 0.5, 1, 0)
        else:
            return model.predict(d_val)

    def val_metric(self, y_val, y_est):

        logger.info('Calculating the evaluation metric...')

        timestamp = datetime.date.today().strftime('%Y-%m-%d')

        if self.model_name == 'clf':
            score = f1_score(y_val, y_est)
            logger.info('F1 score: {}'.format(score))
            metric_name = 'f1'
        else:
            score = np.sqrt(mean_squared_error(y_val, y_est))
            logger.info('RMSE score: {}'.format(score))
            metric_name = 'rmse'

        score_df = pd.DataFrame({'timestamp': [timestamp], 'metric_name': [metric_name],
                                 'value': [score]})

        score_df.to_csv(get_model_dir(self.model_name + '_metrics.csv'), index=False)

        return score

    def _validation(self):

        logger.info('Validating the trained model...')

        if self.model_name == 'clf':
            X_val, y_val = self.fetch_data_val('LTV_active')
        else:
            X_val, y_val = self.fetch_data_val('LTV_52W')

        y_est = self.val_fit(X_val, y_val)

        self.val_metric(y_val, y_est)

    def train(self):

        s3_bucket, id, secret = s3_aws_engine(name=self.aws_env)

        s3_path = RemoteTrain._aws_s3_path(s3_bucket)

        boto_sess = RemoteTrain._boto_session(id, secret)

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
                        output_path=s3_path + 'model_' + self.model_name + '/',
                        sagemaker_session=sage_sess,
                        base_job_name=self.model_name + '-job')

        logger.info('Setting hyper-parameters...')

        est.set_hyperparameters(**self.params)

        if self.model_name == 'clf':
            est.set_hyperparameters(scale_pos_weight=self._get_imb_ratio()['imb_ratio'])

        if est.hyperparam_dict is None:
            raise ValueError('Hyper-parameters are missing')
        else:
            logger.info(est.hyperparam_dict)

        sw = Stopwatch(start=True)

        est.fit({'train': s3_input_train, 'validation': s3_input_val})

        # The following method is inconsistent with newer version of xgboost
        try:
            est.training_job_analytics.export_csv(get_model_dir(self.model_name+'_aws_metrics.csv'))
        except:
            pass

        logger.info('Elapsed time of training: {}'.format(sw.elapsed.human_str()))

        job_name = est.latest_training_job.job_name

        self.dump_model(boto_sess, s3_bucket, job_name)

        self.extract_model()

        self._validation()
