
"""
@name: remote_model.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

import boto3
import botocore
import logging
import sagemaker
import shutil
import xgboost as xgb
from sagemaker.amazon.amazon_estimator import get_image_uri
from sagemaker.estimator import Estimator
from src.utils.db_engine import s3_aws_engine, get_aws_role
from src.utils.path_helper import *
from src.utils.resources import *
from timeutils import Stopwatch

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class RemoteModel(object):

    def __init__(self, model_name, aws_env, params, imb_ratio):

        self.model_name = model_name
        self.aws_env = aws_env
        self.params = params
        self.imb_ratio = imb_ratio
        self.role = get_aws_role('ssense_role')

    @staticmethod
    def aws_s3_path(s3_bucket):

        logger.info('Getting S3 bucket path...')

        s3_bucket_path = s3_bucket + '/'

        return 's3://' + s3_bucket_path

    @staticmethod
    def boto_session(id, secret):

        logger.info('Creating boto session...')

        return boto3.Session(aws_access_key_id=id, aws_secret_access_key=secret, region_name='us-east-2')

    @staticmethod
    def fetch_pred_data(filename):

        logger.info('Fetching prediction data...')

        X_pred = load(get_data_dir(filename))

        return xgb.DMatrix(data=X_pred.values)

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

        model.dump_model(str(get_model_dir(self.model_name + '_model.txt')))

        return model

    def train(self):

        s3_bucket, id, secret = s3_aws_engine(name=self.aws_env)

        s3_path = RemoteModel.aws_s3_path(s3_bucket)

        boto_sess = RemoteModel.boto_session(id, secret)

        logger.info('Getting algorithm image URI...')

        container = get_image_uri(boto_sess.region_name, 'xgboost')

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
            est.set_hyperparameters(scale_pos_weight=self.imb_ratio)

        if est.hyperparam_dict is None:
            raise ValueError('Hyper-parameters are missing')
        else:
            logger.info(est.hyperparam_dict)

        sw = Stopwatch(start=True)

        est.fit({'train': s3_input_train, 'validation': s3_input_val})

        est.training_job_analytics.export_csv(get_model_dir(self.model_name+'_metrics.csv'))

        logger.info('Elapsed time of training: {}'.format(sw.elapsed.human_str()))

        job_name = est.latest_training_job.job_name

        self.dump_model(boto_sess, s3_bucket, job_name)

        self.extract_model()

    def predict(self):

        dpred = RemoteModel.fetch_pred_data(filename='X_pred.pkl')

        model = self.load_model()

        y_pred = model.predict(dpred)

        return y_pred
