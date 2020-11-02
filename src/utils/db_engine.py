
"""
@name: db_engine.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

import logging
import os
import pandas as pd
import yaml
from pathlib import Path
from sqlalchemy import create_engine
from src.utils.path_helper import get_config
from src.utils.mysql_character_fix import mysql_character_fix
from dsutil.access import credential_stats_db, credential_hq_db, credential_data_lake_db

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


def load_configuration(db_name, filename='config.yml'):
    """

    :param filename
    :return: dict

    Notes
    -----
        There needs to be a yaml file containing credentials in your config
        directory. Please consult with the
        template config file in the github repository.
    """

    try:
        if 'CONFIGURATION_FILE' not in os.environ:
            os.environ["CONFIGURATION_FILE"] = str(get_config(filename))

        with Path(os.environ["CONFIGURATION_FILE"]).open('r') as ymlfile:
            cfg = yaml.load(ymlfile)[db_name]
    except Exception as er:
        if er.args[0] == 2:
            logger.error('No yaml config file with the name {} is located in the config directory'.format(
                filename))
        elif er.args[0] == db_name:
            logger.error(
                'Database {} does not exist in the yaml config file'.format(db_name))
        raise er

    return cfg


def mysql_engine(db_name: str, echo=False):

    db_dict = {'ssense': credential_hq_db,
               'stats': credential_stats_db}

    credentials = db_dict[db_name]()

    try:
        engine_name = (f"mysql+pymysql://{credentials['username']}:"
                       f"{credentials['password']}@{credentials['host']}/"
                       f"{credentials['schema']}")
        return create_engine(engine_name,
                             pool_recycle=2,
                             echo=echo)
    except KeyError as err:
        raise err


def get_data_slave(db_name: str, query: str, cols_list: list,
                   echo: bool=False) -> pd.DataFrame:
    """

    Queries Slave data bases

    :param db_name: str, name of the db
    :param query: string, the actual query

    :return pd.DataFrame, a dataframes with queried data, no column names
    """

    try:
        cnx = mysql_engine(db_name, echo)
        dataset = pd.read_sql(query, cnx)
        if len(cols_list) != 0:
            for col in cols_list:
                logger.info('Fixing characters in %s ' % col)
                dataset[col].replace(
                    mysql_character_fix,
                    regex=True,
                    inplace=True
                )
    except Exception as err:
        logger.error('Something went wrong: {}'.format(err))
        raise err

    return dataset


def s3_aws_engine(name: str):
    """

    Retrieve AWS credentials
    """

    s3_cfg = load_configuration(name)

    return s3_cfg['bucket'], s3_cfg['id'], s3_cfg['secret']


def get_aws_role(name: str):
    """

    Retrieve AWS role
    """

    aws_role = load_configuration(name)

    if ':role/' in aws_role['aws_role']:
        return aws_role['aws_role']
    else:
        message = 'The current AWS identity is not a role: {}'
        raise ValueError(message.format(aws_role['aws_role']))
