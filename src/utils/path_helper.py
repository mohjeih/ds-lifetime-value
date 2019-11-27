
"""
@name: path_helper.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

from pathlib import Path

ROOT_DIR = Path(__file__).parents[2].absolute()

RESOURCES_DIR = ROOT_DIR / 'resources'

CONFIG_DIR = RESOURCES_DIR / 'config'

QUERY_DIR = RESOURCES_DIR / 'queries'

SRC_DIR = ROOT_DIR / 'src'

PARAMS_DIR = SRC_DIR / 'params'

TARGET_DIR = ROOT_DIR / 'target'
TARGET_DIR.mkdir(parents=True, exist_ok=True)

MODEL_DIR = TARGET_DIR / 'model'
MODEL_DIR.mkdir(parents=True, exist_ok=True)

DATA_DIR = TARGET_DIR / 'data'
DATA_DIR.mkdir(parents=True, exist_ok=True)

S3_DIR = TARGET_DIR / 's3'
S3_DIR.mkdir(parents=True, exist_ok=True)

TRAIN_DIR = S3_DIR / 'train'
TRAIN_DIR.mkdir(parents=True, exist_ok=True)

VAL_DIR = S3_DIR / 'val'
VAL_DIR.mkdir(parents=True, exist_ok=True)

FIG_DIR = TARGET_DIR / 'fig'
FIG_DIR.mkdir(parents=True, exist_ok=True)


def get_config(filename: str) -> Path:
    return CONFIG_DIR / filename


def get_query(filename: str) -> Path:
    return QUERY_DIR / filename


def get_params_dir(filename: str) -> Path:
    return PARAMS_DIR / filename


def get_model_dir(filename: str) -> Path:
    return MODEL_DIR / filename


def get_data_dir(filename: str) -> Path:
    return DATA_DIR / filename


def get_s3_dir(filename: str) -> Path:
    return S3_DIR / filename


def get_train_dir(filename: str) -> Path:
    return TRAIN_DIR / filename


def get_val_dir(filename: str) -> Path:
    return VAL_DIR / filename

