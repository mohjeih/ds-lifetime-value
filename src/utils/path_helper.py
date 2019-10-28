
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

TRAIN_DIR = DATA_DIR / 'train'
TRAIN_DIR.mkdir(parents=True, exist_ok=True)

PRED_DIR = DATA_DIR / 'predict'
PRED_DIR.mkdir(parents=True, exist_ok=True)

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


def get_train_dir(filename: str) -> Path:
    return TRAIN_DIR / filename


def get_predict_dir(filename: str) -> Path:
    return PRED_DIR / filename
