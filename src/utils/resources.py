
"""
@name: resource.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

import json
import os
import pickle
import sys
import unicodedata


def normalize(x: str):
    return (
        unicodedata.normalize('NFKD', x)
        .encode('ASCII', 'ignore')
        .decode('utf-8')
        .strip()
        .lower()
    )


def dump(obj, filename):
    """
    Dump an object as a pickle file
    :param obj
    :param filename
    """

    try:
        with filename.open('wb') as handle:
                pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)
    except Exception:
        raise ValueError('Failed to dump {}'.format(filename))


def load(filename):
    """
    Load pickle file
    :param filename
    :return dataset
    """

    try:
        with filename.open('rb') as handle:
            dataset = pickle.load(handle)
        return dataset
    except Exception:
        raise ValueError('Failed to load {}'.format(filename))


def dump_param_json(obj, filename):
    """
    Dump output parameters as json file
    :param obj
    :param filename
    """

    try:
        with filename.open('w') as file:
            json.dump(obj, file)
    except Exception:
        raise ValueError("Failed to dump the desired file")


def load_param_json(filename):
    """

    Load input parameters

    :param filename
    :return params
    """

    try:
        with filename.open('r') as file:
            params = json.load(file)
        return params
    except Exception:
        raise ValueError("Failed to load the desired file")


def load_project_id():
    with open(os.environ['GOOGLE_APPLICATION_CREDENTIALS']) as secret:
        return json.loads(secret.read())['project_id']


class FlushFile(object):

    def __init__(self, f):
        self.f = f

    def __getattr__(self, name):
        return object.__getattribute__(self.f, name)

    def write(self, x):
        self.f.write(x)
        self.f.flush()

    def flush(self):
        self.f.flush()