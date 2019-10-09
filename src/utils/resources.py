
"""
@name: resource.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

import pickle
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


