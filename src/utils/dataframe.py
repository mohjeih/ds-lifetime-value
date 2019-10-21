
"""
@name: dataframe.py

@author: Mohammad Jeihoonian

Created on Aug 2019
"""

import collections
import pandas as pd


def cast_type(dataset, col, type):

    return dataset[col].astype(type)


def merge_datasets(left_dataset, right_dataset, how, key):

    """
    Merge dataframes given pivotal columns
    """

    return pd.merge(left_dataset, right_dataset, how=how, on=key)


def join_datasets(left_dataset, right_dataset, how, key):

    """
    Join dataframes given pivotal columns
    """

    if key is not None:

        dataset = left_dataset.set_index(key).join(right_dataset.set_index(key), how=how)
        dataset.reset_index(key, inplace=True)
    else:
        dataset = left_dataset.join(right_dataset, how=how)

    return dataset


def one_hot_encode(dataset, threshold):
    """

    :param dataset
    :param threshold
    :return: dataframe
    """

    dataset = dataset.copy()

    # The ratio of the dummy variable
    count = pd.value_counts(dataset, normalize=True)

    # Check whether the ratios is higher than the threshold
    mask = dataset.isin(count[count > threshold].index)

    # Replace the ones which ratio is lower than the threshold by a special name
    dataset[~mask] = 'others'

    return pd.get_dummies(dataset, prefix=dataset.name, prefix_sep=':')


def str_encode(dataset, threshold, sep=','):
    """

    :param dataset
    :param threshold
    :param sep
    :return: dataframe
    """

    dataset = dataset.fillna('')

    try:
        dataset = sep + dataset + sep
    except TypeError:
        dataset = sep + dataset.astype(str) + sep

    # Split sting into list
    dataset_split = dataset.str.split(sep)

    # Count per list collections.Counter return a dict of element count
    res_list = [*map(lambda x: collections.Counter(x), dataset_split.values)]
    # turn dict to pd.DataFrame
    res = pd.DataFrame(res_list)
    # remove the dummy empty ''
    res = res.drop(columns='', axis=1).fillna(0)

    # Remove infrequent
    res_bin = (res > 0).astype(int)
    count = res_bin.mean()
    ind = (count > threshold).values
    ind_keep = count.index[ind]
    ind_mask = count.index[~ind]

    # Slice and rename
    one_hot = res.loc[:, ind_keep].copy()
    if len(ind_keep) < res.shape[1]:
        one_hot = one_hot.assign(others=res.loc[:, ind_mask].sum(axis=1).astype('float'))

    one_hot.rename(columns=lambda x: dataset.name + ':' + x, inplace=True)

    return one_hot


def get_feature_name(columns):
    """

    :param columns
    :return: dataframe
    """

    feature_name = columns.ravel()
    dframe_feature = pd.DataFrame({'feature_name': feature_name})
    dframe_feature_ = dframe_feature.feature_name.str.split(':', expand=True)
    dframe_feature_.columns = ['var', 'level']
    d_feature = pd.concat([dframe_feature, dframe_feature_], axis=1)
    d_feature = d_feature.assign(type=lambda x: x.level.isnull().apply(lambda x: 'num' if x else 'cat'))

    return d_feature
