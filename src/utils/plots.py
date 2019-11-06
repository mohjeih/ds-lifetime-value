
"""
@name: local_pred.py

@author: Mohammad Jeihoonian

Created on Oct 2019
"""

import itertools
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from scipy.stats import spearmanr


def bar_plot(dataset, col: str, label: tuple, title: str, figsize: tuple):
    plt.figure(figsize=figsize)
    x_pos = np.arange(len(label))
    plt.bar(x_pos, 100 *dataset[col].value_counts(normalize=True),
            align='center', alpha=0.5, width=0.25)
    plt.xticks(x_pos, label)
    plt.title(title)


def crosstab_plot(dataset, target: str, col: str, label: str, figsize: tuple):
    plt.figure(figsize=figsize)
    dim = dataset.shape[0]
    (pd.crosstab(dataset[target], dataset[col])/dim).plot(kind='bar', stacked='True')
    plt.xlabel(target)
    plt.ylabel(label)
    plt.title(target + " vs " + col)


def cat_plot(dataset, x: str, target: str, col: str, figsize: tuple):
    plt.figure(figsize=figsize)
    sns.catplot(x=x, y=target, col=col, data=dataset,
                saturation=.5, kind='bar', ci=None, aspect=.6)


def dist_plot(dataset, figsize: tuple):
    plt.figure(figsize=figsize)
    sns.distplot(dataset.values)
    plt.title('Distribution')


def bi_hist(dataset, col: str, target: str, legend: list, ylabel: str, figsize: tuple):
    plt.figure(figsize=figsize)
    plt.hist([dataset[dataset[target]==1][col],
              dataset[dataset[target]==0][col]],
             stacked=True, color=['g' ,'b'],
             bins = 20, label=legend)
    plt.ylabel(ylabel)
    plt.legend()
    plt.title('Frequency of  ' + target + ' vs ' +col)


def box_plot(dataset, col: str, by_var: list, figsize: tuple):
    plt.figure(figsize=figsize)
    dataset.boxplot(column=col, by=by_var)


def corr_plot(dataset, figsize: tuple):
    plt.figure(figsize=figsize)
    pd.set_option('precision',2)
    sns.heatmap(dataset.corr(), square=True)
    plt.suptitle('Pearson Correlation Heatmap')


def corr_ph(dataset, figsize: tuple):
    plt.figure(figsize=figsize)
    dataset.plot(kind='barh', colormap='Paired')
    plt.title('Correlation with target variable')


def plot_confusion_matrix(cm, classes,
                          title: str,
                          normalize=False,
                          cmap=plt.cm.Blues):

    print("Confusion Matrix")
    print(cm)

    if normalize:
        cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
        print("Normalized confusion matrix")
        print(cm)

    plt.imshow(cm, interpolation='nearest', cmap=cmap)
    plt.title(title)
    plt.colorbar()
    tick_marks = np.arange(len(classes))
    plt.xticks(tick_marks, classes, rotation=45, fontsize=10)
    plt.yticks(tick_marks, classes, fontsize=10)

    fmt = '.4f' if normalize else ',d'
    thresh = cm.max() / 2.
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, format(cm[i, j], fmt),
                 horizontalalignment="center",
                 color="white" if cm[i, j] > thresh else "black")

    plt.tight_layout()
    plt.ylabel('Actual label')
    plt.xlabel('Predicted label')


def plt_rng(y_pred, y_actual):

    pred2eval = pd.DataFrame({'pred': y_pred, 'actual': y_actual})
    plt.rcParams["xtick.labelsize"] = 8
    plt.rcParams["ytick.labelsize"] = 8

    sns.jointplot('pred', 'actual', data=pred2eval,
                  kind='hex', size=10, stat_func=spearmanr,
                  xlim=[2, 13], ylim=[2, 13])