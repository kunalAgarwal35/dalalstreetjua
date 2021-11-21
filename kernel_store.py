import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from scipy.stats import norm, gaussian_kde
from sklearn.neighbors import KernelDensity

import function_store2 as fs
import datetime


now_ts = datetime.datetime(2021,5,18,12,0,0)
expiry = datetime.date(2021,5,25)

n = fs.nifty_distribution_custom(15,17,400,fs.vix, fs.nifty)
n = list(n)
n.sort()
pd.Series(n).plot()



def probability_below(x,n):
    scipy_kde = gaussian_kde(n)
    return scipy_kde.integrate_box_1d(-100, x)
# sample = scipy_kde.resample(size=1000)[0]
# sample.sort()
# u = np.linspace(-20,20,500)
# v = scipy_kde.evaluate(u)
#
# plt.plot(u,v)