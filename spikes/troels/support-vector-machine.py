#
# This spike tries out a python library - scikit-learn
# and basic usage of it's support vector machine implementation
#

from sklearn import svm
from dataset import QueryBuilder
import matplotlib.pyplot as pl
import numpy as np
import smooth

query = QueryBuilder().trades().columns("time", "price").markets("kraken:btc/eur")

result = query.run()
time = result.column_numpy("time")
price = result.column_numpy("price", decimals_as_floats=True)

# Smooth with running average
window = 1000
smoothed_price = smooth.smooth(price, window, "flat")
hw = np.int(window / 2)

# Pick out a few values
sparse_price = smoothed_price[hw : -hw + 1][::1000]
sparse_time = time[::1000]

pl.plot(sparse_time, sparse_price)
# pl.plot(time, smoothed_price[hw : -hw + 1])
pl.show()

x = [[0, 0, 0], [1, 1, 1]]
y = [0, 1]

clf = svm.SVC()
clf.fit(x, y)

# print(clf.predict([[0.5, 0.5, 0.5]]))
# print(clf.support_vectors_)
# print(clf.support_)
# print(clf.n_support_)
