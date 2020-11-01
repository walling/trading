import numpy as np
from scipy import stats
from matplotlib import pyplot as pl

N = 1000000
rng = np.random.default_rng()
ints = rng.normal(0.0, 1.0, size=N)
hist = np.histogram(ints, 100)
p = hist[0] / np.sum(hist[0])
print(stats.entropy(p))
pl.plot(p)
pl.show()
