import pyarrow.parquet as pq
from matplotlib import pyplot as pl
import numpy as np

def normalize(numbers, high, new_max, new_type):
  return (numbers*(new_max/high)).astype(new_type)

table = pq.read_table("../../data/2020_kraken.parquet")
i = 10
p = table["price"].chunk(i).to_numpy().astype(np.int32)
s = table["price_scale"].chunk(i).to_numpy().astype(np.int8)
p = p*10.0**(-s)

maxint8 = 2**7-1
dp = (p[1:] - p[:-1])/2
high = np.max(np.abs(dp))
dp_scaled = normalize(dp, high, maxint8, np.int8)
# dp_scaled.tofile("random/2020_kraken_btc-eur-diff")

N = dp.shape[0]
rng = np.random.default_rng()
# ints = rng.integers(-high, high=high, size=N)
# ints_scaled = normalize(ints, high, maxint8, np.int8)
# ints_scaled.tofile("random/random-uniform")

ints = rng.normal(0.0, 1.0, size=N)#.astype(np.int8)
high = np.max(np.abs(ints))
ints_scaled = normalize(ints, high, maxint8, np.int8)
ints_scaled.tofile("random/random-normal")
# pl.plot(ints_scaled)
# pl.plot(dp_scaled)
# pl.show()