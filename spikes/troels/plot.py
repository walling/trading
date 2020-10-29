import pyarrow.parquet as pq
from matplotlib import pyplot as pl
import numpy as np

table = pq.read_table("../../data/2020_kraken.parquet")
i = 10
t = table["time"].chunk(i)
p = table["price"].chunk(i)
s = table["price_scale"].chunk(i).to_numpy().astype(np.int8)

print(t)

pl.plot(t, p.to_numpy()*10.0**(-s))
pl.show()
