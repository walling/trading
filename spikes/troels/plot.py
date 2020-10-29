import pyarrow.parquet as pq
from matplotlib import pyplot as pl
import numpy as np

table = pq.read_table("../../data/2020_kraken.parquet")

markets = table.schema.metadata[b'markets'].decode().split(',')
market_id = markets.index('kraken:btc/eur')

i = market_id
t = table["time"].chunk(i)
p = table["price"].chunk(i)
s = table["price_scale"].chunk(i).to_numpy().astype(np.int8)

pl.plot(t, p.to_numpy()*10.0**(-s))
pl.show()
