import pyarrow.parquet as pq
import numpy as np
from matplotlib import pyplot as pl
from decimal import Decimal
from datetime import timedelta


table = pq.read_table("../../data/2020_kraken.parquet")
markets = table.schema.metadata[b"markets"].decode().split(",")
market_id = markets.index("kraken:btc/eur")

t = table["time"].chunk(market_id).to_numpy()
p = table["price"].chunk(market_id).to_numpy()
s = table["price_scale"].chunk(market_id).to_numpy().astype(np.int8)

# Apply scale to get price as floating point number
p = p * 10.0 ** (-s)

# Find changes in price
dp = p[:-1] - p[1:]

# Plot previous transaction against current
pl.plot(dp[:-1], dp[1:], ".r", markersize=0.1)
pl.show()
