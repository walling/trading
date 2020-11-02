import pyarrow.parquet as pq
import numpy as np
from decimal import Decimal
from datetime import timedelta


table = pq.read_table('../../data/2020_kraken.parquet')
markets = table.schema.metadata[b'markets'].decode().split(',')
market_id = markets.index('kraken:btc/eur')

t = table["time"].chunk(market_id).to_numpy()
p = table["price"].chunk(market_id).to_numpy()
s = table["price_scale"].chunk(market_id).to_numpy().astype(np.int8)

# Apply scale to get price as floating point number
p = p*10.0**(-s)

# Find changes in price
dp = p[:-1] - p[1:]

# Only consider increases, as investing in these yields a profit
dp = np.maximum(dp, 0.0)

# Sum potential fractional profits
profit = np.sum(dp/p[:-1])

# Measure the period the profit was made over (in nanoseconds)
dt = t[-1] - t[0]

# Get nanoseconds as int
ns = dt.astype(int)

# Convert to python timedelta
dt = timedelta(milliseconds=ns/10**6)

# Compute profit per year
year = timedelta(days=365, hours=6)
profit_per_year = profit*year/dt

# Print result
print("Total maximal profit: ", profit*100, "% over ", dt, ".")

print()

print("This is equivalent to ", profit_per_year*100, "% per year.")
