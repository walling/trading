import pyarrow.parquet as pq
from matplotlib import pyplot as pl
import numpy as np
from decimal import Decimal

# -- Load data table and markets metadata --
table = pq.read_table('../../data/2020_kraken.parquet')
markets = table.schema.metadata[b'markets'].decode().split(',')
market_id = markets.index('kraken:btc/eur')
# ['kraken:ada/btc', 'kraken:algo/btc', 'kraken:atom/btc', ...]
print(f'markets: {markets}')
print(f'market_id: {market_id}')  # 10
print()

# -- Convert to pandas frame --
frame = table.to_pandas()
market = frame[frame.market_id == market_id]
assert market.time.is_monotonic, 'Time not monotonic'
print('-- frame: --')
print(frame)  # Pandas data frame
print('\n-- market: --')
print(market)  # Pandas data frame for the selected market_id
print()

# -- Convert prices to Decimal objects --
assert market.price_scale.min() == market.price_scale.max(), 'Price scale not constant'
price_scale = int(market.price_scale.iloc[0])
price = market.price.apply(lambda price: Decimal(price).scaleb(-price_scale))
print(f'price_scale: {price_scale}')  # 1
print('\n-- price: --')
print(price)  # Pandas data series
print()

print(f'trades count: {len(market)}')  # 2602105
pl.plot(market.time[0:10000], price[0:10000])
pl.show()
