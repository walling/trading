import pyarrow.parquet as pq
from matplotlib import pyplot as pl
import numpy as np
from decimal import Decimal

def iter_row_groups(metadata):
    for r in range(metadata.num_row_groups):
        yield r, metadata.row_group(r)

def iter_column(row_group):
    for c in range(row_group.num_columns):
        yield c, row_group.column(c)

def find_column(metadata, name):
    for c, column in iter_column(metadata.row_group(0)):
        if column.path_in_schema == name: return c
    return None

def get_row_groups_for_market(metadata, market_id):
    result = []
    column_id = find_column(metadata, 'market_id')
    for r, row_group in iter_row_groups(metadata):
        column = row_group.column(column_id)
        assert column.statistics.has_min_max, 'Market id statistics not saved'
        assert column.statistics.min == column.statistics.max, 'Market id not constant within row group'
        if column.statistics.min == market_id: result.append(r)
    return result

def get_price_scale(metadata, row_group_id):
    column_id = find_column(metadata, 'price_scale')
    column = metadata.row_group(row_group_id).column(column_id)
    assert column.statistics.has_min_max, 'Price scale statistics not saved'
    assert column.statistics.min == column.statistics.max, 'Price scale not constant within row group'
    return int(column.statistics.min)

#-- Load data table and markets metadata --
file = pq.ParquetFile('../../data/2020_kraken.parquet')
markets = file.schema_arrow.metadata[b'markets'].decode().split(',')
market_id = markets.index('kraken:btc/eur')
print(f'markets: {markets}') # ['kraken:ada/btc', 'kraken:algo/btc', 'kraken:atom/btc', ...]
print(f'market_id: {market_id}') # 10
print()

#-- Only read pandas frame for selected market_id and the columns we need --
row_group_ids = get_row_groups_for_market(file.metadata, market_id)
table = file.read_row_groups(row_group_ids, ['time', 'price'])
time = table['time'].to_numpy()
price = table['price'].to_numpy()
assert np.all(np.diff(time) >= np.timedelta64(0)), 'Time not monotonic'
print(f'row_group_ids: {row_group_ids}') # [10]
print('\n-- time: --')
print(time) # Numpy array
print('\n-- price: --')
print(price) # Numpy array
print()

#-- Scale prices using floating-point operations --
price_scale = get_price_scale(file.metadata, row_group_ids[0])
price = price * (10**(-price_scale))
print(f'price_scale: {price_scale}') # 1
print('\n-- price: --')
print(price) # Numpy array
print()

print(f'trades count: {len(price)}') # 2602105
pl.plot(time[0:10000], price[0:10000])
pl.show()
