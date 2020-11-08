"""
Small script to show how to query and extract data.

Run it:

    env PYTHONPATH=lib python3 spikes/bjarke/read_trades_data.py

"""

from dataset import QueryBuilder

query = QueryBuilder().trades().columns("time", "price").markets("kraken:btc/eur")
print(query)

result = query.run()
time = result.column_numpy("time")
price = result.column_numpy("price", decimals_as_floats=True)

# Do something interesting with the data.
print(time)
print(price)
print(f"Count: {len(time)}")
