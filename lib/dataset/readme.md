# Dataset tool

## CLI

Run it:

```bash
./tools/dataset --help
```

## Library

Example:

```python
from dataset import QueryBuilder
result = (
    QueryBuilder()
    .trades()
    .columns("time", "price", "side")
    .markets("kraken:btc/eur")
    .run()
)
print(result.column_numpy("time", timestamps_as_floats=True))
print(result.column_numpy("price", decimals_as_floats=True))
print(result.column_numpy("side"))
```

Try running:

```bash
env PYTHONPATH=lib python3 spikes/bjarke/read_trades_data.py
```
