# Dataset tool

## CLI

Run it:

```bash
./tools/dataset --help
```

## Library

Example:

```python
from lib.dataset.model.read.query import QueryBuilder
result = (
    QueryBuilder()
    .trades()
    .columns("time", "price", "side")
    .markets("kraken:btc/eur")
    .run()
)
print(result.column_numpy("time"))
print(result.column_numpy("price"))
print(result.column_numpy("side"))
```

Try running:

```bash
python3 -m lib.dataset.model.read.query
```
