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

### QueryBuilder API

Makes it easy to query the datasets. Using the [builder pattern](https://en.wikipedia.org/wiki/Builder_pattern).

#### QueryBuilder()

Constructs a new QueryBuilder object.

#### .build() -> Query

Build this query into an immutable Query object.

#### .run() -> Result

Build and run this query directly, returning the results.

#### .subject(value : str) -> QueryBuilder

Sets ths subject to query. This is the type of dataset to query for. For now, this can only be `trades`.

#### .trades() -> QueryBuilder

Shorthand for calling `.subject("trades")`.

#### .columns(names : List[str]) -> QueryBuilder

#### .columns(\*names : str)

Set the columns to query for. E.g. `.columns("time", "price")`.

For trades data, the columns include: time, price, amount, side, order, extra_json, year, exchange, instrument, source.

#### .sources(symbols : List[str]) -> QueryBuilder

#### .sources(\*symbols : str)

Set the data source(s) to query for. For now, only `kraken_rest` is relevant here. Could include `kraken_ws`, `cryptowatch_ws`, `binance_rest`, etc. in the future.

#### .exchanges(symbols : List[str]) -> QueryBuilder

#### .exchanges(\*symbols : str)

Set the exchange(s) to query for. For now, only `kraken` is relevant here, but could include more exchanges in the future.

Any previous value set by `.markets()` will be reset.

#### .instruments(symbols : List[str]) -> QueryBuilder

#### .instruments(\*symbols : str)

Set the instrument(s) to query for. For now, only instruments like `btc/eur`, `ada/btc`, etc. on kraken is relevant here, but could include more instruments in the future.

Any previous value set by `.markets()` will be reset.

#### .markets(symbols : List[str]) -> QueryBuilder

#### .markets(\*symbols : str)

Set the market(s) to query for. For now, only markets like `kraken:btc/eur`, `kraken:ada/btc`, etc. is relevant here, but could include more instruments in the future.

This is a shorthand form of calling `.exchanges()` and `.instruments()`. Furthermore it allows to specify all exchanges or all instruments for a given market, e.g.: `.markets("kraken:*", "*:btc/eur")`.

Any previous value set by `.exchanges()` and `.instruments()` will be reset.

#### .start(time : str) -> QueryBuilder

#### .start(\*time : Timestamp)

Set the start time of the period to query. The start time is inclusive, i.e. the dataset expression will be something like `time >= start`.

#### .end(time : str) -> QueryBuilder

#### .end(\*time : Timestamp)

Set the end time of the period to query. The end time is exclusive, i.e. the dataset expression will be something like `time < end`.

#### .period(period : str) -> QueryBuilder

#### .period(\*period : TimeInterval)

Set the period (star and end) to query. E.g. `2020`, `2020Q3`, `2020-11`, `2020-11-02`, or using a slash `2020-05-15/2020-07-15`.

#### .reset(keys : List[str]) -> QueryBuilder

#### .reset(\*keys : str)

Reset a previously set value. E.g. `.reset("exchanges")` reset the values set by `.exchanges()`. You can call `.reset()` to reset the whole query.

Possible keys: subject, columns, sources, exchanges, instruments, markets, time, period, start, end.
