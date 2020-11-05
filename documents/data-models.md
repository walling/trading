# Data models

For efficient processing, we should limit data to signed and unsigned integers (int8 to int64), floats (float16 to float64), and timestamps (stored as int32/int64). Furthermore no null values are allowed in storage.

Decimal numbers are stored as integers with a fixed scale (number of decimal places). The scale can be stored as part of the metadata for a given column or as a separate column (if different decimal scales are to be stored).

## Asset

An asset is something you can own and trade. In forex markets it's money. It can also be precious metals, stocks, property, etc.

Data model:

1.  `symbol` (string): Symbol to identify an asset (e.g. `btc`, `usd`, etc.)

Considerations:

-   We use the [Cryptowatch](https://docs.cryptowat.ch/rest-api/assets/assets-index) symbols whenever available

## Instrument

An instrument is the trading of a base asset by a quoted asset. It can also be a derivative, i.e. a future or option, which is a contract on a future price.

Data model:

1.  `base` (string): Asset symbol of the base (what is bought/sold)
2.  `quote` (string): Asset symbol of the quote (used to pay for the base asset)
3.  `extension` (string): Used to distingish derivatives with identical base/quote pairs

The instrument is represented by a symbol that is the base, quote, and optional extension seperated by a slash, e.g. `eur/usd` or `btc/usd/quarterly-future-2020Q3`.

Considerations:

-   We use the [Cryptowatch](https://docs.cryptowat.ch/rest-api/assets/assets-index) symbols for assets whenever available, however we don't use the Cryptowatch instrument symbol, since they don't divide the elements by a slash `/`, which we do.

## Exchange

A place where multiple instruments are traded. Usually they have an API for trading, accessing live data over Websocket and downloading (some) historical data over REST.

Data model:

1.  `symbol` (string): Symbol to identify an exchange (e.g. `kraken`, `binance`, etc.)

Considerations:

-   We use the [Cryptowatch](https://docs.cryptowat.ch/rest-api/exchanges/list) symbols whenever available

## Data source

An online API to access live data or download historical data.

Data model:

1.  `symbol` (string): Symbol to identify a data source (e.g. `cryptowatch` or `kraken_api`)

Considerations:

-   Currently only `kraken_api` is in use

## Market

A market is a specific instrument traded on a specific exchange.

Data model:

1.  `id` (int64): Stable numeric identification of the market
2.  `exchange` (string): Exchange symbol
3.  `instrument` (string): Instrument symbol

The market is represented by the exchange and instrument symbols seperated by a colon, e.g. `kraken:eur/usd` or `fix:btc/usd/quarterly-future-2020Q3`.

Considerations:

-   The `id` makes it possible to store the market in an integer column, which takes less space and is more computationally effective, e.g. when storing many trades.
-   It's very common with the `exchange:instrument` naming convention, e.g. on [TradingView](https://www.tradingview.com)

## Trades

Data model:

1.  `market_id` (int64): Market the trade occurred in
2.  `external_id` (int64 / uuid / string): Trade id as identified by the exchange
3.  `time` (timestamp64[ns]): UTC time with nanosecond precision, when the trade happened
4.  `price.value` (int64): Price of the base asset (in quote asset)
5.  `price.scale` (int8): Number of decimal places for price
6.  `amount.value` (int64): Amount of base asset bought/sold
7.  `amount.scale` (int8): Number of decimal places for amount
8.  `taker_side` (category[int8]): Taker side cateogry, either 1 (buy) or 2 (sell), may be null if unknown
9.  `order_type` (category[int8]): Order type category, either 1 (market) or 2 (limit), may be null if unknown

Considerations:

-   The `external_id` is only useful for reconcilation process, i.e. creating one record from multiple different recordings. It's not that useful for financial analysis, it can probably be discarded to save storage after reconcilation. It's usually just an integer, and a uuid (stored as bytes[20]) in a few cases. It's rare that a string is needed. In a few cases, the integer id is only unique within a specific market, so market + integer id should be globally unique.
-   The `time` is sorted, which saves space (e.g. delta encoding) and optimizes lookup (e.g. binary search). It's needed to generate OHLC data, plot trades on a chart, and for most financial analysis.
-   The `price_scale` as well as `amount_scale` should be kept constant for a given market, since this allows better compression and efficient computation
