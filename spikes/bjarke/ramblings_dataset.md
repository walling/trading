```
dataset/
    timeperiod/
        instrument + exchange + source + time:
            trade
            ohlc1s
            ohlc1m
            ohlc1h
            ohlc1d
            ohlc1mo
            book
            bookdelta
            spread
```

Trades:

-   sources
-   exchanges
-   assets
-   instruments
-   markets
-   time_interval

```
dataset/
    index.parquet
    metadata/
        instrument=btc_eur/
            exchange=kraken/
                index.parquet
    per-decade/
        decade=2010/
            instrument=btc_eur/
                exchange=kraken/
                    source=kraken_rest/
                        subject=trade/
    per-year/
        year=2019/
            instrument=btc_eur/
                exchange=kraken/
                    source=kraken_rest/
                        subject=trade/
    per-month/
        year=2019/
            month=1/
                instrument=btc_usd/
                    exchange=bitfinex/
                        source=cryptowatch_ws/
                            subject=ohlc1m/
    per-day/
        year=2020/
            month=8/
                day=5/
                    instrument=eth_btc/
                        exchange=binance/
                            source=cryptowatch_rest/
                                subject=ohlc5m/
    trunk/
        job=kraken5y_btc_eur/
            job_subject=lock/
            job_subject=pagination/
            year=2020/
                month=10/
                    day=30/
                        instrument=eth_btc/
                            exchange=binance/
                                source=cryptowatch_rest/
                                    subject=ohlc5m/
```

index.parquet:

| metadata | [instrument, exchange]
| per-decade | [time.decade, instrument, exchange, source, subject]
| per-year | [time.year, instrument, exchange, source, subject]
| per-month | [time.year, time.month, instrument, exchange, source, subject]
| per-day | [time.year, time.month, time.day, instrument, exchange, source, subject]
| trunk | [job, time.year, time.month, time.day, instrument, exchange, source, subject]
| trunk/\$job | [job_subject]

filename:

```
instrument-btc-eur_exchange-kraken_source-kraken-rest_from-2020-03-12T231434.832100000Z_to-2020-03-12T232232.070100000Z.parquet
```
