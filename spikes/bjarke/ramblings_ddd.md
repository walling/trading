commands:

-   AddTradingRecords
-   IndexDataset

queries:

-   DatasetQuery
-   DatasetQueryResult

events:

-   CommandExecuted
-   QueryCompleted

```python
app.add_trading_records(table=xxx)
app.query_trading_records(market='kraken:ada/eth', start_time='2018', end_time='2019')
```
