from typing import Optional
import click
from matplotlib import pyplot as pl
from ..query import query_trades


@click.command()
@click.option(
    "-l",
    "--limit",
    default=10000,
    help="Limit number of data points to show",
)
@click.option(
    "--show-query/--no-show-query",
    default=False,
    help="Only show the query used to fetch dataset",
)
@click.argument("market")
@click.argument("period", required=False)
def show(market: str, period: Optional[str], limit: int, show_query: bool):
    query = query_trades(market, period=period)

    if show_query:
        print("-- query: --")
        print(query)
        print()
        print("-- arrow dataset expression: --")
        print(query.build().dataset_filter())
        return

    result = query.run()
    print(result.raw_table_pandas())
    time = result.column_numpy("time")
    price = result.column_numpy("price", decimals_as_floats=True)

    if limit:
        time = time[-limit:]
        price = price[-limit:]

    pl.plot(time, price)
    pl.show()
